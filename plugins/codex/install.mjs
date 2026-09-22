#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import { copyFile, lstat, mkdir, readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { isMain } from '../otel.mjs';

const PLUGIN = 'doda-datalake-codex';
const MARKETPLACE = 'doda-datalake-local';
const SELECTOR = `${PLUGIN}@${MARKETPLACE}`;
const SOURCE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const MARKER = 'doda-datalake-codex-marketplace-v1\n';
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;

export function installationPlan(args = [], environment = process.env) {
  const { values } = parseArgs({ args, options: {
    home: { type: 'string' }, config: { type: 'string' }, source: { type: 'string', default: 'plugin' },
    codex: { type: 'string', default: 'codex' }, apply: { type: 'boolean', default: false },
    'dry-run': { type: 'boolean', default: false }, uninstall: { type: 'boolean', default: false }, help: { type: 'boolean', default: false },
  } });
  if (!['plugin', 'native'].includes(values.source)) throw new Error('--source는 plugin 또는 native여야 합니다.');
  if (values.apply && values['dry-run']) throw new Error('--apply와 --dry-run을 동시에 사용할 수 없습니다.');
  const home = resolve(values.home ?? environment.HOME ?? homedir());
  const codexHome = values.home ? join(home, '.codex') : resolve(environment.CODEX_HOME ?? join(home, '.codex'));
  const marketplace = join(codexHome, 'doda-datalake-marketplace');
  const commands = values.uninstall
    ? [['plugin', 'remove', SELECTOR]]
    : [['plugin', 'marketplace', 'add', marketplace], ['plugin', 'add', SELECTOR]];
  return {
    home, codexHome, marketplace, pluginRoot: join(marketplace, 'plugin'),
    configPath: values.config ? resolve(values.config) : undefined,
    source: values.source, binary: values.codex, apply: values.apply, uninstall: values.uninstall,
    help: values.help, commands,
  };
}

export async function install(args = []) {
  const plan = installationPlan(args);
  if (plan.help) {
    console.log('사용법: node plugins/codex/install.mjs [--home DIR] [--config PATH] [--source plugin|native] [--codex PATH] [--apply|--dry-run] [--uninstall]\n기본값은 변경 없는 미리보기입니다. --home은 사용자 홈 루트이며 Codex 설정은 DIR/.codex입니다.\nplugin(기본): 훅+현재 세션의 숫자 토큰만 내보냅니다. 같은 데이터레이크의 네이티브 OTel과 병행하지 마세요.\nnative: 기존 네이티브 OTel만 사용하도록 이 플러그인의 내보내기를 모두 생략합니다. 기존 OTel 설정을 수정하지 않습니다.\n설치 후 Codex를 재시작하고 /hooks에서 각 훅을 직접 검토·신뢰해야 합니다. 신뢰·샌드박스 우회는 하지 않습니다.\n제거는 네이티브 plugin remove를 사용하며 다른 플러그인과 재설치용 로컬 마켓플레이스를 보존합니다.');
    return plan;
  }
  for (const command of plan.commands) console.log(`HOME=${quote(plan.home)} CODEX_HOME=${quote(plan.codexHome)} ${quote(plan.binary)} ${command.map(quote).join(' ')}`);
  console.log('설치 후 /hooks에서 검토·신뢰가 필요합니다. 기존 config.toml의 OTel·승인·샌드박스 설정은 변경하지 않습니다.');
  console.log(`수집 소스: ${plan.source}. plugin과 동일 데이터레이크의 네이티브 OTel을 동시에 사용하지 마세요.`);
  if (!plan.apply) { console.log('미리보기: 파일과 Codex 설정을 변경하지 않았습니다. 적용하려면 --apply를 추가하세요.'); return plan; }
  for (const path of [
    plan.home, plan.codexHome, plan.marketplace,
    ...['.doda-datalake-managed', '.agents', '.agents/plugins', '.agents/plugins/marketplace.json',
      'plugin', 'plugin/.codex-plugin', 'plugin/codex', 'plugin/codex/settings.json'].map(path => join(plan.marketplace, path)),
  ]) {
    try {
      if ((await lstat(path)).isSymbolicLink()) throw new Error('설치 경로의 심볼릭 링크는 따라가지 않습니다.');
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  if (plan.configPath) {
    const config = JSON.parse(await readFile(plan.configPath, 'utf8'));
    if (!config || typeof config !== 'object' || Array.isArray(config)) throw new Error('텔레메트리 설정은 JSON 객체여야 합니다.');
  }
  if (!plan.uninstall) {
    const markerPath = join(plan.marketplace, '.doda-datalake-managed');
    try {
      const info = await lstat(plan.marketplace);
      if (!info.isDirectory() || await readFile(markerPath, 'utf8') !== MARKER) throw new Error('관리 대상이 아닌 기존 마켓플레이스 경로는 덮어쓰지 않습니다.');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      // A directory without our marker is never adopted, even if it happens to be empty.
      try { await lstat(plan.marketplace); throw new Error('기존 마켓플레이스 경로에 소유 표식이 없습니다.'); }
      catch (inner) { if (inner.code !== 'ENOENT') throw inner; }
      await mkdir(plan.marketplace, { recursive: true, mode: 0o700 });
      await writeFile(markerPath, MARKER, { mode: 0o600, flag: 'wx' });
    }
    for (const path of ['.agents/plugins', 'plugin/.codex-plugin', 'plugin/codex']) await mkdir(join(plan.marketplace, path), { recursive: true, mode: 0o700 });
    for (const path of ['otel.mjs', '.codex-plugin/plugin.json', 'codex/hook.mjs', 'codex/usage.mjs', 'codex/hooks.json']) {
      const target = join(plan.pluginRoot, path);
      try { if ((await lstat(target)).isSymbolicLink()) throw new Error('플러그인 파일의 심볼릭 링크는 덮어쓰지 않습니다.'); }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
      await copyFile(join(SOURCE_ROOT, path), target);
    }
    await writeFile(join(plan.pluginRoot, 'codex/settings.json'), JSON.stringify({ configPath: plan.configPath, source: plan.source }) + '\n', { mode: 0o600 });
    await writeFile(join(plan.marketplace, '.agents/plugins/marketplace.json'), JSON.stringify({
      name: MARKETPLACE,
      plugins: [{ name: PLUGIN, source: { source: 'local', path: './plugin' }, policy: { installation: 'AVAILABLE', authentication: 'ON_USE' }, category: 'Productivity' }],
    }, null, 2) + '\n', { mode: 0o600 });
  }
  for (const command of plan.commands) {
    try {
      execFileSync(plan.binary, command, {
        cwd: plan.codexHome, env: { ...process.env, HOME: plan.home, CODEX_HOME: plan.codexHome },
        stdio: ['ignore', 'pipe', 'pipe'], timeout: 60_000, maxBuffer: 1024 * 1024,
      });
    } catch { throw new Error('Codex 네이티브 플러그인 명령이 실패했습니다. 위 명령을 직접 실행해 로컬 오류를 확인하세요.'); }
  }
  console.log(plan.uninstall ? 'Codex 플러그인을 제거했습니다. 다른 설정과 로컬 소스는 보존했습니다.' : 'Codex 플러그인을 설치했습니다. 재시작 후 /hooks에서 훅을 신뢰하세요.');
  return plan;
}

if (isMain(import.meta.url)) {
  try { await install(process.argv.slice(2)); }
  catch { console.error('Codex 설치를 완료하지 못했습니다. 경로·설정 형식·Codex 설치 상태를 확인하세요.'); process.exitCode = 1; }
}
