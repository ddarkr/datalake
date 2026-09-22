import { copyFile, lstat, mkdir, readFile, rmdir, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { isMain } from '../otel.mjs';

const pluginName = 'doda-datalake-otel';
const root = fileURLToPath(new URL('../', import.meta.url));
const ownedFiles = ['.claude-plugin/plugin.json', 'hooks/hooks.json', 'claude-code/hook.mjs', 'otel.mjs', 'claude-code/config-path.json'];
const markerName = '.datalake-otel-install.json';
const fail = message => { throw Object.assign(new Error(message), { code: 'DATALAKE_INSTALL' }); };

async function readOptional(path) {
  try { return await readFile(path, 'utf8'); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}

export async function install(argv = process.argv.slice(2)) {
  const { values } = parseArgs({ args: argv, options: {
    home: { type: 'string' }, config: { type: 'string' }, apply: { type: 'boolean' },
    uninstall: { type: 'boolean' }, help: { type: 'boolean' },
  } });
  if (values.help) {
    console.log('사용법: node plugins/claude-code/install.mjs [--home DIR] [--config PATH] [--apply] [--uninstall]\n기본값은 dry-run입니다. Claude Code 2.1.223+ skills-dir 플러그인으로 설치하며 기존 settings.json은 변경하지 않습니다. --home은 사용자 홈 디렉터리입니다.');
    return;
  }
  const home = resolve(values.home || homedir());
  const claudeDir = values.home ? join(home, '.claude') : resolve(process.env.CLAUDE_CONFIG_DIR || join(home, '.claude'));
  const destination = join(claudeDir, 'skills', pluginName);
  const configPath = resolve(values.config || process.env.DATALAKE_OTEL_CONFIG || join(home, '.config', 'doda-datalake', 'otel.json'));
  const markerPath = join(destination, markerName);
  const markerText = await readOptional(markerPath);
  let exists = false;
  try {
    const info = await lstat(destination);
    exists = true;
    if (!info.isDirectory() || info.isSymbolicLink()) fail('기존 설치 경로가 일반 디렉터리가 아니므로 변경하지 않습니다.');
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (exists && (!markerText || JSON.parse(markerText).owner !== 'doda-datalake-otel-installer')) {
    fail('설치 도구가 소유하지 않은 디렉터리는 덮어쓰거나 삭제하지 않습니다.');
  }
  if (exists) {
    for (const path of ['.claude-plugin', 'hooks', 'claude-code', ...ownedFiles, markerName]) {
      try {
        if ((await lstat(join(destination, path))).isSymbolicLink()) fail('설치 경로 내부의 심볼릭 링크를 따라 파일을 변경하지 않습니다.');
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
  }
  if (!values.uninstall) {
    const settings = await readOptional(join(claudeDir, 'settings.json'));
    // Read only: even comments and formatting in existing settings remain byte-for-byte intact.
    if (settings) {
      let disabled;
      try { disabled = JSON.parse(settings).enabledPlugins?.[`${pluginName}@skills-dir`] === false; }
      catch { disabled = /"doda-datalake-otel@skills-dir"\s*:\s*false/.test(settings); }
      if (disabled) fail('이 플러그인은 사용자 설정에서 명시적으로 비활성화되어 있습니다. claude plugin enable doda-datalake-otel@skills-dir 실행 후 다시 설치하세요.');
    }
    for (const file of ownedFiles.slice(0, -1)) await lstat(join(root, file));
  }
  console.log(JSON.stringify({ client: 'claude-code', action: values.uninstall ? 'uninstall' : 'install', apply: !!values.apply, destination, ...(values.uninstall ? {} : { configPath, plugin: `${pluginName}@skills-dir` }) }, null, 2));
  if (!values.apply) return;
  if (values.uninstall) {
    if (!exists) return;
    for (const file of [...ownedFiles, markerName]) await unlink(join(destination, file)).catch(error => { if (error.code !== 'ENOENT') throw error; });
    for (const directory of ['.claude-plugin', 'hooks', 'claude-code', '']) {
      await rmdir(join(destination, directory)).catch(error => { if (!['ENOENT', 'ENOTEMPTY', 'EEXIST'].includes(error.code)) throw error; });
    }
    console.log('플러그인 소유 파일만 제거했습니다. 사용자 설정과 로컬 세션 메타데이터는 보존됩니다.');
    return;
  }
  await mkdir(destination, { recursive: true, mode: 0o700 });
  await writeFile(markerPath, JSON.stringify({ owner: 'doda-datalake-otel-installer', version: 1 }) + '\n', { mode: 0o600 });
  for (const file of ownedFiles.slice(0, -1)) {
    await mkdir(dirname(join(destination, file)), { recursive: true, mode: 0o700 });
    await copyFile(join(root, file), join(destination, file));
  }
  await writeFile(join(destination, 'claude-code', 'config-path.json'), JSON.stringify({ configPath }) + '\n', { mode: 0o600 });
  console.log('설치 완료: Claude Code를 재시작하거나 /reload-plugins를 실행하세요. 조직 정책이나 프로젝트의 명시적 비활성화 설정은 우회하지 않습니다.');
}

if (isMain(import.meta.url)) {
  install().catch(error => { console.error(error.code === 'DATALAKE_INSTALL' ? error.message : 'Claude Code 플러그인 설치를 완료하지 못했습니다. 경로 소유권, 비활성화 설정 및 파일 접근 권한을 확인하세요.'); process.exitCode = 1; });
}
