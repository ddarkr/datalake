#!/usr/bin/env node
import { mkdir, readFile, writeFile, unlink, lstat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve, relative, isAbsolute } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const marker = '// Managed by doda-datalake OMP telemetry installer.\n';
const extensionURL = new URL('./index.mjs', import.meta.url).href;
const help = `사용법: node plugins/omp/install.mjs [--home DIR] [--profile NAME] [--config PATH] [--apply] [--uninstall]
기본값은 미리보기입니다. --apply만 파일을 변경합니다.
OMP_PROFILE/PI_PROFILE과 PI_CODING_AGENT_DIR을 따릅니다. --home 사용 시 외부 경로는 거부합니다.
config.yml은 변경하지 않고 공식 extensions/*.js 자동 검색 경로에 전용 로더를 설치합니다.
패키지 디렉터리를 이동/삭제하지 마세요. 제거: 같은 대상에 --uninstall --apply.
`;

function profileName(value) {
  const name = value?.trim();
  if (!name || name === 'default') return undefined;
  if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(name) || name.endsWith('.') || /^(con|prn|aux|nul|com[0-9]|lpt[0-9])(\.|$)/i.test(name)) {
    throw new Error('올바르지 않은 OMP 프로필 이름입니다.');
  }
  return name;
}

export function agentDirectory({ home = homedir(), profile, isolated = false, env = process.env } = {}) {
  const active = profileName(profile ?? env.OMP_PROFILE ?? env.PI_PROFILE);
  const root = join(resolve(home), env.PI_CONFIG_DIR || '.omp');
  let agentDir = active ? join(root, 'profiles', active, 'agent') : join(root, 'agent');
  if (!active && env.PI_CODING_AGENT_DIR) {
    // dirs.ts does not inherit PI_CODING_AGENT_DIR derived from a bypassed PI_PROFILE.
    const legacy = profileName(env.PI_PROFILE);
    const legacyDir = legacy ? join(root, 'profiles', legacy, 'agent') : undefined;
    if (env.PI_CODING_AGENT_DIR !== legacyDir) agentDir = resolve(env.PI_CODING_AGENT_DIR);
  }
  if (isolated) {
    const rel = relative(resolve(home), agentDir);
    if (rel === '..' || rel.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`) || isAbsolute(rel)) {
      throw new Error('--home 외부의 OMP 경로를 거부했습니다. PI_CODING_AGENT_DIR/PI_CONFIG_DIR을 해제하거나 격리 경로로 설정하세요.');
    }
  }
  return agentDir;
}

function loader(configPath, moduleURL = extensionURL, profile) {
  const options = { ...(configPath ? { configPath } : {}), ...(profile ? { profile } : {}) };
  return marker + `import extension from ${JSON.stringify(moduleURL)};\nexport default pi => extension(pi, ${JSON.stringify(options)});\n`;
}

function ownedLoader(text) {
  if (!text.startsWith(marker)) return false;
  const match = text.slice(marker.length).match(/^import extension from (".*");\nexport default pi => extension\(pi, (.*)\);\n$/);
  if (!match) return false;
  try {
    const url = JSON.parse(match[1]);
    const options = JSON.parse(match[2]);
    return typeof url === 'string' && url.startsWith('file:') && options &&
      Object.keys(options).every(key => key === 'configPath' || key === 'profile') &&
      (options.configPath === undefined || typeof options.configPath === 'string') &&
      (options.profile === undefined || typeof options.profile === 'string') &&
      text === loader(options.configPath, url, options.profile);
  } catch { return false; }
}

export async function install(args = process.argv.slice(2), env = process.env) {
  const { values } = parseArgs({ args, options: {
    home: { type: 'string' }, profile: { type: 'string' }, config: { type: 'string' },
    apply: { type: 'boolean', default: false }, uninstall: { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  } });
  if (values.help) { console.log(help); return; }
  const home = resolve(values.home ?? homedir());
  const directory = agentDirectory({ home, profile: values.profile, isolated: values.home !== undefined, env });
  const path = join(directory, 'extensions', 'doda-datalake-otel.js');
  // Refuse symlinks anywhere in the target ancestry: --home must not mutate
  // an external user tree through a pre-existing extensions/profile symlink.
  let ancestor = path;
  while (true) {
    try {
      if ((await lstat(ancestor)).isSymbolicLink()) throw new Error('심볼릭 링크 설치 경로를 거부했습니다.');
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    const parent = dirname(ancestor);
    if (parent === ancestor || ancestor === home) break;
    ancestor = parent;
  }
  let existing;
  try { existing = await readFile(path, 'utf8'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (existing !== undefined && !ownedLoader(existing)) throw new Error('기존 파일이 관리 로더와 다릅니다. 변경하지 않았습니다.');
  const configPath = values.config ? resolve(values.config) : undefined;
  const content = loader(configPath, extensionURL, directory);
  const migrate = existing === loader(configPath) && existing !== content;
  if (!values.uninstall && existing !== undefined && existing !== content && !migrate) {
    throw new Error('설치 옵션이 기존 로더와 다릅니다. 먼저 --uninstall --apply로 제거하세요.');
  }
  const action = values.uninstall ? '제거' : '설치';
  console.log(`${values.apply ? '적용' : '미리보기'}: OMP 로더 ${action}: ${path}`);
  if (!values.apply) return { path, changed: false };
  if (values.uninstall) {
    if (existing !== undefined) await unlink(path);
  } else if (existing === undefined) {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, content, { flag: 'wx', mode: 0o600 });
  } else if (migrate) {
    await writeFile(path, content, { mode: 0o600 });
  }
  return { path, changed: values.uninstall ? existing !== undefined : existing === undefined || migrate };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  install().catch(() => {
    console.error('OMP 설치 실패: 인수/권한/기존 로더 충돌 또는 격리 경로를 확인하세요. 다른 설정은 변경하지 않았습니다.');
    process.exitCode = 1;
  });
}
