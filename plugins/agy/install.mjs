#!/usr/bin/env node
import { mkdir, readFile, writeFile, unlink, copyFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const __dirname = dirname(fileURLToPath(import.meta.url));
const help = `사용법: node plugins/install.mjs agy [--home DIR] [--config PATH] [--apply] [--uninstall]
기본값은 미리보기입니다. --apply만 파일을 변경합니다.
Antigravity (AGY) CLI의 전역 플러그인 (~/.gemini/config/plugins/doda-datalake/)을 설정합니다.
`;

export async function installAgy(options = {}) {
  const home = resolve(options.home || homedir());
  const apply = Boolean(options.apply);
  const uninstall = Boolean(options.uninstall);
  const configPath = options.config ? resolve(options.config) : join(home, '.config', 'doda-datalake', 'agy-arcane.json');

  const geminiDir = join(home, '.gemini', 'config');
  const targetPluginDir = join(geminiDir, 'plugins', 'doda-datalake');
  const configFile = join(geminiDir, 'config.json');

  console.log(`[AGY Datalake Plugin Installer]`);
  console.log(`- 모드: ${apply ? '실제 적용 (--apply)' : '미리보기 (Dry-run)'}`);
  console.log(`- 대상 플러그인 디렉터리: ${targetPluginDir}`);
  console.log(`- 텔레메트리 설정 파일: ${configPath}`);

  if (uninstall) {
    if (!apply) {
      console.log(`[미리보기] ${targetPluginDir} 삭제 예정`);
      return;
    }
    // uninstall
    try {
      await unlink(join(targetPluginDir, 'plugin.json'));
      await unlink(join(targetPluginDir, 'hooks.json'));
      await unlink(join(targetPluginDir, 'hook.mjs'));
    } catch {}
    console.log(`✅ 플러그인 제거 완료`);
    return;
  }

  // 1. 배포될 hooks.json 내용 (저장소의 hook.mjs 절대 경로 참조)
  const hookScriptPath = join(__dirname, 'hook.mjs');
  const hooksConfig = {
    "datalake-telemetry": {
      "PreToolUse": [
        {
          "matcher": "*",
          "hooks": [
            {
              "type": "command",
              "command": `node "${hookScriptPath}" --pre-tool`,
              "timeout": 5
            }
          ]
        }
      ],
      "PostToolUse": [
        {
          "matcher": "*",
          "hooks": [
            {
              "type": "command",
              "command": `node "${hookScriptPath}"`,
              "timeout": 5
            }
          ]
        }
      ],
      "Stop": [
        {
          "type": "command",
          "command": `node "${hookScriptPath}" --stop`,
          "timeout": 15
        }
      ]
    }
  };

  const pluginManifest = {
    "$schema": "https://antigravity.google/schemas/v1/plugin.json",
    "name": "doda-datalake-telemetry",
    "version": "1.0.0",
    "description": "Export AGY CLI AI coding telemetry to Doda DataLake (GreptimeDB + Alloy)",
    "hooks": "./hooks.json"
  };

  if (!apply) {
    console.log(`\n[미리보기] 생성될 파일들:`);
    console.log(`1. ${join(targetPluginDir, 'plugin.json')}`);
    console.log(`2. ${join(targetPluginDir, 'hooks.json')} -> ${hookScriptPath}`);
    console.log(`\n실제 적용하려면 --apply 옵션을 추가하세요.`);
    return;
  }

  // 실제 적용
  await mkdir(targetPluginDir, { recursive: true, mode: 0o755 });
  await writeFile(join(targetPluginDir, 'plugin.json'), JSON.stringify(pluginManifest, null, 2) + '\n', 'utf8');
  await writeFile(join(targetPluginDir, 'hooks.json'), JSON.stringify(hooksConfig, null, 2) + '\n', 'utf8');

  // config.json 갱신 (enabled: true)
  try {
    let currentConfig = {};
    try {
      currentConfig = JSON.parse(await readFile(configFile, 'utf8'));
    } catch {}
    currentConfig.plugins = currentConfig.plugins || {};
    currentConfig.plugins["doda-datalake"] = { enabled: true };
    await writeFile(configFile, JSON.stringify(currentConfig, null, 2) + '\n', 'utf8');
  } catch (err) {
    console.warn(`경고: config.json 업데이트 실패: ${err.message}`);
  }

  console.log(`\n🎉 AGY CLI 전역 데이터레이크 플러그인 설치 완료!`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({
    options: {
      help: { type: 'boolean', short: 'h' },
      apply: { type: 'boolean' },
      uninstall: { type: 'boolean' },
      home: { type: 'string' },
      config: { type: 'string' },
    },
    strict: false,
  });

  if (values.help) {
    console.log(help);
    process.exit(0);
  }

  installAgy(values).catch(err => {
    console.error('오류 발생:', err.message);
    process.exit(1);
  });
}
