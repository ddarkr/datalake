# 코딩 에이전트를 데이터레이크에 연결하기

Codex, OMP, opencode2, Claude Code, AGY CLI(Antigravity)에서 관측한 토큰·세션·도구 실행 메타데이터를 데이터레이크로 보냅니다. 프롬프트, 답변·추론 원문, 도구 인자·결과, 파일 내용은 보내지 않습니다.

AGY의 `error.type`은 고정 `tool_error`로 보내며, 서버도 trace·log·datapoint의 나머지 `error.type`을 `reported_error`, trace·log의 `error_class`를 `reported_error`로 제한합니다. 오류 분류에 섞인 원문·비밀 유출을 막기 위해 상세 진단 문자열은 의도적으로 보존하지 않습니다.

**설치와 실제 연결은 별개입니다.** 아래 순서대로 **수집 주소 선택 → 인증 설정 → 연결 확인 → 원하는 도구 설치 → 새 세션 확인**을 진행하세요. 설치기는 기존의 다른 설정을 보존하며, `--apply`가 없으면 미리보기만 합니다.

이 문서의 셸 명령은 macOS/Linux의 bash 또는 zsh 기준입니다. `plugins/` 안이 아니라 **이 저장소의 최상위 디렉터리**에서 실행하세요.

## 1. 준비하기

필요한 항목:

- Node.js 22 이상과 npm, 사용할 코딩 에이전트. 플러그인은 호스트 또는 훅 프로세스 안에서 직접 HTTP로 전송하며 별도 sender를 실행하지 않습니다.
- 서버의 `server` 프로필이 기동된 데이터레이크와 OTLP **HTTP** 수집 주소.
- 서버 관리자가 제공한 `OTLP_USER`, `OTLP_PASSWORD`. Grafana나 DB 로그인 비밀번호와는 다릅니다.
- 다른 컴퓨터의 서버라면 HTTPS 주소 또는 SSH 접속 권한. 평문 전송 위험을 수용하는 사설 IPv4 연결은 아래 D를 참고하세요.

```bash
node --version
npm ci --prefix plugins
```

OMP·opencode2·AGY는 이 저장소의 플러그인 경로를 직접 참조합니다. **설치 후 저장소를 이동하거나 삭제하지 마세요.** Codex와 Claude는 실행 파일을 복사하므로 저장소 코드만 갱신해서는 설치본이 바뀌지 않습니다. 같은 설정·홈으로 설치기를 다시 실행하고 해당 도구를 재시작·재로딩하세요. 모든 플러그인의 기존 미전송 큐는 자동 이관·삭제되지 않습니다.

## 2. 수집 주소 선택하기

아래 네 가지 중 자신의 환경에 맞는 **한 가지**를 선택하세요. 주소에는 비밀번호를 넣지 않습니다. `/v1/traces`는 설정기가 자동으로 붙입니다.

> `127.0.0.1`은 항상 **코딩 에이전트를 실행하는 컴퓨터 자신**입니다. 노트북에서 원격 서버의 `127.0.0.1`로 직접 접속할 수는 없습니다.

### A. 에이전트와 데이터레이크가 같은 컴퓨터에 있을 때

서버의 `OTLP_HTTP_PORT`가 기본값 `4318`이라면:

```bash
export OTLP_ENDPOINT='http://127.0.0.1:4318'
```

서버에서 포트를 변경했다면 그 값으로 바꾸세요. `4317`은 gRPC용이므로 이 플러그인에는 사용하지 않습니다. Grafana 주소도 수집 주소가 아닙니다.

### B. 원격 서버가 localhost에만 수집 포트를 열어 둔 경우 — SSH 터널

서버의 공개 바인딩을 바꾸지 않고 연결하는 방법입니다. **별도 터미널**에서 실행하고 연결 중에는 켜 두세요.

다음 예시의 `ssh-user@server.example`은 실제 SSH 계정·호스트로 바꾸세요. 오른쪽 `4318`은 서버의 `OTLP_HTTP_PORT`이며, 기본 Compose 설정과 다르면 배포 설정을 확인하세요.

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L 127.0.0.1:24318:127.0.0.1:4318 ssh-user@server.example
```

그런 다음 설치를 진행하는 원래 터미널에서:

```bash
export OTLP_ENDPOINT='http://127.0.0.1:24318'
```

- `24318`: 내 컴퓨터에서 사용할 비어 있는 포트입니다.
- 원격 `127.0.0.1:4318`: **SSH로 접속한 서버**의 localhost와 해당 서버의 `OTLP_HTTP_PORT`입니다(기본 Compose는 `4318`, 실제 배포 설정을 확인하세요). SSH 호스트와 데이터레이크 호스트가 같다는 전제입니다.
- 터널을 종료하면 전송도 끊깁니다. 서버 포트를 `0.0.0.0`으로 바꾸거나 SSH 호스트 키 검사를 끄지 마세요.

### C. 관리자가 제공한 HTTPS 수집 주소가 있을 때

아래 예시를 실제 OTLP HTTP 주소로 바꾸세요. 브라우저 로그인 페이지로 리디렉션되는 관리 화면 주소는 사용할 수 없습니다.

```bash
export OTLP_ENDPOINT='https://collector.example/otlp'
```

플러그인은 기본적으로 **HTTPS 또는 localhost HTTP만 허용**합니다. 사설 인증서를 쓰면 신뢰할 CA를 설정해야 하며, 인증서 검증을 끄는 방법은 권장하지 않습니다.

### D. 평문 전송 위험을 수용하고 내부망 HTTP를 사용할 때

**인증 비밀번호와 메타데이터가 암호화되지 않습니다.** 서버의 `OTLP_HTTP_BIND_ADDRESS`를 해당 서버의 LAN IP로 지정하고 재배포하세요. `0.0.0.0` 바인딩이나 공유기의 외부 포트 포워딩은 사용하지 마세요. DB·Grafana·gRPC 포트는 변경할 필요가 없습니다.

```bash
export OTLP_ENDPOINT='http://192.168.99.10:4318'
```

주소와 포트는 실제 배포에 맞게 바꾸세요. **3단계의 두 `configure.mjs` 명령에 `--allow-insecure-http`를 추가**해야 합니다. 이 옵션은 사설 IPv4(`10/8`, `172.16/12`, `192.168/16`)만 허용하며, 공인 IP나 DNS 이름의 HTTP는 여전히 거부합니다. 허용 여부는 생성된 private JSON에 저장되고, 다른 설정 파일의 기본 보안 정책은 바뀌지 않습니다.
OMP에만 적용하려면 3단계의 설정 파일명을 `omp-arcane.json`처럼 구분하고, OMP 설치 시 그 파일을 `--config`로 지정하세요. SSH 터널 프로세스는 필요 없습니다. OMP의 종료 동작은 아래 OMP 항목을 참고하세요.

## 3. 인증을 안전하게 설정하기

### 인증 정보 파일 준비

에이전트 컴퓨터에는 서버의 전체 `.env`를 복사할 필요가 없습니다. 특히 S3·DB 비밀번호를 함께 옮기지 마세요. 사용자 전용 디렉터리에 파일을 만들고 권한을 제한합니다.

```bash
mkdir -p "$HOME/.config/doda-datalake"
touch "$HOME/.config/doda-datalake/otlp.env"
chmod 600 "$HOME/.config/doda-datalake/otlp.env"
```

이 파일을 편집기로 열어 **서버에 설정된 실제 값**을 입력하세요. 다음은 파일 형식 예시이며 터미널에 실행하는 명령이 아닙니다.

```dotenv
OTLP_USER='관리자에게 받은 사용자명'
OTLP_PASSWORD='관리자에게 받은 비밀번호'
```

비밀번호를 명령 인자, 채팅, 스크린샷, Git에 남기지 마세요. dotenv에서 `#` 등이 값으로 들어가면 따옴표로 감싸고, 값 자체에 따옴표가 있다면 해당 값을 보존할 수 있는 dotenv 표기를 사용하세요.

### 미리보기 후 저장

2단계에서 `OTLP_ENDPOINT`를 설정한 터미널에서 실행합니다.

```bash
export DATALAKE_OTEL_CONFIG="$HOME/.config/doda-datalake/otel.json"

node plugins/configure.mjs \
  --endpoint "$OTLP_ENDPOINT" \
  --config "$DATALAKE_OTEL_CONFIG" \
  --from-env-file "$HOME/.config/doda-datalake/otlp.env"
```

대상 파일과 `authentication: configured`를 확인한 다음 저장합니다.

```bash
node plugins/configure.mjs \
  --endpoint "$OTLP_ENDPOINT" \
  --config "$DATALAKE_OTEL_CONFIG" \
  --from-env-file "$HOME/.config/doda-datalake/otlp.env" \
  --apply
```

`otel.json`은 권한 `0600`으로 생성됩니다. **인증 정보가 들어 있으므로 내용을 출력하거나 공유하지 마세요.** 이미 파일이 있으면 덮어쓰지 않습니다. 의도적으로 주소·인증을 변경할 때만 같은 명령에 `--replace`를 추가하세요. 교체할 인증 정보도 다시 지정해야 합니다.

이미 로컬에 서버의 `.env`가 있다면 `--from-env-file .env`를 사용할 수도 있습니다. 설정기는 그중 `OTLP_USER`와 `OTLP_PASSWORD`만 가져옵니다.

## 4. 도구를 설치하기 전에 연결 확인하기

다음 명령은 저장한 인증을 사용해 **빈 OTLP 요청**을 보냅니다. 가짜 세션이나 토큰 사용량을 만들지 않고, 인증 헤더와 서버 응답 본문도 출력하지 않습니다.

```bash
node --input-type=module <<'NODE'
import { loadConfig } from './plugins/otel.mjs';
try {
  const config = loadConfig();
  if (!config) throw new Error('missing config');
  const response = await fetch(config.endpoint, {
    method: 'POST', redirect: 'error',
    headers: { ...config.headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ resourceSpans: [] }),
    signal: AbortSignal.timeout(config.timeoutMs),
  });
  await response.body?.cancel();
  console.log(`HTTP ${response.status}: ${response.ok ? '수집기 연결 확인' : '인증 또는 수집 경로 확인 필요'}`);
  if (!response.ok) process.exitCode = 1;
} catch {
  console.error('연결 실패: 설정 파일 권한, 수집 주소, SSH 터널, 인증서와 timeout을 확인하세요.');
  process.exitCode = 1;
}
NODE
```

정상 OTLP 수집기라면 `HTTP 200: 수집기 연결 확인`이 나옵니다. **이는 HTTP 수집 경로 확인이지 DB 저장·Grafana 반영의 증거는 아닙니다.** 실제 세션은 6단계에서 확인합니다.

## 5. 원하는 도구에 설치하기

다섯 도구를 모두 설치할 필요는 없습니다. 사용할 도구의 명령만 실행하세요. 아래의 `--apply`를 빼면 변경할 위치를 먼저 확인할 수 있습니다. 설치기에 설정 파일 경로를 전달하므로 새 터미널에서도 같은 파일을 사용합니다. 아래 명령은 사용자가 적용할 절차이며, 구현 검증 과정에서 실제 사용자 설치나 재시작을 대신 수행하지는 않습니다.

### Codex

```bash
node plugins/install.mjs codex --config "$DATALAKE_OTEL_CONFIG" --apply
codex plugin list --json
```

`doda-datalake-codex`가 활성 상태인지 확인하세요. **Codex를 재시작하고 `/hooks`에서 설치된 훅을 검토·신뢰해야 합니다.** 설치기는 신뢰·샌드박스 검사를 우회하지 않습니다.

`codex`가 PATH에 없다면 실제 실행 파일을 지정하세요. 아래 경로는 예시이므로 자신의 설치 경로로 바꿉니다. 플러그인 조회에도 같은 실행 파일을 사용하세요.

```bash
node plugins/install.mjs codex \
  --codex '/실제/설치/경로/codex' \
  --config "$DATALAKE_OTEL_CONFIG" --apply
```

기본 수집 소스는 `plugin`입니다. **동일 세션의 Codex 네이티브 OTel을 같은 데이터레이크로 함께 보내지 마세요.** `--source native`는 이 플러그인의 전송을 모두 끄는 선택이며, 네이티브 OTel 설정을 대신 구성해 주지는 않습니다.

Codex는 현재 transcript의 숫자 사용량과 메타데이터를 직접 전송합니다. 관측 checkpoint와 중복 방지 상태는 전송 시도를 기록하며, 수집기 저장 증거가 아닙니다. 실패한 이벤트를 다음 훅에서 자동 재시도하지 않습니다.

### OMP

```bash
node plugins/install.mjs omp --config "$DATALAKE_OTEL_CONFIG" --apply
```

사용 중인 OMP를 종료하고 다시 시작하세요. **플러그인 코드 갱신 후에도 이미 실행 중인 OMP는 재시작해야 합니다.** `config.yml`은 수정하지 않고 확장 검색 경로에 전용 로더를 추가합니다. 설치기는 `--profile`·`OMP_PROFILE`·`PI_PROFILE`·`PI_CODING_AGENT_DIR`로 설치 대상의 active agent 디렉터리를 선택할 뿐, 생성된 로더에는 런타임 profile을 전달하지 않고 설정 경로만 담습니다. 같은 설정·같은 디렉터리의 profile 포함 기존 관리 로더는 재설치로 갱신하지만, 다른 사용자 로더나 다른 설정·디렉터리의 기존 로더는 임의로 덮어쓰지 않습니다. 소스만 갱신해도 실행 중인 OMP 호스트가 멈추지 않으므로 새 전송 방식을 쓰려면 정상 재시작이 필요합니다. 갱신 전에 전송하지 못한 과거 도구 호출은 자동 복구되지 않습니다.

`work` 같은 이름 있는 프로필을 쓴다면 **설치와 실행에 같은 프로필**을 지정합니다.

```bash
node plugins/install.mjs omp --profile work --config "$DATALAKE_OTEL_CONFIG" --apply
omp --profile work
```

`OMP_PROFILE`, `PI_PROFILE`, `PI_CODING_AGENT_DIR`도 설치 대상에 영향을 줍니다. 미리보기의 경로가 맞는지 확인하세요. 설치한 확장을 다시 `-e`로 중복 지정하지 말고, 자동 로딩을 막는 `--no-extensions`도 사용하지 마세요.

기존 네이티브 **트레이스**가 같은 수집 주소로 전송되는 경우 토큰 사용량은 네이티브 쪽에 맡깁니다. 메트릭·로그만 켰거나 다른 주소로 보내는 경우에는 플러그인 사용량 전송을 유지합니다.

OMP는 `createDirectTelemetry()`로 프로세스 안에서 직접 비동기 HTTP 전송만 합니다. 새 OMP 이벤트는 outbox나 sender로 가지 않으며, 같은 공유 계약의 설정·직렬화·검증·개인정보·timeout으로 즉시 HTTP 요청 하나를 시작합니다. `send()`의 `true`는 수집기 전체 ACK이며, 비활성·무효·timeout·거부·부분 성공은 `false`입니다. 디스크 저장·프로세스 분리·자동 재시도·대기열 상한이 없고, 진행 중인 요청만 `flush()`가 기다립니다. 각 identity는 실패해도 한 번만 시도합니다. HTTP timeout은 기본 2000ms(허용 100–30000ms)이며, 종료는 이 범위 안의 진행 중 요청만 기다립니다. 보조 Git 정보는 500ms 안에 준비되지 않으면 생략합니다.

오프라인·실패·강제 종료 때 전송하지 못한 OMP 이벤트는 유실될 수 있으며, 새로운 영속 기록·재시도·sender를 만들지 않습니다. 기존 `~/.local/state/doda-datalake/otel` OMP 큐는 자동으로 비우거나 옮기거나 지우지 않고 그대로 둡니다. 소스만 갱신해도 실행 중인 OMP 호스트·sender가 멈추지 않으므로, 새 직접 전송을 쓰려면 OMP를 정상 재시작하세요. 대기열을 임의로 삭제하지 마세요.

### opencode2

```bash
node plugins/install.mjs opencode2 --config "$DATALAKE_OTEL_CONFIG" --apply
```

일반 OpenCode v1이 아니라 **네이티브 OpenCode 2**용입니다. 설정의 `plugins`에 플러그인 **디렉터리**를 등록하며 기존 JSONC 주석과 다른 플러그인을 보존합니다.

`OPENCODE_CONFIG_DIR`가 설정되어 있으면 그 경로에 설치됩니다. Orca 실행 환경에서는 `~/Library/Application Support/orca/opencode-hooks/shared`가 사용될 수 있으므로, 미리보기의 설치 경로를 확인하세요. 다른 실행 환경의 기본 설정까지 자동으로 수정하지는 않습니다.

이미 떠 있는 opencode2 클라이언트와 해당 서버를 정상 종료한 뒤 다시 시작하세요. 백그라운드 서버를 여러 클라이언트가 공유하고 있다면 다른 작업이 끝난 뒤 재시작합니다. 독립 실행으로 확인하려면:

```bash
opencode2 run --standalone --format json 'OK라고만 답해 주세요.'
```

기존에 모델과 인증이 설정되어 있어야 합니다. 실제 모델 사용료가 발생할 수 있습니다. `--standalone`은 기존 공유 서버 대신 별도 서버로 실행합니다.

`opencode2 plugin list`에서 `doda.datalake.otel`을 확인할 수 있습니다. 단, 2.0.12에서는 **위치 초기화 직후의 일회성 플러그인 API 조회가 활성화 완료 전에 빈 목록을 반환할 수 있습니다.** 빈 목록 한 번만으로 설치 실패라고 판단하지 말고 실제 세션 실행과 수신도 확인하세요.

원시 이벤트 bus는 HTTP 응답을 기다리지 않고 허용된 메타데이터를 직접 전송합니다. 별도 메타데이터 버퍼·용량 상한·로컬 저장 재시도는 없습니다. 설정이 비활성화된 경우 bus를 구독하지 않습니다. 정상 cleanup은 진행 중인 HTTP 요청을 설정된 timeout 범위 안에서 기다립니다.

### Claude Code

```bash
node plugins/install.mjs claude-code --config "$DATALAKE_OTEL_CONFIG" --apply
claude plugin list --json
```

`doda-datalake-otel@skills-dir`가 활성 상태인지 확인합니다. Claude Code를 재시작하거나 `/reload-plugins`를 실행하세요. 사용자 skills 디렉터리에 설치하며 기존 `settings.json`은 보존합니다.

조직 정책, `disableAllHooks`, 명시적인 플러그인 비활성화는 우회하지 않습니다. Claude는 훅 프로세스에서 `OTEL_*` 환경 변수를 제거하므로, 이 가이드처럼 **private 설정 파일과 `--config` 경로**를 사용하세요.

현재 세션 transcript는 관측 checkpoint와 시도한 identity로 중복을 막습니다. 전송 실패도 자동 재처리하지 않으며 checkpoint는 수집기 ACK를 뜻하지 않습니다. 병행 훅은 세션별로 직렬화하며 살아 있는 프로세스의 lock을 시간만으로 빼앗지 않습니다. `SessionStart`는 기존 transcript의 EOF에서 시작하므로 재개한 과거 대화는 수집하지 않습니다.

`--source plugin|native`는 사용량 소유자를 명시적으로 선택합니다(기본 `plugin`). `native`는 transcript LLM 사용량을 보내지 않고 상호작용 훅은 유지합니다. 선택은 `OTEL_*` 환경 변수가 제거되어도 남는 설치본 `config-path.json`에 저장되며, 재설치 때 생략하면 기존 선택을 유지합니다. 네이티브 OTel 활성화·인증 설정은 사용자가 별도로 해야 하며 설치기가 대신 켜지 않습니다. 새 `UserPromptSubmit`·`MessageDisplay` 훅은 같은 홈·설정으로 **재설치한 뒤 재시작 또는 `/reload-plugins`**해야 등록됩니다.

### AGY CLI (Antigravity)

```bash
node plugins/install.mjs agy --config "$DATALAKE_OTEL_CONFIG" --apply
```

`~/.gemini/config/plugins/doda-datalake/`에 플러그인과 훅이 등록되며, `~/.gemini/config/config.json`의 플러그인 목록에 활성화됩니다. 수동으로 구성한 같은 전역 plugins 디렉터리도 계속 지원합니다. 설치기는 `--config` → `DATALAKE_OTEL_CONFIG` → 기존 `~/.config/doda-datalake/agy-arcane.json` → `otel.json` 순서로 설정을 선택하고, 선택한 절대 경로를 **모든 훅 명령의 `--config`에 고정**합니다. 명시한 파일 대신 다른 파일을 몰래 선택하지 않습니다. 수동 훅도 같은 절대 스크립트·설정 경로를 지정하세요.

`PreToolUse`는 시작 메타데이터를 기록하고, `PostToolUse`와 `Stop`은 훅 프로세스 안에서 직접 HTTP 전송을 시도합니다. 훅 응답은 도구 동작을 바꾸지 않는 중립 응답(`PreToolUse`의 `allow`, 나머지 `{}`)입니다. 설정·코드를 갱신했다면 AGY를 다시 시작해 새 훅을 로드하세요.

`PreToolUse`·`PreInvocation`처럼 시작 checkpoint만 기록하는 경로에서는 Git을 조회하지 않습니다. 전송할 이벤트를 구성할 때 저장소·브랜치를 조회하며 프로세스 사이에 메타데이터를 캐시하지 않습니다.

도구 오류는 원문 대신 고정 분류 `error.type=tool_error`만 전송합니다. transcript cursor와 도구·Stop 관측 상태는 중복 시도를 막기 위한 소스 checkpoint이며 수집기 ACK가 아닙니다. HTTP 실패 때문에 같은 구간이나 완료 이벤트를 자동 재전송하지 않습니다.

AGY는 토큰·캐시 사용량을 문자 수로 추정하지 않습니다. 보고된 사용량이 없는 invocation에는 토큰·tier·TTFT·첫 텍스트 필드를 보내지 않습니다. 새 `PreInvocation`·`PostInvocation` 훅을 등록하려면 같은 홈·설정으로 설치기를 다시 실행한 뒤 AGY를 재시작하세요.

### 공통: 직접 HTTP 전송

JavaScript 어댑터는 `createDirectTelemetry()`의 `send(event)`로 허용된 메타데이터를 안정적 OTLP identity로 직렬화해 HTTP 요청 하나를 보냅니다. `true`는 수집기의 전체 ACK이며 비활성·무효 이벤트·HTTP 거부·timeout·부분 거부는 `false`입니다. 영속 큐·별도 sender·자동 재시도·완료 tombstone·전송 용량 상한은 없습니다. Hermes도 Python 훅에서 직접 HTTP를 사용합니다.

OMP·opencode2의 이벤트 처리는 HTTP를 기다리지 않고 정상 종료 때 진행 중인 요청만 기다립니다. Codex·Claude·AGY의 단기 훅은 직접 전송 완료나 timeout을 기다리고 종료합니다. JavaScript HTTP timeout은 기본 2000ms, 허용 범위 100–30000ms입니다. Hermes의 timeout 범위는 [Hermes 가이드](hermes/README.md)를 따릅니다.

실패·오프라인·강제 종료 시 이벤트가 유실될 수 있습니다. 전송 실패 뒤 자동 복구하지 않으며, checkpoint나 프로세스 종료가 수집기 저장을 증명하지 않습니다. 서버 Alloy 큐는 서버에 도착한 이후만 보호합니다.

설정은 JavaScript exporter 생성 때 읽습니다. 설정·인증·주소를 바꾼 장기 실행 호스트는 재시작·재로딩해야 합니다. 단기 훅은 다음 실행 때 새 설정을 읽습니다. 프롬프트·답변·추론·도구 인자/결과·파일 내용과 인증 헤더는 telemetry 본문에 넣지 않습니다.

기존 `~/.local/state/doda-datalake/otel`, Codex·Claude data root의 `outbox/`, Hermes의 기존 SQLite 큐는 읽거나 재전송·삭제하지 않습니다. 실행 중인 이전 호스트·sender는 소스 갱신만으로 종료되지 않습니다. 기존 큐를 임의로 삭제하거나 잠금을 지우지 마세요. 설치본을 갱신하고 정상 재시작으로 전환하세요.

Codex의 `metadata/`, Claude의 `sessions/`, AGY의 임시 세션 상태는 사용량 관측·중복 방지용으로 남습니다. 전송용 큐가 아닙니다. 해당 상태를 삭제하면 과거 관측의 중복 방지에 영향을 줄 수 있습니다. 소스 checkpoint 잠금은 PID뿐 아니라 OS 부팅·프로세스 시작 identity를 확인하며 살아 있는 owner를 나이만으로 회수하지 않습니다.

### 성능 필드와 해석

아래는 구현이 읽을 수 있는 관측값입니다. 해당 upstream 이벤트·속성이 실제로 없으면 필드도 없습니다. 지원 목록은 현재 프로필 설치, 실제 공급자 호출, 운영 저장 확인을 뜻하지 않습니다.

| 도구 / 사용량 소유자 | 시간 관측 | 설정·사용량과 한계 |
|---|---|---|
| OMP plugin / 선택한 native | plugin: `message.duration`은 `provider_call`, `message.ttft`는 검증된 Responses 경로에서 `first_output_item`, 나머지는 `provider_reported`; `message_update`의 첫 비어 있지 않은 text delta와 명확한 단일 입력만 상호작용 측정. native chat span은 `logical_call` | wire 또는 harness의 요청 tier·effort·명시 budget. `message.serviceTier`는 provider 응답이 아니라 요청 fallback일 수도 있어 **harness tier**로만 저장. native 사용량 소유 시 plugin LLM 사용량 중복 생략 |
| Codex plugin / opt-in native | transcript 사용량은 시점 이벤트. native completion log의 공급자 보고 TTFT만 보존하며 **같은 요청의 duration은 없음** | 보고 사용량 및 native 요청 설정. 별도 `api_request`를 시간 근접성으로 연결하지 않음; request-total throughput 계산 불가 |
| Claude plugin / opt-in native | native `llm_request` duration은 `logical_call`, TTFT는 `provider_reported`, `first_content_ms`는 vendor content block. 훅은 `hook_submit_to_display_ready` | native speed는 **요청 tier**, effort는 요청 설정. 훅의 동일 `prompt_id`, 유효 turn/message ID, 첫 non-final batch 0만 허용. human origin·UI paint는 알 수 없으며 SDK/`-p` final-only display는 생략 |
| OpenCode2 plugin | 첫 output part는 `first_output_part`; ephemeral `session.text.delta`의 첫 비어 있지 않은 delta는 harness 첫 텍스트; `step.started`→`step.streamed`가 `stream` duration | 보고 사용량과 명시 model variant. `step.ended`까지의 tool settle을 stream 시간에 포함하지 않음. inboxID→응답 매핑이 없어 입력 latency 생략; tier/effort를 variant에서 추론하지 않음 |
| Hermes plugin | `started_at`→`ended_at`은 retry 포함 `logical_call`; `first_chunk_at`은 첫 SDK/SSE **event**인 `first_stream_event`, 첫 text 아님 | `pre_api_request`에서 요청 tier·effort·명시 budget만 allowlist. 일부 provider/auxiliary 경로에는 관측값이 없고 실제 tier echo 없음. usage가 없어도 latency 유지; 무효 usage는 token으로 내보내지 않음 |
| AGY plugin | 같은 conversation+invocationNum의 pre/post **훅 호출 간격**인 `hook_invocation` | 모델 네트워크 시간·첫 token·첫 text가 아님. 토큰·tier·effort·TTFT를 생성하지 않으며 invocation 시간으로 TPS 계산하지 않음 |

시간 필드는 모두 **밀리초**입니다. native OMP `gen_ai.response.time_to_first_chunk`의 초 값만 collector가 ×1000으로 변환합니다. `ttft_ms`는 반드시 `coding_agent.ttft.kind`와 함께 해석하고 첫 text·사용자 응답·UI paint와 혼용하지 않습니다. native OMP는 정확한 OpenAI Codex 경로만 `first_output_item`, generic OpenAI는 `provider_reported`입니다. `coding_agent.signal_source`는 `datalake-plugin`/`native`, `request.settings_source`는 `wire`/`harness`/`native`이며 서로 다른 출처 의미입니다. harness version도 실제 보고값만 저장합니다.

요청 tier(`request.service_tier`), 공급자가 echo한 실제 tier(`response.service_tier`), fallback 가능 harness tier(`harness.service_tier`)는 별개입니다. 요청 `auto`가 실제 처리 tier를 증명하지 않습니다. reasoning effort는 관측된 설정 enum이고 budget은 명시 정수 토큰 수, model variant는 독립 라벨입니다. 모델 이름·variant·tier에서 다른 값을 추정하지 않습니다.

누락·NULL은 **unknown**, 숫자 0은 **관측된 0**입니다. 새 canonical token은 `usage.source=reported`일 때만 보고 사용량이며, reasoning은 output 상세로 중복 합산하지 않습니다. 네이티브 사용량 소유권은 사용자의 opt-in이고 설치가 이를 자동 변경하지 않습니다.

Grafana의 **request-total output throughput**은 같은 기록의 보고 output tokens ÷ (`request.duration_ms`/1000), 양의 duration 및 `provider_call`/`logical_call`/`stream`만 대상으로 합니다. 생성 구간 TPS가 아니며 TTFT를 빼지 않습니다. 같은 workload·모델/variant·effort·요청/실제 tier·캐시·출력 길이·provider·동시성·timing kind·signal source를 맞추지 않은 도구 순위는 공정한 비교가 아닙니다. 서로 다른 trace/log를 시간 근접성으로 조인하지 않습니다.

OMP·OpenCode2는 저장소 소스를 갱신한 뒤 실행 호스트/서버를 재로딩해야 합니다. Hermes는 설치 위치에 소스를 다시 복사하고 해당 프로필을 재시작합니다. Codex·Claude의 복사된 설치본과 AGY의 새 훅 등록은 재설치가 필요합니다. 이 안내는 설치 실행 지시나 live profile 변경 기록이 아닙니다. nullable schema·Alloy projection·Grafana 변경은 별도로 승인된 서버 배포가 필요하며 이번 로컬 작업으로 배포하지 않았습니다.

#### 확인한 upstream 계약

소스 조사 기준은 [OMP 18.11.0 / c2cb79e4](https://github.com/can1357/oh-my-pi/tree/c2cb79e4c227aa4b22c4ccb933ff72fd88bceba4)(`packages/ai/src/types.ts`, extension types, agent telemetry), [Codex 0.160.0](https://github.com/openai/codex/blob/rust-v0.160.0/codex-rs/otel/src/events/session_telemetry.rs), [OpenCode2 v2.0.12 / 2670273](https://github.com/anomalyco/opencode/tree/2670273ff17da96f85c5826ced57aa1b368754fa), [Hermes f42f579c](https://github.com/NousResearch/hermes-agent/tree/f42f579cf8bac4918ac9599bece71618afadd846), [Claude monitoring](https://code.claude.com/docs/en/monitoring-usage)·[hooks](https://code.claude.com/docs/en/hooks), [공식 AGY hooks](https://antigravity.google/docs/hooks.md)입니다. Claude native `first_content_ms`는 2.1.268+, effort는 2.1.274+ 계약이며 skills-dir 설치기는 2.1.223+를 요구합니다. 설치 가능 최소 버전이 모든 새 native 필드 지원을 뜻하지 않습니다.

이 핀은 **조사한 upstream 계약**입니다. 로컬 adapter fixture/loopback 검증, 실제 upstream 바이너리 실행, 실제 공급자 호출은 별도의 증거가 필요합니다. 아래 과거 검증 기록을 이번 성능 필드의 최종 gate 통과나 live profile 갱신으로 해석하지 마세요.

## 6. 실제 사용량이 보이는지 확인하기

1. 연결 확인이 성공한 상태에서, 설치한 도구의 **새 세션**을 엽니다.
2. “OK라고만 답해 주세요”처럼 짧은 요청을 한 번 보냅니다. 실제 모델 호출에는 비용이 발생할 수 있습니다.
3. 응답을 기다리고 세션을 정상 종료합니다. 정상 종료는 진행 중인 직접 전송을 기다리지만, 실패한 이벤트는 보존·재시도하지 않습니다. 도구 호출을 사용하지 않았다면 도구 호출 통계가 없는 것이 정상입니다. 호스트 종료나 cursor 전진만으로 HTTP 전송 완료를 판단하지 마세요.
4. Grafana에서 일별 요약을 확인할 때는 **최근 7일**로 설정하고 새로고침합니다. 최근 15분처럼 오늘의 시작 시각을 제외하는 범위는 당일 일별 요약을 숨길 수 있습니다.
   - **Overview → Last OTel span**: 최근 수신 시각 확인.
   - **AI Usage → Usage by client & model**: 클라이언트·모델별 토큰 확인.
   - **AI Sessions → Session ledger**: 세션 확인.
   - **AI Tools**: 실제 도구를 사용한 경우 호출 통계 확인.
5. 원시 span은 보이는데 요약이 없다면 서버의 집계 작업이 반영될 때까지 기다립니다. 즉시 반영을 보장하지는 않습니다.

표의 클라이언트 이름은 Codex=`codex`, OMP=`oh-my-pi`, opencode2=`opencode`, Claude Code=`claude-code`, AGY=`agy`입니다. Overview의 최신 시각은 다른 클라이언트의 데이터일 수도 있으므로 클라이언트별 요약까지 확인하세요.

Grafana 역시 원격 localhost에만 열려 있다면 별도 SSH 터널을 사용할 수 있습니다. 예를 들어 원격 Grafana 호스트 포트가 기본 Compose 값인 `3000`인 경우:

```bash
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:23000:127.0.0.1:3000 ssh-user@server.example
```

브라우저에서 `http://127.0.0.1:23000`에 접속합니다. 기본 Compose의 Grafana 호스트 포트는 `3000`이며, 실제 배포의 `GF_HTTP_PORT`에 맞춰 오른쪽 포트를 바꾸세요. 로그인은 Grafana 계정을 사용합니다.

## 문제가 생겼을 때

| 증상 | 먼저 확인할 것 |
|---|---|
| `HTTP 401` 또는 `403` | OTLP 계정이 서버와 같은지 확인. Grafana·DB 계정과 혼동하지 않았는지 확인. 프록시의 추가 접근 정책도 확인. 비밀번호를 로그에 출력하지 말 것. |
| `404`, `405` 또는 리디렉션 상태 | OTLP HTTP 주소인지 확인. 프록시가 최종 `/v1/traces` POST 경로를 전달하는지 확인. Grafana·Arcane 관리 주소는 사용하지 않음. |
| 연결 실패 또는 timeout | SSH 터널이 살아 있는지, 원격 포트가 실제 `OTLP_HTTP_PORT`인지, 서버가 기동되어 있는지 확인. TLS 인증서 신뢰도 확인. |
| OMP `handler timed out after 2000ms` | 갱신된 확장을 로드하도록 OMP 재시작. 새 OMP 전송은 진행 중 HTTP 요청만 범위 안에서 기다리며 outbox·sender를 쓰지 않으므로, 계속 발생하면 수집기 응답 지연이나 종료 때 쌓인 진행 중 요청을 확인. HTTP timeout을 늘려 해결하지 말 것. |
| `Export disabled: private configuration unavailable` | 지정한 파일이 존재하는지, JSON 형식·소유권·권한이 올바른지 확인. `chmod 600 "$DATALAKE_OTEL_CONFIG"`로 권한 제한. symlink·공유 파일을 사용하지 말고 파일 내용은 공유하지 말 것. |
| `Configuration already exists` | 재설정이 맞는지 확인 후 configure 명령에 `--replace` 추가. 기존 인증을 자동 승계하지 않으므로 인증 파일도 다시 지정. |
| 설치는 됐는데 새 span이 없음 | 실제로 사용하는 홈·프로필에 설치했는지, 재시작했는지 확인. Codex `/hooks` 신뢰, OMP 확장 비활성화 옵션, Claude 정책·비활성화 상태 확인. |
| opencode2 관리 포트 충돌 | 기존 사용자 서버를 강제 종료하지 말 것. `run --standalone`으로 분리하여 확인하거나 정상적인 서버 관리 절차로 해결. |
| 토큰은 있지만 비용이 비어 있음 | Codex·Claude 훅에는 비용 원본이 없어 누락 상태로 둠. 0원이라는 뜻이 아님. OMP·opencode2의 보고 비용도 청구서와 같다고 보장하지 않음. |
| 오프라인 동안 사용량 누락 | 모든 플러그인은 직접 전송하며 실패·오프라인·강제 종료 시 유실될 수 있음. 자동 재시도나 영속 전송 기록 없음. 연결·인증·터널을 확인하고 새 이벤트로 검증. |
| `Accepted telemetry remains pending` / `pending-*` / `Local telemetry queue unavailable or full` / `busy` | 이전 설치본이나 이미 실행 중인 구 호스트·sender에서 나오는 경고. 설치본을 갱신하고 호스트를 정상 재시작. 기존 큐나 lock을 임의로 삭제하지 말 것. |

서버 관리자가 추가 진단할 때는 서버의 Compose 디렉터리에서 `docker compose logs --since 10m alloy aggregate`를 확인할 수 있습니다. 로그를 공유하기 전 민감한 정보가 없는지 검토하세요.

이전 큐에는 미전송 데이터와 중복 방지 기록이 남아 있습니다. 새 직접 전송 구현은 이를 자동 복구하거나 삭제하지 않습니다. 코드 갱신만으로 실행 중인 구 sender가 종료되지 않으며, 이 문서는 큐 삭제나 사용자 프로세스 강제 종료를 권하지 않습니다.

## 제거하기

필요한 도구의 명령만 실행합니다. 미리보기는 `--apply`를 빼세요.

```bash
node plugins/install.mjs codex --uninstall --apply
node plugins/install.mjs omp --uninstall --apply
node plugins/install.mjs opencode2 --uninstall --apply
node plugins/install.mjs claude-code --uninstall --apply
node plugins/install.mjs agy --uninstall --apply
```

- OMP는 설치 때 지정한 `--profile`을 제거 때도 지정합니다. 별도 홈에 설치했다면 같은 `--home`을 사용합니다.
- Codex 실행 파일을 따로 지정했다면 `--codex`도 동일하게 지정합니다.
- 제거 후 해당 클라이언트·서버를 재시작하세요.
- 공용 `otel.json`과 원본 인증 파일은 다른 도구가 사용할 수 있어 자동으로 지우지 않습니다.
- Codex의 재설치용 로컬 marketplace, 관측 checkpoint와 이전 버전의 outbox·tombstone은 남을 수 있습니다. 등록 제거와 데이터 삭제는 별개이며, 이미 실행 중인 구 sender도 등록 제거만으로 종료되지 않습니다. 새 직접 전송 구현은 sender를 만들지 않습니다.

## 검증한 범위와 한계

이전 영속 큐 구현은 **fresh home의 실제 Codex 0.160.0, Claude Code 2.1.223, OMP 18.5.1, OpenCode 2.0.12, AGY 1.2.16**에 설치해 검증한 기록이 있습니다. macOS sandbox와 loopback 합성 LLM을 사용한 이 결과는 현재 직접 전송의 종료·재시도 동작을 증명하지 않습니다. 당시 보류한 ACK 뒤 독립 sender가 전송한 결과와 OMP 자식 span 검증은 구 구현의 기록입니다.

처음 받은 native payload 11개를 격리된 로컬 Alloy·Greptime에 두 번씩 replay했을 때 raw row는 22개, unique span identity는 11개였고 집계의 root 세션별 billable turn은 1개였습니다. 이는 안정적 identity와 해당 집계 경로의 중복 제거 확인이지 raw 저장의 exactly-once 보장이 아닙니다. 합성 사용량은 네 클라이언트가 입력/출력 5/2를 반영했지만 AGY는 기존 transcript 추정 1/1을 유지했고 네이티브 Gemini CLI의 5/2와 달랐습니다. 실제 공급자 청구·운영 전송·다른 OS의 호스트 종료까지 검증한 결과로 확대하지 않습니다.

현재 직접 전송의 회귀 검사와 실제 loopback HTTP 검증 명령·한계는 [개발 가이드](../docs/development.md)를 참고하세요. 사용자 설치·재시작과 운영 수집기 도착 여부는 4·6단계로 별도 확인하세요.

아래는 이전 설치·연결 검증 기록입니다. 현재 비차단 구현의 검증이나 현재 사용자 프로필의 설치 상태와는 구분합니다.

| 도구 | 확인한 버전 | 실제 검증 범위 |
|---|---|---|
| OMP | 18.2.6 | 실제 기본 프로필의 `gpt-6-astra` 호출을 운영 Arcane 수집기로 전송. GreptimeDB 원본 trace와 자동 session summary의 토큰·추정 비용 일치 확인. OpenAI 복합 도구 ID(`call_id\|item_id`) 거부 오류 수정 후 실제 `read` 1회가 원본 tool span, `ai_tool_daily`의 `oh-my-pi/read = 1`, Grafana AI Tools까지 반영되는 것 확인 |
| opencode2 | 2.0.12 | 격리 환경의 실제 `run --standalone` 전송 검증에 더해 Orca 실행 설정에 설치, 네이티브 API의 플러그인 `active`와 운영 수집기 인증 요청 `200` 확인. 사용자 모델 호출은 공급자의 `402` 크레딧 부족으로 완료하지 못함 |
| Claude Code | 2.1.223 | 실제 플러그인 인식·훅 자동 실행과 토큰·세션 전송 확인. 모델 응답은 로컬 테스트 서버 사용 |
| Codex | 0.142.4 / 0.154.0-alpha.6.2 | 설치·격리 훅 검증에 더해 실제 사용자 설정에 설치하고 네이티브 `/hooks`에서 해당 플러그인 7개 훅 신뢰. 실제 모델 호출 성공과 자동 훅의 숫자 사용량 cursor 확인. 운영 수집기 인증 요청 `200` 확인 |
| AGY CLI | 2.0+ (CLI) | 실제 사용자 환경 전역 플러그인 설치 및 네이티브 라이프사이클 훅(`PreToolUse`, `PostToolUse`, `Stop`) 연동. 실제 모델 호출과 도구 실행의 `duration_ms` 실측정 확인. 트랜스크립트 기반 `llm.turn` 및 서브에이전트 계층 트레이스, 세션 요약 스팬이 운영 수집기로 정상 전송됨을 확인. 전송 대상·측정치는 비공개 배포 기록에만 둠 |

회귀 검사와 OMP의 운영 연결을 검증했습니다. **OMP 확인 결과가 나머지 도구의 현재 프로필 설치나 운영 전송까지 증명하지는 않습니다.** 이 문서의 4·6단계로 각 연결을 확인하세요. LAN HTTP 검증에서는 인증 없는 요청의 `401`, 인증 요청의 `200`, 실제 DB 저장과 GitOps 주기 이후 설정 유지까지 확인했습니다.

추가 Codex·opencode2 설치 검증에서는 Arcane 브라우저 relay 연결이 끊겨 서버 DB를 다시 조회하지 못했습니다. 인증 요청 성공이나 로컬 usage cursor를 DB 저장·집계 확인과 동일하게 취급하지 않습니다. 기존 공유 서버는 중단하지 않았으므로 실행 중이던 클라이언트·서버는 별도 재시작이 필요합니다.

Codex·Claude는 현재 세션의 로컬 transcript에서 숫자 사용량만 추출합니다. 기록이 꺼져 있거나 마지막 기록이 훅보다 늦게 쓰이고 이후 훅이 실행되지 않으면 일부 사용량이 누락될 수 있습니다. 과거 대화 전체를 스캔하지 않습니다. 도구 버전이 달라지면 훅·이벤트 계약을 다시 확인해야 합니다.

현재 모든 플러그인은 실패한 전송을 영속 보관하거나 자동 재시도하지 않습니다. 관측 checkpoint와 실제 수집기 ACK·DB 저장·집계는 구분하세요. 소스 기록이 늦게 쓰이거나 훅이 실행되지 않은 경우의 누락도 자동 복구하지 않습니다. 전송·집계 성공과 실제 청구를 혼동하지 마세요.
