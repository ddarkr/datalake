# 코딩 에이전트를 데이터레이크에 연결하기

Codex, OMP, opencode2, Claude Code, AGY CLI(Antigravity)에서 관측한 토큰·세션·도구 실행 메타데이터를 데이터레이크로 보냅니다. 프롬프트, 답변·추론 원문, 도구 인자·결과, 파일 내용은 보내지 않습니다.

AGY의 `error.type`은 고정 `tool_error`로 보내며, 서버도 trace·log·datapoint의 나머지 `error.type`을 `reported_error`, trace·log의 `error_class`를 `reported_error`로 제한합니다. 오류 분류에 섞인 원문·비밀 유출을 막기 위해 상세 진단 문자열은 의도적으로 보존하지 않습니다.

**설치와 실제 연결은 별개입니다.** 아래 순서대로 **수집 주소 선택 → 인증 설정 → 연결 확인 → 원하는 도구 설치 → 새 세션 확인**을 진행하세요. 설치기는 기존의 다른 설정을 보존하며, `--apply`가 없으면 미리보기만 합니다.

이 문서의 셸 명령은 macOS/Linux의 bash 또는 zsh 기준입니다. `plugins/` 안이 아니라 **이 저장소의 최상위 디렉터리**에서 실행하세요.

## 1. 준비하기

필요한 항목:

- Node.js 22 이상과 npm, 사용할 코딩 에이전트. 별도 sender에도 실제 Node 실행 파일이 필요합니다. Bun 기반 호스트라고 해서 Bun을 sender로 쓰지는 않습니다.
- 서버의 `server` 프로필이 기동된 데이터레이크와 OTLP **HTTP** 수집 주소.
- 서버 관리자가 제공한 `OTLP_USER`, `OTLP_PASSWORD`. Grafana나 DB 로그인 비밀번호와는 다릅니다.
- 다른 컴퓨터의 서버라면 HTTPS 주소 또는 SSH 접속 권한. 평문 전송 위험을 수용하는 사설 IPv4 연결은 아래 D를 참고하세요.

```bash
node --version
npm ci --prefix plugins
```

OMP와 opencode2는 이 저장소의 플러그인 경로를 참조하며, AGY 설치 훅도 저장소의 `plugins/agy/hook.mjs`를 절대 경로로 호출합니다. **설치 후 저장소를 이동하거나 삭제하지 마세요.** Codex와 Claude는 설치 과정에서 공유 outbox·sender를 포함한 실행 파일을 복사하므로, 저장소 코드만 갱신해서는 설치본이 바뀌지 않습니다. 갱신하려면 같은 설정·홈으로 설치기를 다시 실행하고 해당 도구의 재시작·재로딩 절차를 따르세요.

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

OMP에만 적용하려면 3단계의 설정 파일명을 `omp-arcane.json`처럼 구분하고, OMP 설치 시 그 파일을 `--config`로 지정하세요. SSH 터널 프로세스는 필요 없습니다. OMP의 종료·재전송 동작은 아래 OMP 항목을 참고하세요.

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

다섯 도구를 모두 설치할 필요는 없습니다. 사용할 도구의 명령만 실행하세요. 아래의 `--apply`를 빼면 변경할 위치를 먼저 확인할 수 있습니다. 설치기에 설정 파일 경로를 전달하므로 새 터미널에서도 같은 파일을 사용합니다. 아래 명령은 사용자가 적용할 절차이며, 현재 구현 검증 과정에서 실제 사용자 설치나 재시작을 대신 수행하지는 않았습니다.

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

Codex 메타데이터의 로컬 수락 영수증과 숫자 사용량 cursor는 수집기 응답이 아니라 영속 outbox 저장 성공 뒤 기록합니다. 같은 메타데이터 identity와 같은 사용량 cursor만 각각 잠그므로 서로 다른 완료 이벤트를 HTTP 전송 때문에 직렬화하지 않습니다. 로컬 저장을 거부한 사용량은 숫자 기준선과 해당 offset을 전진시키지 않습니다.

### OMP

```bash
node plugins/install.mjs omp --config "$DATALAKE_OTEL_CONFIG" --apply
```

사용 중인 OMP를 종료하고 다시 시작하세요. **플러그인 코드 갱신 후에도 이미 실행 중인 OMP는 재시작해야 합니다.** `config.yml`은 수정하지 않고 확장 검색 경로에 전용 로더를 추가합니다. 설치 로더는 설치 대상의 active agent 디렉터리를 profile identity로 고정합니다. 같은 설정의 기존 관리 로더가 profile을 담지 않은 이전 형식이면 재설치로 갱신하지만, 다른 사용자 로더나 다른 설치 옵션을 임의로 덮어쓰지는 않습니다. 갱신 전에 큐에 저장되지 않은 과거 도구 호출은 자동 복구되지 않습니다.

`work` 같은 이름 있는 프로필을 쓴다면 **설치와 실행에 같은 프로필**을 지정합니다.

```bash
node plugins/install.mjs omp --profile work --config "$DATALAKE_OTEL_CONFIG" --apply
omp --profile work
```

`OMP_PROFILE`, `PI_PROFILE`, `PI_CODING_AGENT_DIR`도 설치 대상에 영향을 줍니다. 미리보기의 경로가 맞는지 확인하세요. 설치한 확장을 다시 `-e`로 중복 지정하지 말고, 자동 로딩을 막는 `--no-extensions`도 사용하지 마세요.

기존 네이티브 **트레이스**가 같은 수집 주소로 전송되는 경우 토큰 사용량은 네이티브 쪽에 맡깁니다. 메트릭·로그만 켰거나 다른 주소로 보내는 경우에는 플러그인 사용량 전송을 유지합니다.

OMP 이벤트는 먼저 사용자 전용 영속 큐에 저장하고, 별도 Node sender가 HTTP 전송과 재시도를 처리합니다. 종료 훅은 로컬 저장과 sender의 private pipe에 bootstrap 전달이 끝날 때까지만 기다립니다. sender 초기화 ACK나 수집기의 HTTP 응답은 기다리지 않으며, bootstrap을 받은 sender는 OMP가 종료된 뒤에도 전송을 계속합니다. 보조 Git 정보는 500ms 안에 준비되지 않으면 생략합니다. 로컬 디스크 I/O나 Node 런타임 탐색 자체가 지연되면 종료가 늦어질 수 있습니다.

sender 시작에 실패해도 이미 저장한 이벤트는 대기열에 남습니다. 다음 이벤트가 들어오면 sender 시작을 다시 시도합니다. 큐가 가득 차거나 로컬 저장에 실패한 이벤트까지 보존되는 것은 아니므로 진단 경고를 확인하고, 대기열을 임의로 삭제하지 마세요.

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

원시 이벤트 bus는 HTTP 전송이나 로컬 handoff를 기다리지 않고 계속 읽으며, 허용된 메타데이터로 즉시 변환한 span만 최대 256개 메모리 버퍼에 둡니다. 버퍼가 가득 차면 기존 항목을 버리지 않고 새 메타데이터를 거부하며 경고합니다. 이 버퍼 자체는 영속 큐가 아니므로 outbox 저장 전 강제 종료에는 남지 않습니다. 설정이 비활성화된 경우에는 bus를 구독하지 않습니다. 정상 cleanup은 남은 span마다 마지막 로컬 저장을 시도하고 `flushLocal()`만 기다리며 sender의 HTTP 완료는 기다리지 않습니다.

### Claude Code

```bash
node plugins/install.mjs claude-code --config "$DATALAKE_OTEL_CONFIG" --apply
claude plugin list --json
```

`doda-datalake-otel@skills-dir`가 활성 상태인지 확인합니다. Claude Code를 재시작하거나 `/reload-plugins`를 실행하세요. 사용자 skills 디렉터리에 설치하며 기존 `settings.json`은 보존합니다.

조직 정책, `disableAllHooks`, 명시적인 플러그인 비활성화는 우회하지 않습니다. Claude는 훅 프로세스에서 `OTEL_*` 환경 변수를 제거하므로, 이 가이드처럼 **private 설정 파일과 `--config` 경로**를 사용하세요.

현재 세션 transcript의 cursor는 해당 batch의 모든 이벤트가 **로컬 outbox에 영속 수락된 뒤** 확정합니다. 일부 로컬 저장이 실패하거나 예외가 발생하면 다음 훅에서 같은 구간을 다시 읽으며, 이미 수락된 identity는 다시 큐에 넣지 않습니다. 이전 `sent-*` 영수증은 기존 수집기 ACK의 증거로 보존하고, 새 `queued-v2-*` 영수증은 로컬 수락만 뜻하도록 구분합니다. 병행 훅은 세션별로 직렬화하며 살아 있는 프로세스의 lock을 시간만으로 빼앗지 않습니다. `SessionStart`는 기존 transcript의 EOF에서 시작하므로 재개한 과거 대화는 수집하지 않습니다.

### AGY CLI (Antigravity)

```bash
node plugins/install.mjs agy --config "$DATALAKE_OTEL_CONFIG" --apply
```

`~/.gemini/config/plugins/doda-datalake/`에 플러그인과 훅이 등록되며, `~/.gemini/config/config.json`의 플러그인 목록에 활성화됩니다. 수동으로 구성한 같은 전역 plugins 디렉터리도 계속 지원합니다. 설치기는 `--config` → `DATALAKE_OTEL_CONFIG` → 기존 `~/.config/doda-datalake/agy-arcane.json` → `otel.json` 순서로 설정을 선택하고, 선택한 절대 경로를 **모든 훅 명령의 `--config`에 고정**합니다. 명시한 파일 대신 다른 파일을 몰래 선택하지 않습니다. 수동 훅도 같은 절대 스크립트·설정 경로를 지정하세요.

`PreToolUse`는 시작 메타데이터를 기록하고, `PostToolUse`와 `Stop`은 span을 로컬 영속 큐에 넘깁니다. 훅 응답은 도구 동작을 바꾸지 않는 중립 응답(`PreToolUse`의 `allow`, 나머지 `{}`)이며, 전부 `async: true`로 바꾸거나 이벤트마다 독립 HTTP worker를 만드는 방식이 아닙니다. 설정·코드를 갱신했다면 AGY를 다시 시작해 새 훅을 로드하세요.

도구 오류는 원문 대신 고정 분류 `error.type=tool_error`만 전송합니다. 증분 턴과 해당 서브에이전트가 모두 **로컬 수락**되어야 cursor를 전진시킵니다. version 2 cursor는 과거 ACK를 뜻하는 `sentEventIds`를 유지하면서 새 `queuedEventIds`를 별도로 저장합니다. 도구 상태도 version 2에서 step 번호만이 아닌 전체 도구 identity로 구분하고, 첫 완료의 시각·duration·본문과 queued 영수증을 보존합니다. `Stop`의 첫 세션 요약도 고정해 로컬 재시도 때 다시 계산하지 않습니다.

AGY의 토큰 값은 기존 transcript 기반 **추정치**입니다. 네이티브 API의 정확한 사용량이나 실제 청구 토큰과 같다고 보장하지 않습니다.

### 공통: 로컬 수락과 독립 sender

다섯 Node 어댑터의 `createTelemetry()`는 읽기 전용 `enabled`와 `enqueue(event)`, `flushLocal()`만 제공합니다. 허용된 메타데이터를 안정적 OTLP trace/span identity와 wire 본문으로 **먼저 직렬화한 뒤** outbox에 저장합니다. 기존 wire 형식과 identity 규칙은 유지합니다. `enqueue()`의 `true`는 로컬 파일·디렉터리 fsync를 포함한 영속 수락이며 수집기 HTTP ACK가 아닙니다. `flushLocal()` 역시 이미 시작한 로컬 저장·sender bootstrap 작업을 마칠 뿐 HTTP를 flush하지 않습니다. 비차단은 **HTTP를 기다리지 않는다**는 뜻이지, 로컬 디스크·잠금·런타임 탐색·private pipe 전달이 지연될 수 없다는 뜻이 아닙니다. OMP는 pipe 전달까지만, 나머지는 sender 준비 ACK까지 기다릴 수 있습니다.

sender는 클라이언트·설정 소스·profile·endpoint hash로 구분한 route마다 하나의 소유 worker가 pending만 처리합니다. 초기 시작 경합에서 생긴 추가 후보는 소유권을 얻지 못하면 종료합니다. 부모가 종료돼도 worker는 계속 재시도하며, pending이 없어지면 새 enqueue와 원자적으로 handoff하고 종료합니다. 상시 설치 daemon은 아닙니다. sender가 죽으면 이후 같은 route의 이벤트가 시작을 재시도하지만, 후속 실행 없이 프로세스 재시작·호스트 재부팅 뒤에도 반드시 전달된다는 보장은 없습니다.

#### 홈·프로필별 저장 위치

각 outbox는 아래 기준 디렉터리 안의 `<route-hash>/`에 있습니다. 기본 기준 디렉터리는 `$XDG_STATE_HOME/doda-datalake/otel`, 변수가 없으면 `~/.local/state/doda-datalake/otel`입니다. 생성 시 홈·설정·profile을 캡처하므로 이후 환경 변수를 바꾼다고 이미 생성한 route가 이동하지 않습니다.

| 도구 | 기준 디렉터리와 profile |
|---|---|
| Codex | 캡처한 data root의 `outbox/`. data root는 `PLUGIN_DATA`, 없으면 `${CODEX_HOME:-~/.codex}/doda-datalake-state`이며 이 root가 profile입니다. 소스 메타데이터·cursor는 같은 root의 `metadata/`에 둡니다. |
| Claude Code | 캡처한 data root의 `outbox/`. data root는 `CLAUDE_PLUGIN_DATA`, 없으면 `${CLAUDE_CONFIG_DIR:-~/.claude}/plugins/data/doda-datalake-otel`이며 이 root가 profile입니다. 소스 checkpoint는 `sessions/`에 둡니다. |
| OMP | 기본 기준 디렉터리. 설치 로더가 캡처한 active agent 디렉터리(기본 `~/.omp/agent`, 이름 있는 프로필은 `~/.omp/profiles/<profile>/agent`)가 profile입니다. 큐가 extensions 디렉터리에 생기는 것은 아닙니다. `PI_CONFIG_DIR`·`PI_CODING_AGENT_DIR`와 설치 대상도 구분하세요. |
| AGY | 기본 기준 디렉터리의 **영속 outbox**. 훅에서 캡처한 `homedir()`가 profile입니다. 기존 transcript cursor·도구·Stop 소스 상태는 OS 임시 디렉터리의 `doda-datalake-agy/<session>/`에 남는 별도 상태이므로 임시 파일 정리는 아직 수락되지 않은 구간의 복구에 영향을 줄 수 있습니다. |
| opencode2 | 기본 기준 디렉터리와 기본 profile `default`; 플러그인 `options.stateRoot`·`options.profile`이 있으면 이를 사용합니다. 설치기가 저장한 `options.configPath`는 setup에서 캡처합니다. `OPENCODE_CONFIG_DIR`는 설치할 OpenCode 설정 위치이며 outbox의 직접 저장 위치가 아닙니다. |

#### 주소 고정과 인증 교체

route manifest에는 설정 파일 경로와 endpoint hash를 고정하며 인증 헤더나 실제 수집 URL을 spool에 쓰지 않습니다. 파일 설정 sender는 **매 전송 시도마다** private 설정 파일을 다시 읽습니다. 같은 endpoint에서 인증만 교체하면 pending에도 새 인증을 사용합니다. endpoint를 바꾸면 예전 pending을 새 서버로 보내지 않고 `pending-config`로 남깁니다. 새 설정을 읽은 새 호스트·훅은 새 route를 만들 수 있지만 예전 큐의 자동 이관은 없습니다. 기존 호스트가 캡처한 설정과 새 route 선택을 갱신하려면 해당 도구의 재시작·재로딩이 필요합니다. 파일이 아닌 환경·inline 설정은 private bootstrap으로 전달한 설정을 사용하므로 실행 중 파일 인증 교체와 같은 동작을 기대하지 마세요.

전송 실패는 1초부터 최대 60초까지 backoff하며 계속 보존합니다. HTTP timeout은 기본 2000ms, 허용 범위 100–30000ms이고 부모 종료 대기시간이 아닙니다. HTTP 거부·timeout, OTLP partial success의 span 거부, 잘못된 응답은 완료로 처리하지 않습니다. 같은 identity는 첫 수락 본문을 유지하며, 다시 enqueue해도 새 레코드를 만들지 않고 기존 수락을 `true`로 확인합니다. 완전한 수집기 성공 응답 뒤에는 본문을 지우고 identity의 `done` tombstone을 남겨 재전송을 막습니다. 수집기가 저장했지만 응답이 유실되면 같은 identity가 다시 전송될 수 있으므로 end-to-end exactly-once는 아닙니다.

#### 용량·권한·진단

기본 한도는 route당 **pending + done 합계 100,000 identities**, 레코드 파일 바이트 합계 **128MiB**, 단일 레코드 **1MiB**입니다. tombstone도 합계와 바이트에 포함되며 자동 삭제·오래된 항목 퇴출이 없습니다. 코드 옵션 `maxEntries`·`maxBytes`는 첫 생성 때 manifest에 고정되며 기존 큐의 live resize가 아닙니다. 기존 한도와 다른 값을 주면 거부합니다. 공통 로컬 동시 작업은 128개까지이며, opencode2의 별도 256-span 버퍼와는 다릅니다. 용량 초과나 로컬 저장 실패는 `false`이므로 성공한 것처럼 cursor를 전진시키지 않지만, 이후 소스 훅이 없거나 소스 기록이 사라진 미수락 이벤트까지 보존하지는 못합니다.

큐 디렉터리는 `0700`, 파일은 `0600`인 현재 사용자 소유로 제한하고 안전하지 않은 symlink·hardlink·권한·소유권은 거부합니다. **private는 암호화가 아닙니다.** pending에는 토큰 수·모델·도구명·세션/요청 identity·시각·허용된 브랜치 메타데이터 등이 평문으로 남습니다. 프롬프트·답변·추론·도구 인자/결과·파일 내용·원시 호스트 이벤트와 인증 헤더는 outbox에 저장하지 않습니다. 설정 파일 자체에는 인증이 있으므로 큐와 설정을 통째로 업로드하거나 Git에 넣지 마세요.

sender는 프로젝트 내부나 안전하지 않은 PATH 후보 대신 소유권·권한을 확인한 프로젝트 밖의 실제 Node 절대 경로를 사용하고, 별도 private cwd·stdio에서 실행합니다. 전달 환경도 제한하므로 agent의 비밀·preload·프록시·TLS 검증 해제 설정을 그대로 물려주지 않습니다. 신뢰할 Node가 없으면 이미 수락한 이벤트는 pending에 남습니다. 내장 어댑터 옵션 `nodeBinary`를 직접 지정하는 경우에는 지정자가 신뢰할 실행 파일을 선택할 책임이 있습니다.

잠금 소유자는 PID뿐 아니라 OS 부팅·프로세스 시작 identity로 확인합니다. Linux는 `/proc`, macOS는 시간·출력 크기를 제한한 기본 JXA의 libproc 조회와 `sysctl`을 사용합니다. probe가 막히면 PID-only legacy owner로 보수적으로 처리하며, 살아 있는 PID의 identity를 확인할 수 없다고 잠금을 시간만으로 빼앗지 않습니다. 특히 재사용된 PID와 legacy lock의 조합은 자동 회수가 지연될 수 있습니다. 잠금 삭제를 일반 해결책으로 쓰지 마세요.

각 route의 `sender-status.json`은 최대 256바이트의 고정 상태(`pending-http`, `pending-config`, `pending-launch`, `idle`)와 timestamp만 기록합니다. 원시 오류·응답·URL·인증은 기록하지 않으며, 다음 producer가 generic pending 경고를 낼 수 있습니다. 상태는 마지막 기록이지 프로세스 생존이나 DB 저장 증거가 아닙니다. 전용 큐 관리 CLI나 자동 prune 명령은 없습니다.

## 6. 실제 사용량이 보이는지 확인하기

1. 연결 확인이 성공한 상태에서, 설치한 도구의 **새 세션**을 엽니다.
2. “OK라고만 답해 주세요”처럼 짧은 요청을 한 번 보냅니다. 실제 모델 호출에는 비용이 발생할 수 있습니다.
3. 응답을 기다리고 세션을 정상 종료합니다. 세션 종료 시점에 로컬 큐에 넣는 정보도 있습니다. 도구 호출을 사용하지 않았다면 도구 호출 통계가 없는 것이 정상입니다. 호스트 종료나 로컬 cursor 전진만으로 HTTP 전송 완료를 판단하지 마세요.
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
| OMP `handler timed out after 2000ms` | 갱신된 확장을 로드하도록 OMP 재시작. 종료 시 sender 초기화·HTTP 응답은 기다리지 않으며, 계속 발생하면 로컬 디스크·큐 잠금·Node 탐색 지연 확인. HTTP timeout을 늘려 해결하지 말 것. |
| `Export disabled: private configuration unavailable` | 지정한 파일이 존재하는지, JSON 형식·소유권·권한이 올바른지 확인. `chmod 600 "$DATALAKE_OTEL_CONFIG"`로 권한 제한. symlink·공유 파일을 사용하지 말고 파일 내용은 공유하지 말 것. |
| `Configuration already exists` | 재설정이 맞는지 확인 후 configure 명령에 `--replace` 추가. 기존 인증을 자동 승계하지 않으므로 인증 파일도 다시 지정. |
| 설치는 됐는데 새 span이 없음 | 실제로 사용하는 홈·프로필에 설치했는지, 재시작했는지 확인. Codex `/hooks` 신뢰, OMP 확장 비활성화 옵션, Claude 정책·비활성화 상태 확인. |
| opencode2 관리 포트 충돌 | 기존 사용자 서버를 강제 종료하지 말 것. `run --standalone`으로 분리하여 확인하거나 정상적인 서버 관리 절차로 해결. |
| 토큰은 있지만 비용이 비어 있음 | Codex·Claude 훅에는 비용 원본이 없어 누락 상태로 둠. 0원이라는 뜻이 아님. OMP·opencode2의 보고 비용도 청구서와 같다고 보장하지 않음. |
| 오프라인 동안 사용량 누락 | 다섯 Node 어댑터 모두 로컬 영속 수락된 이벤트는 독립 sender가 재시도. 로컬 저장 실패·용량 초과·버퍼 초과 경고와 sender 시작 실패 확인. 수락 전 누락이나 갱신 전 과거 이벤트는 자동 복구되지 않음. |
| `Accepted telemetry remains pending` / `pending-http` | 수집기 연결·인증·터널을 4단계로 확인. 상세 HTTP 응답은 호스트 훅에서 기다리거나 출력하지 않음. `sender-status.json`의 timestamp도 확인하고 이를 DB 저장 확인으로 취급하지 말 것. |
| `pending-config` | route에 고정된 endpoint와 현재 private 설정의 endpoint가 달라졌거나 설정이 읽히지 않는지 확인. 같은 endpoint의 인증 교체는 다음 시도에 반영되지만 예전 pending을 새 endpoint로 자동 이동하지 않음. |
| `Sender unavailable` / `pending-launch` | 프로젝트 밖에서 신뢰 가능한 실제 Node가 실행되는지, sender 파일·private cwd·잠금 권한이 안전한지 확인. sender가 죽은 뒤에는 이후 같은 route의 producer 실행이 재시작을 시도. |
| `Local telemetry queue unavailable or full` / `busy` | 로컬 디스크·권한·잠금과 pending+done 한도를 확인. tombstone만으로도 한도에 도달할 수 있음. 살아 있는 legacy owner를 시간만으로 지우거나 한도 옵션 변경으로 기존 큐를 확장하려 하지 말 것. |
| opencode2 `local metadata buffer full` | 로컬 저장 거부가 지속되는 원인을 해결. 256-span 메모리 버퍼를 넘은 새 이벤트는 미수락이며 원시 bus backlog를 보관하지 않음. 정상 cleanup도 실패한 로컬 저장을 HTTP 완료까지 기다려 해결하지 않음. |

서버 관리자가 추가 진단할 때는 서버의 Compose 디렉터리에서 `docker compose logs --since 10m alloy aggregate`를 확인할 수 있습니다. 로그를 공유하기 전 민감한 정보가 없는지 검토하세요.

대기열이나 tombstone의 수동 삭제는 복구와 중복 방지 기록을 함께 없앱니다. 용량 문제로 route를 명시적으로 폐기할 때는 관련 producer와 sender를 정상적으로 멈추고, pending의 처리 여부와 private 백업을 먼저 확인하세요. 보존 기간에 따른 자동 prune·eviction은 없으며 이 문서가 큐 삭제나 사용자 서버 강제 종료를 권하지는 않습니다.

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
- Codex의 재설치용 로컬 marketplace와 각 도구의 로컬 메타데이터·outbox·tombstone은 남을 수 있습니다. 플러그인 등록 제거와 데이터 삭제는 별개입니다. 이미 실행 중인 sender도 등록 제거만으로 종료되지는 않습니다.

## 검증한 범위와 한계

현재 비차단 구현은 **fresh home의 실제 Codex 0.160.0, Claude Code 2.1.223, OMP 18.5.1, OpenCode 2.0.12, AGY 1.2.16**에 설치해 검증했습니다. macOS sandbox에서 실제 사용자 데이터 접근·쓰기와 외부 socket 연결을 금지하고 loopback 합성 LLM을 사용했습니다. 다섯 호스트와 검증용 OpenCode 서버가 보류한 HTTP ACK보다 먼저 종료했고, 독립 sender는 ACK를 해제한 뒤 전달했습니다. 실제 OMP `task` 자식 두 개와 `wait`에서 subagent 2개·자식 LLM span 8개도 확인했습니다. 프롬프트 canary는 wire·spool에 없었고 spool에 인증도 남지 않았습니다.

처음 받은 native payload 11개를 격리된 로컬 Alloy·Greptime에 두 번씩 replay했을 때 raw row는 22개, unique span identity는 11개였고 집계의 root 세션별 billable turn은 1개였습니다. 이는 안정적 identity와 해당 집계 경로의 중복 제거 확인이지 raw 저장의 exactly-once 보장이 아닙니다. 합성 사용량은 네 클라이언트가 입력/출력 5/2를 반영했지만 AGY는 기존 transcript 추정 1/1을 유지했고 네이티브 Gemini CLI의 5/2와 달랐습니다. 실제 공급자 청구·운영 전송·다른 OS의 호스트 종료까지 검증한 결과로 확대하지 않습니다.

Node 회귀 검사는 macOS의 실제 Node·Bun과 Linux Node 22에서 실행했고, Linux 환경의 미설치 native Codex·Bun 검사는 skip했습니다. 관련 명령과 coverage는 [개발 가이드](../docs/development.md)를 참고하세요. 현재 검증은 사용자 설치·재시작, 운영 서버 변경, commit·push 없이 진행했습니다. 각자의 실제 연결은 4·6단계로 별도 확인하세요.

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

Claude·AGY의 **미수락** 구간 재처리는 원본 transcript·소스 상태와 후속 훅이 있어야 하지만, 이미 outbox에 영속 수락된 이벤트는 그 원본 없이도 살아 있는 sender가 독립적으로 재시도합니다. 소스 checkpoint의 로컬 수락과 수집기 ACK는 서로 다른 단계입니다. OS owner probe가 불가능한 살아 있는 legacy PID의 lock은 보수적으로 유지하며 나이만으로 회수하지 않습니다. 전송·집계 성공과 실제 청구를 혼동하지 말고, stable identity 재전송이 가능한 at-least-once 경계와 로컬 저장 거부의 한계를 함께 고려하세요.
