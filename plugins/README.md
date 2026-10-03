# 코딩 에이전트를 데이터레이크에 연결하기

Codex, OMP, opencode2, Claude Code, AGY CLI(Antigravity)에서 관측한 토큰·세션·도구 실행 메타데이터를 데이터레이크로 보냅니다. 프롬프트, 답변·추론 원문, 도구 인자·결과, 파일 내용은 보내지 않습니다.

AGY의 `error.type`은 고정 `tool_error`로 보내며, 서버도 trace·log·datapoint의 나머지 `error.type`을 `reported_error`, trace·log의 `error_class`를 `reported_error`로 제한합니다. 오류 분류에 섞인 원문·비밀 유출을 막기 위해 상세 진단 문자열은 의도적으로 보존하지 않습니다.

**설치와 실제 연결은 별개입니다.** 아래 순서대로 **수집 주소 선택 → 인증 설정 → 연결 확인 → 원하는 도구 설치 → 새 세션 확인**을 진행하세요. 설치기는 기존의 다른 설정을 보존하며, `--apply`가 없으면 미리보기만 합니다.

이 문서의 셸 명령은 macOS/Linux의 bash 또는 zsh 기준입니다. `plugins/` 안이 아니라 **이 저장소의 최상위 디렉터리**에서 실행하세요.

## 1. 준비하기

필요한 항목:

- Node.js 22 이상과 npm, 사용할 코딩 에이전트.
- 서버의 `server` 프로필이 기동된 데이터레이크와 OTLP **HTTP** 수집 주소.
- 서버 관리자가 제공한 `OTLP_USER`, `OTLP_PASSWORD`. Grafana나 DB 로그인 비밀번호와는 다릅니다.
- 다른 컴퓨터의 서버라면 HTTPS 주소 또는 SSH 접속 권한. 평문 전송 위험을 수용하는 사설 IPv4 연결은 아래 D를 참고하세요.

```bash
node --version
npm ci --prefix plugins
```

OMP와 opencode2는 이 저장소의 플러그인 경로를 참조합니다. **설치 후 저장소를 이동하거나 삭제하지 마세요.** Codex와 Claude는 설치 과정에서 필요한 실행 파일을 복사합니다.

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

네 도구를 모두 설치할 필요는 없습니다. 사용할 도구의 명령만 실행하세요. 아래의 `--apply`를 빼면 변경할 위치를 먼저 확인할 수 있습니다. 설치기에 설정 파일 경로를 전달하므로 새 터미널에서도 같은 파일을 사용합니다.

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

### OMP

```bash
node plugins/install.mjs omp --config "$DATALAKE_OTEL_CONFIG" --apply
```

사용 중인 OMP를 종료하고 다시 시작하세요. **플러그인 코드 갱신 후에도 이미 실행 중인 OMP는 재시작해야 합니다.** `config.yml`은 수정하지 않고 확장 검색 경로에 전용 로더를 추가합니다. 갱신 전 전송에 실패한 과거 도구 호출은 자동 복구되지 않습니다.

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

### Claude Code

```bash
node plugins/install.mjs claude-code --config "$DATALAKE_OTEL_CONFIG" --apply
claude plugin list --json
```

`doda-datalake-otel@skills-dir`가 활성 상태인지 확인합니다. Claude Code를 재시작하거나 `/reload-plugins`를 실행하세요. 사용자 skills 디렉터리에 설치하며 기존 `settings.json`은 보존합니다.

조직 정책, `disableAllHooks`, 명시적인 플러그인 비활성화는 우회하지 않습니다. Claude는 훅 프로세스에서 `OTEL_*` 환경 변수를 제거하므로, 이 가이드처럼 **private 설정 파일과 `--config` 경로**를 사용하세요.

현재 세션 transcript의 cursor는 해당 batch의 전송 ACK를 모두 확인한 뒤 확정합니다. 일부 전송이 실패하거나 예외가 발생하면 다음 훅에서 같은 구간을 다시 읽고, 이미 ACK된 message identity는 다시 보내지 않습니다. 병행 훅은 세션별로 직렬화하며 살아 있는 프로세스의 lock을 시간만으로 빼앗지 않습니다. `SessionStart`는 기존 transcript의 EOF에서 시작하므로 재개한 과거 대화는 수집하지 않습니다.

### AGY CLI (Antigravity)

```bash
node plugins/install.mjs agy --config "$DATALAKE_OTEL_CONFIG" --apply
```

`~/.gemini/config/plugins/doda-datalake/`에 플러그인과 훅이 등록되며, `~/.gemini/config/config.json`의 플러그인 목록에 활성화됩니다.
도구 실행(`PostToolUse`) 및 세션 종료(`Stop`) 시 자동으로 OTLP span이 데이터레이크로 전송됩니다.

도구 오류는 원문 대신 고정 분류 `error.type=tool_error`만 전송합니다. 증분 턴과 해당 서브에이전트가 모두 ACK되어야 cursor를 전진시키며, 부분 성공 identity를 보존해 다음 훅의 재시도에서 이미 성공한 사용량을 중복 전송하지 않습니다. 도구 완료 시각과 duration도 ACK 전까지 로컬 상태에 보존합니다.

## 6. 실제 사용량이 보이는지 확인하기

1. 연결 확인이 성공한 상태에서, 설치한 도구의 **새 세션**을 엽니다.
2. “OK라고만 답해 주세요”처럼 짧은 요청을 한 번 보냅니다. 실제 모델 호출에는 비용이 발생할 수 있습니다.
3. 응답을 기다리고 세션을 정상 종료합니다. 세션 종료 시점에 보내는 정보도 있습니다. 도구 호출을 사용하지 않았다면 도구 호출 통계가 없는 것이 정상입니다.
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
| `Export disabled: invalid private configuration` | 지정한 파일이 존재하는지, JSON 형식이 올바른지 확인. `chmod 600 "$DATALAKE_OTEL_CONFIG"`로 권한 제한. 파일 내용은 공유하지 말 것. |
| `Configuration already exists` | 재설정이 맞는지 확인 후 configure 명령에 `--replace` 추가. 기존 인증을 자동 승계하지 않으므로 인증 파일도 다시 지정. |
| 설치는 됐는데 새 span이 없음 | 실제로 사용하는 홈·프로필에 설치했는지, 재시작했는지 확인. Codex `/hooks` 신뢰, OMP 확장 비활성화 옵션, Claude 정책·비활성화 상태 확인. |
| opencode2 관리 포트 충돌 | 기존 사용자 서버를 강제 종료하지 말 것. `run --standalone`으로 분리하여 확인하거나 정상적인 서버 관리 절차로 해결. |
| 토큰은 있지만 비용이 비어 있음 | Codex·Claude 훅에는 비용 원본이 없어 누락 상태로 둠. 0원이라는 뜻이 아님. OMP·opencode2의 보고 비용도 청구서와 같다고 보장하지 않음. |
| 오프라인 동안 사용량 누락 | OMP는 영속 큐와 별도 sender로 재시도. 로컬 저장 실패·용량 초과 경고와 sender 시작 실패 확인. 갱신 전에 큐에 저장되지 않은 과거 이벤트는 복구되지 않음. |

서버 관리자가 추가 진단할 때는 서버의 Compose 디렉터리에서 `docker compose logs --since 10m alloy aggregate`를 확인할 수 있습니다. 로그를 공유하기 전 민감한 정보가 없는지 검토하세요.

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
- Codex의 재설치용 로컬 marketplace와 훅의 로컬 메타데이터 상태는 남을 수 있습니다. 플러그인 등록 제거와 데이터 삭제는 별개입니다.

## 검증한 범위와 한계

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

Claude·AGY의 ACK checkpoint 재시도는 원본 transcript·로컬 상태가 남아 있고 이후 훅이 실행될 때만 동작합니다. lock 경합으로 처리를 미루거나 실패한 전송을 독립적으로 재시도하는 백그라운드 작업·영속 offline queue는 없습니다. 프로세스가 종료되어 남긴 lock은 소유 PID가 더 이상 살아 있지 않을 때 회수합니다. 수집기가 저장한 뒤 ACK 응답만 유실되는 경우에는 같은 안정적 OTLP span identity가 다시 전송될 수 있으므로 end-to-end exactly-once 보장은 아닙니다.
