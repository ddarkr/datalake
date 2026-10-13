# 개발과 검증

[프로젝트 소개](../README.md) · [설치와 운영](operations.md)

아래 명령은 저장소 최상위 디렉터리에서 실행합니다.


| 경로 | 역할 |
| --- | --- |
| [`compose/`](../compose/) | 서비스·수집기·대시보드 원본 조각 |
| [`scripts/`](../scripts/) | 도메인별 Python 실행 모듈과 라이브러리 |
| [`plugins/`](../plugins/) | 코딩 에이전트 연결과 설치기 |
| [`tools/render.py`](../tools/render.py) | 원본을 단일 배포 파일로 생성 |
| [`tools/check_env.py`](../tools/check_env.py) | Compose 환경변수와 예제의 일치 검사 |
| [`tools/demo.py`](../tools/demo.py) | stdlib 데모 CLI, 격리 런타임·복원 검사와 필수 CAN/RAW 검사 |
| [`tests/`](../tests/) | 동작·보안·스토리지·렌더링 검증 |
| [`.github/workflows/compose.yml`](../.github/workflows/compose.yml) | push/PR 시 비밀 정보·플러그인·회귀·CAN 성능·생성물·프로필 검사 |

### Python 도메인

| 경로 | 책임 |
| --- | --- |
| `scripts/ingest/can/` | 서버 CAN OTLP 수신, SQLite 원본 보관, 해석·wire 계약 |
| `scripts/ingest/fleet_recorder.py` | Fleet ZMQ 수신과 영속 전송 대기열 |
| `scripts/vehicle/` | 차량 정의 준비와 VSS 수집 |
| `scripts/vehicle/raw/` | SocketCAN 원본 기록, MF4 검증·업로드·재해석 |
| `scripts/analytics/` | AI·차량·홈 집계 실행 |
| `scripts/analytics/battery/` | 배터리 분석과 DB 실행 어댑터 |
| `scripts/telemetry/` | AI 활동 필드·요약 계약과 수집 개인정보 처리 |
| `scripts/database/` | DB 설정 검증·준비와 스키마 초기화 |
| `scripts/storage/` | 백업·복구와 저장소 지표 |

`scripts`는 Python namespace package입니다. 저장소 루트에서 `python -m scripts.<도메인>.<모듈>`로 실행하며, 로컬 모듈은 같은 정규 경로로 import합니다. 컨테이너도 `/app/scripts/`에 같은 구조를 마운트하고 `/app`에서 실행합니다. 파일 직접 실행이나 이전 평탄 경로는 지원하지 않습니다. 예를 들어 CAN 상태 조회는 `python -m scripts.ingest.can.can_receiver status --database /private/path/raw.sqlite3`입니다.

테스트는 루트에서 `python -m tests.test_can_receiver`처럼 실행합니다. 배터리 계산 모듈은 DB·수집기를 import하지 않으며 `battery_runtime`이 조회·분석·저장을 연결합니다. SocketCAN/MF4 원본 경로와 서버 CAN/SQLite 원본 경로는 별도 계약입니다.

### CAN 성능 회귀 검사

`can-decode-perf`는 PR의 base 또는 push 직전 커밋과 현재 코드를 별도 프로세스에서 비교합니다. 합성 데이터 7종을 순서를 교대하여 3회 실행하고, 최종 행·순서·커서·부분 체크포인트·카운터가 다르거나 어느 fixture의 중앙 실행 시간이 25% 넘게 느려지면 실패합니다. 실패 때도 JSON 결과를 artifact로 남깁니다. 최초 push처럼 기준 커밋이 없는 경우는 성능 검증을 했다고 표시하지 않으며, 잘못되거나 가져올 수 없는 기존 커밋은 실패 처리합니다.

`Archive.decode_once()`의 반환값은 해당 호출에서 처리한 작업 수이며, 부분 처리된 청크는 다음 호출에서 다시 포함될 수 있습니다. 완료 여부는 반환값의 합이 아니라 최종 행·커서·부분 체크포인트·카운터로 검증합니다. 내부 prefetch hint의 값은 고정 계약이 아니며, 성능 회귀는 위의 실제 실행 시간과 결과 비교로 확인합니다.

```bash
python -m pip install --only-binary=:all: cantools==40.7.1 \
  opentelemetry-proto==1.38.0 protobuf==6.33.6
python tools/benchmark_can_decode.py \
  --baseline-root /path/to/pre-change-checkout \
  --chunks 2000 --repeats 3 --no-instrument --max-slowdown 0.25 \
  --json-out /tmp/datalake-can-decode-perf.json
# Docker가 있는 환경: 격리된 GreptimeDB 1.2.1에 실제 적재·ACK·재시작·재전송 검사
python -m tests.test_can_receiver_compose
python -m unittest tests.test_can_prefetch -v
```

CI에는 합성 DBC·OTLP만 사용합니다. 운영 원본·정의·차량 식별자·접속 정보는 공개 checkout과 artifact에 넣지 않습니다. 합성 처리율과 격리 재생 결과만으로 운영 수집·적재 처리율이나 적체 감소를 보장하지 않습니다.

Prefetch 회귀는 1 MiB를 넘는 합성 입력으로 byte-cap을 실제로 넘기며, partial 재시작 후 전체 행·상태·카운터를 단일 chunk 디코드 결과와 비교합니다. `decode_once`의 반환값은 완료 chunk 수가 아니라 partial을 포함한 처리 횟수입니다. CAS 충돌 때는 별도 writer의 커서 변경을 보존하면서 디코드 배치의 행·카운터가 커밋되지 않았는지 확인합니다.

### 배포 번들 생성

`compose.yaml`은 생성물입니다. 직접 편집하지 말고 `compose/*.yaml`과 `scripts/**/*.py`를 수정한 뒤 아래 **명시적 순서**로 재생성하세요. 기본 glob 순서는 CI의 생성 순서와 다릅니다.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install PyYAML==6.0.2

python tools/render.py \
  compose/core.yaml compose/database.yaml compose/ingest.yaml \
  compose/grafana.yaml compose/backup.yaml compose/vehicle-raw.yaml \
  compose/vehicle-vss.yaml compose/tesla-fleet.yaml compose/redecode.yaml \
  compose/can-receiver.yaml \
  --out compose.yaml

python tools/check_env.py
python -m tests.test_render
python -m tests.test_dashboards
python -m tests.test_grafana_folders

for profile in server home mqtt vehicle fleet backup redecode can-receiver '*'; do
  docker compose --env-file .env.example --profile server --profile "$profile" config --quiet
done
```

`grafana-folders`는 `scripts/database/grafana_folders.py`를 실행하는 초기화 서비스입니다. 폴더 계층은 Grafana 12.4.0의 `folder.grafana.app/v1beta1` API로 맞추고 파일 공급자는 각 폴더 UID를 고정합니다. Grafana 업그레이드 시 이 API 지원을 확인하세요. 오프라인 검사는 기존 폴더 재사용·페이지 처리·반복 실행과 CAN 최신 무효 값·수집원·해석 버전 분리를 확인합니다. 실제 화면 검증에는 별도 Grafana·Greptime과 합성 데이터를 사용하며 운영 데이터를 테스트용으로 복제하지 않습니다.

플러그인을 변경했다면 Node.js 22 이상에서 기존 Node 회귀 suite를 실행합니다.

```bash
npm ci --prefix plugins --ignore-scripts --no-audit --no-fund
node --test tests/test_plugin_*.mjs
# 선택: 실제 Bun 호출자 검사 (신뢰할 수 있는 절대 경로)
DATALAKE_TEST_BUN=/absolute/path/to/bun node --test tests/test_plugin_*.mjs
# 선택: PATH에 없는 실제 Codex CLI로 native 설치 보존 검사
CODEX_BINARY=/absolute/path/to/codex node --test tests/test_plugin_codex.mjs
```

Node suite는 임시 private home/state와 합성 loopback HTTP를 사용합니다. 실제 Codex CLI가 없으면 native 설치 보존 검사가 skip됩니다. skip은 통과가 아니며 이유를 남기세요. 프로세스 소유권·0700/0600 검사는 POSIX/macOS/Linux 경계의 증거이지 Windows 지원 증거가 아닙니다.

모든 JavaScript 어댑터는 직접 HTTP 전송을 사용합니다. `send() === true`는 전체 collector ACK이며, `flush()`는 진행 중 요청만 기다립니다. 영속 outbox·분리 sender·backoff·완료 tombstone·전송 용량 상한은 없습니다. Hermes도 Python 훅에서 직접 전송합니다.

- 합성 loopback 수신으로 실제 OTLP wire identity·숫자 사용량·계층·개인정보 필터를 확인합니다.
- HTTP 거부·partial success·잘못된 응답·보류한 ACK의 timeout을 확인합니다. 실패 이벤트가 다음 동일 훅에서 자동 재전송되지 않아야 합니다.
- OMP·OpenCode의 이벤트 처리는 HTTP를 기다리지 않고, 정상 cleanup은 진행 중 요청을 기다립니다. 단기 Codex·Claude·AGY 훅은 전송 완료나 timeout 뒤 종료합니다.
- 관측 checkpoint·숫자 사용량 baseline과 중복 시도 방지를 확인합니다. checkpoint는 collector 저장 증거가 아닙니다. 기존 큐는 재생·변경하지 않고 새 전송 큐를 만들지 않아야 합니다.
- 메타데이터 checkpoint 잠금의 live owner 보존·죽은 owner 회수·symlink/hardlink 거부를 확인합니다. PID 재사용 fixture는 실제 OS PID 재사용을 강제한 검사가 아닙니다.

callback/installer fixture와 실제 native host smoke는 별도 gate입니다. 이전 macOS native host smoke에서 ACK 전에 부모가 종료하고 별도 sender가 나중에 전달한 결과는 구 영속 전송 구현의 기록이며 현재 직접 전송의 증거가 아닙니다. 현재 직접 전송의 회귀·loopback 검증도 사용자 설치·재시작, 운영 DB 저장·집계, provider 청구, 모든 native host 경로를 증명하지 않습니다.

Node syntax와 LSP 검사는 구분하세요. LSP가 구성되지 않은 환경에서는 syntax·실행 검사 결과만 보고합니다. 실패·오프라인·강제 종료 시 직접 전송 이벤트는 유실될 수 있고 자동 재시도하지 않습니다. 설치본을 복사하는 Codex·Claude는 재설치하고, 실행 중인 장기 호스트는 정상 재시작·재로딩해야 변경이 적용됩니다. 기존 sender·큐는 소스 갱신으로 종료·이관·삭제하지 않습니다.

직접 전송 전환의 최종 macOS 검증은 실제 Codex CLI를 지정한 `node --test --test-concurrency=1 tests/test_plugin_*.mjs`에서 **84개 통과, 실패·skip 0개**였습니다. 별도 실행한 실제 adapter loopback smoke는 Codex·Claude·AGY 수신, OpenCode 비차단 전송·318ms timeout 종료, Hermes 중복 억제·실패 단일 시도를 확인했습니다. 운영 수집기로 보내거나 사용자 설치본을 갱신하지 않았습니다.

`.env.example`을 사용한 프로필 검사는 실제 자격 증명 없이 수행하는 구조 검사입니다. 실제 S3 연결, 물리 CAN, 운영 서버 배포·데이터 수집을 검증하지 않습니다. 변경한 기능의 런타임 검증은 별도로 수행하세요.


Hermes 플러그인을 변경했다면 별도 Python 테스트도 실행합니다. 최종 직접 전송 suite는 22개 중 **19개 통과, native Hermes 3개 skip**입니다. `HERMES_SOURCE`의 검증된 외부 checkout이 없어 native gate는 실행하지 않았습니다. core·실제 HTTP·Node `serializeEvent()` ID oracle 검사와 native gate를 구분하세요. 필요한 체크아웃과 제약은 [Hermes 문서](../plugins/hermes/README.md#tests)를 참고하세요.

```bash
python3 -B -m unittest discover -s plugins/hermes/tests -v
```

## CI와 개인정보 검사

CI는 `push`와 `pull_request`에서 실행하며 권한은 `contents: read`만 사용합니다. 외부 PR 검사에 운영 비밀을 제공하지 않습니다. 고정 버전·SHA256 검증을 거친 Gitleaks가 현재 파일과 전체 Git 이력을 각각 검사합니다.

Gitleaks `8.30.1`이 설치된 개발 환경에서는 다음 명령으로 동일 범위를 검사할 수 있습니다. 개인정보나 임의 형식의 키를 모두 탐지하는 도구는 아니므로 실제 운영 식별자에 대한 별도 검토도 필요합니다.

```bash
gitleaks dir . --redact=100 --ignore-gitleaks-allow
gitleaks git . --redact=100 --ignore-gitleaks-allow --log-opts="--all --full-history"
python -m tests.test_trace_privacy
```

개인 `.env`나 운영 설정이 있는 로컬 디렉터리에서는 `gitleaks dir`가 해당 파일을 읽을 수 있습니다. 공유 가능한 로그만 남기고, 실제 운영 파일을 공개 CI에 업로드하지 마세요. CI의 작업 디렉터리에는 공개 checkout만 있습니다.

서버·운영 자격 증명이 필요 없는 Python 회귀 목록은 workflow의 `Check offline Python regressions` 단계에 명시합니다. CI는 Compose vehicle/redecode와 같은 `python-can==4.6.1`, `asammdf==8.8.27`, `zstd==1.5.6.1`, `cantools==40.7.1`, `py-expression-eval==0.3.14`, `boto3==1.43.98`을 설치합니다. `python tools/demo.py regressions`는 RAW/MF4·재해석·CAN validation 검사 중 하나라도 skip되면 실패합니다. 재해석용 `eclipse-kuksa/kuksa-can-provider`는 태그가 아닌 commit `d03dd7db364dd1ce9f7d0c614d80ebf6642ad167`의 세 `dbcfeederlib` 파일을 내려받아 기존 테스트의 SHA256으로 검증합니다. 이 다운로드 또는 PyPI에 접근할 수 없으면 CI는 실패하며 성공으로 대체하지 않습니다. 선택적 외부 Hermes checkout 호환성은 별도 범위입니다.

CAN receiver의 합성 회귀는 `python -m pip install cantools==40.7.1 opentelemetry-proto==1.38.0` 후 `python -m tests.test_can_receiver`, `python -m tests.test_can_decoder`, `python -m tests.test_can_otlp_wire`로 실행합니다. 조밀한 frame의 bounded decode·부분 cursor 재개·중복 event ID, 10,000레코드/2 MiB wire 경계와 엄격한 metadata 검증을 포함합니다. `python -m tests.test_can_backup`과 `python -m tests.test_storage_metrics`는 일관 snapshot·fresh target·공간 부족·private mode 및 cached counter/unknown 지표를 확인합니다. 운영 DBC·원본·자격 증명은 사용하지 않습니다. 기존 wire privacy 런타임의 `opentelemetry-proto==1.39.1`은 별도 감사 컨테이너에 유지하며 receiver의 venv와 섞지 않습니다.

`python -m tests.test_can_http_deadline`은 실제 loopback HTTP에서 요청별 절대
deadline, 같은 keep-alive socket의 timeout 초과 재사용, 느린 client 종료와
ACK 유실 후 원본 중복 제거를 검사합니다. deadline은 연결 전체 수명이 아니라
각 요청에 적용되며, idle 연결도 동일 timeout과 concurrency 제한을 받습니다.

성능 비교는 같은 row 집합·27개 column·event ID·정수 nanosecond를 검증한 뒤 수행하세요. SQL timestamp CAST와 정수 literal, native gRPC의 HTTP/protobuf body 및 encode/ACK 시간을 분리합니다. full ACK, ACK 유실 뒤 재전송 중복 제거, NULL/0/false/빈 문자열/Unicode, 잘못된 인증을 함께 확인하며 backend가 거부한 timestamp 경계는 성공 행 수에서 숨기지 않습니다. 소규모 로컬 fixture의 gRPC 우위나 index/cache query plan은 운영 처리량 증거가 아니며 의존성·운영 query 선택도를 확인하기 전 transport/index 기본값을 바꾸지 않습니다.

`python -m tests.test_can_encoding`은 실제 flush의 SQL/form bytes, UTF-8·특수문자,
정수 timestamp, byte budget 경계와 full signal/dirty ACK 이전 outbox 보존을
검사합니다. SQL cell은 한 번 인코딩하고 크기 판단과 최종 본문에 재사용합니다.
`python tools/benchmark_can_encoding.py --baseline-root <변경 전 checkout>
--rows 1,2000,20000 --repeats 3`은 두 실제 flush 경로를 교차 실행해 본문
SHA-256, CPU/wall 시간과 전체 child peak RSS를 비교합니다.

`python -m unittest tests.test_can_prefetch`는 세션 간 도착 순서, byte/page 경계,
부분 decode 재개와 cursor 충돌 시 rollback을 검사합니다.
`python -m unittest discover -s tests -p test_can_processes.py`는 분리된 `serve`·`work` 실프로세스로 독립 기동·순서 무관 처리·worker 중복/`re-decode` 상호배제·역할 lock·종료 내구성을 검사합니다.
`python -m unittest discover -s tests -p test_can_files.py`는 파일 범위의 내구성 ACK·중단 재개·인증/경로 경계와 worker 편입·기존 OTLP 중복 제거를 검사합니다. Pi 저장소의 `can_collection.test_go_runtime`은 실제 Go 파일 uploader를 이 receiver/importer에 연결합니다.
`python tools/benchmark_can_decode.py --baseline-root <변경 전 checkout>
--cases xsession_contig,xsession_roundrobin,xsession_uneven --chunks 12000
--repeats 3`으로 1/4/100/400개 세션의 실제 읽기 증폭과 처리 시간을 비교합니다.

### 성능·보존 정책 검사

AI 집계의 native Flight 경로는 `grpcio==1.84.0`, `pyarrow==25.0.1`, `protobuf==6.33.6`을 사용합니다. `aggregate-deps`가 Python ABI와 고정 버전을 확인해 전용 venv를 준비하며, 수집기 venv와 공유하지 않습니다. 로컬 AI 회귀에는 같은 버전을 설치하고 `python -m tests.test_ai_incremental`을 실행하세요.

측정 조건·복구 경계·재현 명령은 [AI 증분 집계](ai-incremental.md), [Fleet/VSS 배치 저장](outbox-batching.md), [CAN pending 조회](can-pending.md), [RAW 메모리·임시 디스크](raw-memory.md), [백업 세대 보존](backup-retention.md), [OTel 보존·용량 정책](otel-retention.md)에 정리되어 있습니다. 합성 비교는 전체 재계산·원본 hash·빈 대상 복원을 먼저 확인하고, 운영 처리량이나 물리 장치 내구성으로 확대 해석하지 않습니다.

## 합성 런타임 통합 검사

로컬 Docker Engine/Desktop과 Compose 2.24.4 이상에서 아래 명령을 사용합니다. 호스트 CLI에는 Python 3.12 stdlib만 필요합니다. generated `compose.yaml`을 사용하므로 소스를 변경했다면 위 순서대로 재생성한 뒤 실행하세요.

```bash
python3 tools/demo.py start
python3 tools/demo.py check
python3 tools/demo.py status
# UI 관찰이 끝난 뒤:
python3 tools/demo.py stop
```

네 명령은 동일한 `--state /임시/경로`를 선택적으로 받습니다. 기본은 사용자별 임시 디렉터리이며 기존 state를 덮어쓰지 않습니다. JSON 출력은 `project`, `directory`, `grafana_url`, `credentials_file`, `ui_path`와 검사 후 `last_check`를 제공합니다. `last_check`에는 세션별 정확한 NULL/0/토큰 값, 실제 Grafana datasource query 결과, 교정 전력·팩 전압 쿼리 8개의 결과와 절대 조회 범위(`battery_panels`), fresh restore와 일치한 테이블별 행 수, wire/DB 개인정보 및 SIGKILL 후 대기열 전달 검증 결과가 있습니다. 전력·전압 카드는 조회 끝을 다음 시간창으로 옮겼을 때 이전 숫자 대신 NULL이 되는지도 검사합니다. state/env/기대값은 비공개 `0600`, 디렉터리는 `0700`입니다.

`start`는 원본 `.env`를 읽지 않고 환경에서 운영 Compose 설정을 제거합니다. 별도 프로젝트·File 저장소·합성 인증을 사용하며, 모든 게시 포트는 `127.0.0.1`의 Docker 자동 할당 포트입니다. 저장소 전체를 컨테이너에 마운트하지 않고 필요한 세 공개 코드 파일만 읽기 전용으로 마운트합니다. `start` 출력의 URL에 로그인(사용자 `demo`, `credentials_file`의 합성 `GF_ADMIN_PASSWORD`)해 `/d/datalake-ai-usage`의 세션 원장을 보세요. `demo-known` 7/3, `demo-zero` 0/0, `demo-unknown` 미보고 값을 구분합니다. `check` 후 `demo-queued` 19/5가 추가됩니다. 비용이 미보고·미산정이면 NULL이며 0으로 채우지 않습니다.

실제 합성 Fleet recorder 경로는 2-frame 검증 → `process_frame` → SQLite outbox → `upload_tick` → 실제 Greptime acknowledgement입니다. 가상 차량만 사용하며 실제 ZMQ 네트워크 발행자나 물리 CAN을 검증하지 않습니다. 시간창 마지막 10분 이내에 있는 이전 완료 시간의 V/I 쌍에 데모 범위 교정만 적용합니다. 전력·전압 카드는 완료된 시간창의 마지막 관측이며, raw 단위가 불명확한 다른 물리 값은 unknown 상태로 남습니다.

교정 검사와 패널의 절대 조회 범위는 합성 원시 관측이 있는 시간창에서 정합니다. 같은 Fleet epoch·지표의 최신 개정을 먼저 선택하므로, 다른 빈 시간창의 NULL 사유 행이 측정값을 덮지 않으며 해당 관측 시간창의 최신 실패는 과거 정상값으로 대체하지 않습니다.

호스트와 Docker VM의 시계를 동기화해야 Grafana 로그인 세션과 상대 시각이 정상 동작합니다. 과거의 교정 관측은 `last_check.battery_panels.from_ms/to_ms`를 절대 조회 범위로 사용해 확인할 수 있습니다. 현재 시간창에 관측이 없으면 이전 전력·전압 숫자를 현재 값처럼 표시하지 않습니다.

CI의 `synthetic-runtime` job도 같은 `start/check/stop`을 실행하며 운영 secrets를 받지 않습니다. `check`는 `tests/test_privacy.py`의 실제 protobuf trace/log/metric·exemplar·인증 검사와 error-type 비밀 marker를 재사용합니다. 컨테이너 내부 감사 프록시를 Alloy exporter와 DB 사이에 두므로, VM host-gateway 설정이 필요 없으며 **DB가 필드를 버리기 전 wire protobuf**를 검사합니다. `opentelemetry-proto==1.39.1`은 감사 컨테이너에만 설치합니다.

이 job은 고정 버전 Python·GreptimeDB·Grafana·Alloy 이미지를 [Google의 공개 Docker Hub 캐시](https://cloud.google.com/artifact-registry/docs/pull-cached-dockerhub-images) `mirror.gcr.io`에서 직접 받습니다. `validate`의 packaged CAN 검사도 Python·GreptimeDB에 같은 캐시를 사용합니다. `start`는 기존 `PYTHON_IMAGE`, `BACKUP_IMAGE`, `GREPTIME_IMAGE`, `GRAFANA_IMAGE`, `ALLOY_IMAGE` override만 데모 설정에 전달하며 운영 기본 이미지는 바꾸지 않습니다. 캐시에 이미지가 없으면 Hub로 우회하지 않고 pull 오류로 실패합니다.

대기열 검사는 DB를 중지하고 exporter queue occupancy를 확인한 뒤 Alloy를 SIGKILL합니다. DB와 Alloy를 다시 시작해 동일 identity의 사용량이 정확히 한 번 집계되는지 확인합니다. 복원 검사는 집계기를 먼저 멈춰 합성 원시행·세션·일별·차량 집계와 token metric 테이블의 모든 열을 고정하고, 전체 데모를 멈춘 뒤 오프라인 백업을 만듭니다. 별도 복원 프로젝트의 빈 볼륨에 복원해 DB를 실제로 시작하고 전체 행을 정확히 비교한 뒤 복원 프로젝트만 삭제합니다. 원래 데모는 다시 켜 두며 `stop`은 데모가 만든 두 프로젝트와 그 볼륨에만 한정됩니다.

이 검사는 File-store 소프트웨어 경로에 대한 증거입니다. 실제 S3 계정·호환성, 실제 차량/CAN·펌웨어, 외부 Fleet ZMQ, 개인 에이전트 계정, 운영 배포·대용량 처리, 브라우저의 시각적 렌더링은 CI가 주장하지 않습니다. Grafana query 응답과 dashboard provisioning은 실제 검사하지만 화면 관찰은 별도로 해야 합니다. 시작 중 오류가 나면 같은 state의 `stop`으로 소유 자원을 정리하세요.
