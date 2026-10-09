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
| [`.github/workflows/compose.yml`](../.github/workflows/compose.yml) | push/PR 시 비밀 정보·플러그인·회귀·생성물·프로필 검사 |

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

Node suite는 임시 private home/state와 합성 loopback HTTP를 사용합니다. `DATALAKE_TEST_BUN`이 없으면 실제 Bun 호출자 검사가, Codex CLI가 없으면 native 설치 보존 검사가 skip됩니다. skip은 통과가 아니며 결과에 이유를 남기세요. SIGKILL·FIFO·프로세스 소유권·0700/0600 검사는 POSIX/macOS/Linux 경계의 증거이지 Windows 지원 증거가 아닙니다.

이번 검증에서는 macOS Node suite를 실제 Bun 경로와 함께 실행해 skip 없이 통과했습니다. 별도 네트워크 없는 `node:22-bookworm` Linux 컨테이너에서는 plugin/test 소스를 read-only로 마운트해 Node suite가 통과했지만, 이미지에 실제 Codex CLI와 Bun이 없어 그 두 검사는 skip되었습니다. 이 Linux 결과는 다섯 native host의 Linux 실행 증거가 아닙니다.

- HTTP ACK를 의도적으로 보류한 동안 `enqueue()`와 `flushLocal()`, producer 종료·stdio 닫힘, OMP shutdown 및 OpenCode 소비·cleanup이 끝나는지 확인합니다. `enqueue() === true`는 로컬 영속 수락이지 HTTP ACK가 아닙니다.
- 실제 sender SIGKILL 전후 재전송, 파일/디렉터리 sync 실패, immutable 최초 body/ID/timestamp, pending+done 용량 상한과 tombstone 보존을 확인합니다.
- 파일 credential 교체를 다음 HTTP 시도에서 읽고, endpoint 변경 시 기존 pending을 다른 목적지로 보내지 않는지 확인합니다.
- Codex source snapshot/queued receipt, Claude·AGY의 legacy sent ACK 보존과 새 queued receipt, Claude 부분 batch checkpoint를 검사합니다. 로컬 수락 실패는 source checkpoint를 앞당기지 않아야 합니다.
- 동시 producer/worker의 단일 HTTP 소유권, 죽은 owner 복구, empty-worker handoff, 오래된 live owner 보존을 검사합니다. PID 재사용/이전 boot 사례는 incarnation fixture로 재현하며 실제 OS PID 재사용을 강제한 검사는 아닙니다.
- canary가 wire/spool에 없고 credential이 spool/진단에 없으며, project PATH의 가짜 Node가 실행되지 않는지 확인합니다. OpenCode는 raw bus를 계속 비우고 bounded sanitized metadata만 보관하며 overflow 시 새 ID를 명시적으로 거절해야 합니다.

이 suite의 callback/installer fixture와 실제 native host smoke는 별도 gate입니다. 별도 macOS smoke에서는 새 home, 실제 설치된 Codex·Claude Code·OMP·OpenCode·AGY와 합성 loopback LLM을 사용하고 실제 사용자 데이터 접근/쓰기 및 외부 socket을 sandbox로 차단했습니다. host(및 소유한 OpenCode server)가 ACK 전에 종료하고 남은 sender가 ACK 후 전달하는 것, 실제 OMP `task` 두 child와 `wait`의 child/subagent span, wire/spool canary 제외를 확인했습니다. 소유한 로컬 Alloy/Greptime에 native payload를 두 번 재생한 검사는 해당 fixture의 raw 중복/ID 기반 집계만 확인합니다. 운영 exactly-once, 실제 provider 청구, 모든 child 실행 경로나 AGY native 정확 token 계측을 증명하지 않습니다.

Node syntax 검사 통과와 LSP 검사는 구분하세요. 이번 검증에서는 변경 Node 파일의 syntax 검사는 통과했지만 LSP가 구성되지 않아 실행하지 않았습니다. Node와 Hermes는 모두 로컬 handoff와 network worker를 분리하지만, Node의 private 파일 outbox/분리된 sender와 Hermes의 SQLite/in-process daemon thread는 저장 형식·종료 수명·route 정책이 서로 다릅니다.

`.env.example`을 사용한 프로필 검사는 실제 자격 증명 없이 수행하는 구조 검사입니다. 실제 S3 연결, 물리 CAN, 운영 서버 배포·데이터 수집을 검증하지 않습니다. 변경한 기능의 런타임 검증은 별도로 수행하세요.


Hermes 플러그인을 변경했다면 별도 Python 테스트도 실행합니다. `HERMES_SOURCE`가 없으면 외부 native Hermes 검사는 skip되며, core/HTTP/multiprocess 및 Node `serializeEvent()` ID oracle 검사 통과와 구분해야 합니다. 이번 검증에서는 외부 verified checkout이 없어 native Hermes gate를 실행하지 않았습니다. 필요한 체크아웃과 제약은 [Hermes 문서](../plugins/hermes/README.md#tests)를 참고하세요.

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

성능 비교는 같은 row 집합·27개 column·event ID·정수 nanosecond를 검증한 뒤 수행하세요. SQL timestamp CAST와 정수 literal, native gRPC의 HTTP/protobuf body 및 encode/ACK 시간을 분리합니다. full ACK, ACK 유실 뒤 재전송 중복 제거, NULL/0/false/빈 문자열/Unicode, 잘못된 인증을 함께 확인하며 backend가 거부한 timestamp 경계는 성공 행 수에서 숨기지 않습니다. 소규모 로컬 fixture의 gRPC 우위나 index/cache query plan은 운영 처리량 증거가 아니며 의존성·운영 query 선택도를 확인하기 전 transport/index 기본값을 바꾸지 않습니다.

### 성능·보존 정책 검사

AI 집계의 native Flight 경로는 `grpcio==1.84.0`, `pyarrow==25.0.1`, `protobuf==6.33.6`을 사용합니다. `aggregate-deps`가 Python ABI와 고정 버전을 확인해 전용 venv를 준비하며, 수집기 venv와 공유하지 않습니다. 로컬 AI 회귀에는 같은 버전을 설치하고 `python -m tests.test_ai_incremental`을 실행하세요.

측정 조건·복구 경계·재현 명령은 [AI 증분 집계](ai-incremental.md), [Fleet/VSS 배치 저장](outbox-batching.md), [CAN pending 조회](can-pending.md), [RAW 메모리·임시 디스크](raw-memory.md)에 정리되어 있습니다. 합성 비교는 전체 재계산·원본 hash·빈 대상 복원을 먼저 확인하고, 운영 처리량이나 물리 장치 내구성으로 확대 해석하지 않습니다.

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

호스트와 Docker VM의 시계를 동기화해야 Grafana 로그인 세션과 상대 시각이 정상 동작합니다. 과거의 교정 관측은 `last_check.battery_panels.from_ms/to_ms`를 절대 조회 범위로 사용해 확인할 수 있습니다. 현재 시간창에 관측이 없으면 이전 전력·전압 숫자를 현재 값처럼 표시하지 않습니다.

CI의 `synthetic-runtime` job도 같은 `start/check/stop`을 실행하며 운영 secrets를 받지 않습니다. `check`는 `tests/test_privacy.py`의 실제 protobuf trace/log/metric·exemplar·인증 검사와 error-type 비밀 marker를 재사용합니다. 컨테이너 내부 감사 프록시를 Alloy exporter와 DB 사이에 두므로, VM host-gateway 설정이 필요 없으며 **DB가 필드를 버리기 전 wire protobuf**를 검사합니다. `opentelemetry-proto==1.39.1`은 감사 컨테이너에만 설치합니다.

대기열 검사는 DB를 중지하고 exporter queue occupancy를 확인한 뒤 Alloy를 SIGKILL합니다. DB와 Alloy를 다시 시작해 동일 identity의 사용량이 정확히 한 번 집계되는지 확인합니다. 복원 검사는 집계기를 먼저 멈춰 합성 원시행·세션·일별·차량 집계와 token metric 테이블의 모든 열을 고정하고, 전체 데모를 멈춘 뒤 오프라인 백업을 만듭니다. 별도 복원 프로젝트의 빈 볼륨에 복원해 DB를 실제로 시작하고 전체 행을 정확히 비교한 뒤 복원 프로젝트만 삭제합니다. 원래 데모는 다시 켜 두며 `stop`은 데모가 만든 두 프로젝트와 그 볼륨에만 한정됩니다.

이 검사는 File-store 소프트웨어 경로에 대한 증거입니다. 실제 S3 계정·호환성, 실제 차량/CAN·펌웨어, 외부 Fleet ZMQ, 개인 에이전트 계정, 운영 배포·대용량 처리, 브라우저의 시각적 렌더링은 CI가 주장하지 않습니다. Grafana query 응답과 dashboard provisioning은 실제 검사하지만 화면 관찰은 별도로 해야 합니다. 시작 중 오류가 나면 같은 state의 `stop`으로 소유 자원을 정리하세요.
