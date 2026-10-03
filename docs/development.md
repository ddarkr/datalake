# 개발과 검증

[프로젝트 소개](../README.md) · [설치와 운영](operations.md)

아래 명령은 저장소 최상위 디렉터리에서 실행합니다.


| 경로 | 역할 |
| --- | --- |
| [`compose/`](../compose/) | 서비스·수집기·대시보드 원본 조각 |
| [`scripts/`](../scripts/) | 초기화, 집계, 차량 기록, 백업·복원 구현 |
| [`plugins/`](../plugins/) | 코딩 에이전트 연결과 설치기 |
| [`tools/render.py`](../tools/render.py) | 원본을 단일 배포 파일로 생성 |
| [`tools/check_env.py`](../tools/check_env.py) | Compose 환경변수와 예제의 일치 검사 |
| [`tools/demo.py`](../tools/demo.py) | stdlib 데모 CLI, 격리 런타임·복원 검사와 필수 CAN/RAW 검사 |
| [`tests/`](../tests/) | 동작·보안·스토리지·렌더링 검증 |
| [`.github/workflows/compose.yml`](../.github/workflows/compose.yml) | push/PR 시 비밀 정보·플러그인·회귀·생성물·프로필 검사 |

`compose.yaml`은 생성물입니다. 직접 편집하지 말고 `compose/*.yaml`과 `scripts/*`를 수정한 뒤 아래 **명시적 순서**로 재생성하세요. 기본 glob 순서는 CI의 생성 순서와 다릅니다.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install PyYAML==6.0.2

python tools/render.py \
  compose/core.yaml compose/database.yaml compose/ingest.yaml \
  compose/grafana.yaml compose/backup.yaml compose/vehicle-raw.yaml \
  compose/vehicle-vss.yaml compose/tesla-fleet.yaml compose/redecode.yaml \
  --out compose.yaml

python tools/check_env.py
python tests/test_render.py

for profile in server home mqtt vehicle fleet backup redecode '*'; do
  docker compose --env-file .env.example --profile server --profile "$profile" config --quiet
done
```

플러그인을 변경했다면 Node.js 22 이상에서 다음도 실행합니다.

```bash
npm ci --prefix plugins --ignore-scripts --no-audit --no-fund
node --test tests/test_plugin_*.mjs
```

`.env.example`을 사용한 프로필 검사는 실제 자격 증명 없이 수행하는 구조 검사입니다. 실제 S3 연결, 물리 CAN, 운영 서버 배포·데이터 수집을 검증하지 않습니다. 변경한 기능의 런타임 검증은 별도로 수행하세요.


Hermes 플러그인을 변경했다면 별도 Python 테스트도 실행합니다. native 호환성 검증에 필요한 외부 체크아웃과 제약은 [Hermes 문서](../plugins/hermes/README.md#tests)를 참고하세요.

```bash
python3 -m unittest discover -s plugins/hermes/tests -v
```

## CI와 개인정보 검사

CI는 `push`와 `pull_request`에서 실행하며 권한은 `contents: read`만 사용합니다. 외부 PR 검사에 운영 비밀을 제공하지 않습니다. 고정 버전·SHA256 검증을 거친 Gitleaks가 현재 파일과 전체 Git 이력을 각각 검사합니다.

Gitleaks `8.30.1`이 설치된 개발 환경에서는 다음 명령으로 동일 범위를 검사할 수 있습니다. 개인정보나 임의 형식의 키를 모두 탐지하는 도구는 아니므로 실제 운영 식별자에 대한 별도 검토도 필요합니다.

```bash
gitleaks dir . --redact=100 --ignore-gitleaks-allow
gitleaks git . --redact=100 --ignore-gitleaks-allow --log-opts="--all --full-history"
python tests/test_trace_privacy.py
```

개인 `.env`나 운영 설정이 있는 로컬 디렉터리에서는 `gitleaks dir`가 해당 파일을 읽을 수 있습니다. 공유 가능한 로그만 남기고, 실제 운영 파일을 공개 CI에 업로드하지 마세요. CI의 작업 디렉터리에는 공개 checkout만 있습니다.

서버·운영 자격 증명이 필요 없는 Python 회귀 목록은 workflow의 `Check offline Python regressions` 단계에 명시합니다. CI는 Compose vehicle/redecode와 같은 `python-can==4.6.1`, `asammdf==8.8.27`, `zstd==1.5.6.1`, `cantools==40.7.1`, `py-expression-eval==0.3.14`, `boto3==1.43.98`을 설치합니다. `python tools/demo.py regressions`는 RAW/MF4·재해석·CAN validation 검사 중 하나라도 skip되면 실패합니다. 재해석용 `eclipse-kuksa/kuksa-can-provider`는 태그가 아닌 commit `d03dd7db364dd1ce9f7d0c614d80ebf6642ad167`의 세 `dbcfeederlib` 파일을 내려받아 기존 테스트의 SHA256으로 검증합니다. 이 다운로드 또는 PyPI에 접근할 수 없으면 CI는 실패하며 성공으로 대체하지 않습니다. 선택적 외부 Hermes checkout 호환성은 별도 범위입니다.

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

`start`는 원본 `.env`를 읽지 않고 환경에서 운영 Compose 설정을 제거합니다. 별도 프로젝트·File 저장소·합성 인증을 사용하며, 모든 게시 포트는 `127.0.0.1`의 Docker 자동 할당 포트입니다. 저장소 전체를 컨테이너에 마운트하지 않고 필요한 세 공개 코드 파일만 읽기 전용으로 마운트합니다. `start` 출력의 URL에 로그인(사용자 `demo`, `credentials_file`의 합성 `GF_ADMIN_PASSWORD`)해 `/d/datalake-ai-usage`의 Session ledger를 보세요. `demo-known` 7/3, `demo-zero` 0/0, `demo-unknown` 미보고 값을 구분합니다. `check` 후 `demo-queued` 19/5가 추가됩니다. 비용이 미보고·미산정이면 NULL이며 0으로 채우지 않습니다.

실제 합성 Fleet recorder 경로는 2-frame 검증 → `process_frame` → SQLite outbox → `upload_tick` → 실제 Greptime acknowledgement입니다. 가상 차량만 사용하며 실제 ZMQ 네트워크 발행자나 물리 CAN을 검증하지 않습니다. 시간창 마지막 10분 이내에 있는 이전 완료 시간의 V/I 쌍에 데모 범위 교정만 적용합니다. 전력·전압 카드는 완료된 시간창의 마지막 관측이며, raw 단위가 불명확한 다른 물리 값은 unknown 상태로 남습니다.

호스트와 Docker VM의 시계를 동기화해야 Grafana 로그인 세션과 상대 시각이 정상 동작합니다. 과거의 교정 관측은 `last_check.battery_panels.from_ms/to_ms`를 절대 조회 범위로 사용해 확인할 수 있습니다. 현재 시간창에 관측이 없으면 이전 전력·전압 숫자를 현재 값처럼 표시하지 않습니다.

CI의 `synthetic-runtime` job도 같은 `start/check/stop`을 실행하며 운영 secrets를 받지 않습니다. `check`는 `tests/test_privacy.py`의 실제 protobuf trace/log/metric·exemplar·인증 검사와 error-type 비밀 marker를 재사용합니다. 컨테이너 내부 감사 프록시를 Alloy exporter와 DB 사이에 두므로, VM host-gateway 설정이 필요 없으며 **DB가 필드를 버리기 전 wire protobuf**를 검사합니다. `opentelemetry-proto==1.39.1`은 감사 컨테이너에만 설치합니다.

대기열 검사는 DB를 중지하고 exporter queue occupancy를 확인한 뒤 Alloy를 SIGKILL합니다. DB와 Alloy를 다시 시작해 동일 identity의 사용량이 정확히 한 번 집계되는지 확인합니다. 복원 검사는 집계기를 먼저 멈춰 합성 원시행·세션·일별·차량 집계와 token metric 테이블의 모든 열을 고정하고, 전체 데모를 멈춘 뒤 오프라인 백업을 만듭니다. 별도 복원 프로젝트의 빈 볼륨에 복원해 DB를 실제로 시작하고 전체 행을 정확히 비교한 뒤 복원 프로젝트만 삭제합니다. 원래 데모는 다시 켜 두며 `stop`은 데모가 만든 두 프로젝트와 그 볼륨에만 한정됩니다.

이 검사는 File-store 소프트웨어 경로에 대한 증거입니다. 실제 S3 계정·호환성, 실제 차량/CAN·펌웨어, 외부 Fleet ZMQ, 개인 에이전트 계정, 운영 배포·대용량 처리, 브라우저의 시각적 렌더링은 CI가 주장하지 않습니다. Grafana query 응답과 dashboard provisioning은 실제 검사하지만 화면 관찰은 별도로 해야 합니다. 시작 중 오류가 나면 같은 state의 `stop`으로 소유 자원을 정리하세요.
