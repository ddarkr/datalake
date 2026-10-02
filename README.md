# doda_datalake

코딩 에이전트의 사용량, 차량 CAN/VSS, 홈 센서 데이터를 수집하고 GreptimeDB에 저장하는 개인 데이터레이크입니다. Grafana에서 사용량·세션·차량 상태·수집기 상태를 확인합니다.

**서버 배포에 필요한 파일은 `compose.yaml`과 `.env` 두 개입니다.** Python 스크립트, 수집기 설정, Grafana 대시보드는 생성된 Compose 파일에 포함되어 있습니다. 차량 수집기와 코딩 에이전트 플러그인은 각 데이터를 만드는 장비에서 별도로 실행합니다.

## 구성

```text
코딩 에이전트 / OTel 생산자 ── OTLP ── Alloy ── 개인정보 필터 ─┐
Home Assistant ── Prometheus scrape ── Alloy Home ───────────┤
기존 MQTT 브로커 ── Telegraf ────────────────────────────────┤
                                                           ▼
차량 CAN ── KUKSA ── VSS recorder / SQLite outbox ────── GreptimeDB
   │                                                       │
   └── raw recorder ── MF4 + sidecar + manifest ── S3        ├── 정기 집계
                                                           └── Grafana
```

- **GreptimeDB:** standalone 구성. 기본 저장소는 S3 호환 스토리지이며, 로컬 named volume도 필요합니다. S3 버킷만으로 전체 DB를 복구할 수 있다고 가정하면 안 됩니다.
- **Alloy:** 인증된 OTLP 수신, 데이터 필터링, 영속 전송 대기열, 인프라 메트릭 수집을 담당합니다. trace 경로에는 별도의 `trace-privacy` 서비스가 있습니다.
- **집계:** AI 세션·모델·도구 사용량, 차량 주행·충전, 설정된 홈 센서 데이터를 집계합니다. 기본 실행 간격은 300초입니다.
- **차량:** Linux SocketCAN에서 원본 CAN을 기록하고, 고정된 DBC/VSS 정의로 해석한 신호를 별도 저장합니다. CAN 송신은 사용하지 않습니다.
- **백업:** DB를 정지한 상태에서 로컬 데이터와 S3 SST 객체를 검증하여 백업·복원합니다. 실시간 백업은 제공하지 않습니다.

현재 기본 이미지: GreptimeDB `v1.2.1`, Grafana `12.4.0`, Alloy `v1.19.2`, Telegraf `1.40.0`. 전체 설정과 이미지 재정의는 [`.env.example`](.env.example)을 참고하세요.

## 서버 시작하기

### 1. 준비

- Docker Engine과 `configs.content`를 지원하는 최신 Docker Compose 플러그인.
- 이미지 다운로드와 초기 Python 의존성 설치가 가능한 네트워크.
- S3 호환 버킷과 접근 자격 증명. 로컬 시험만 할 때는 `File` 저장소를 사용할 수 있습니다.
- Git으로 받은 저장소 또는 생성된 `compose.yaml`과 `.env.example`.

다음 명령은 저장소 최상위 디렉터리에서 실행합니다. **이미 `.env`가 있으면 복사하지 말고 기존 파일을 편집하세요.**

```bash
cp .env.example .env
chmod 600 .env
```

`.env`를 편집해 다음 항목을 설정하세요. 비밀번호를 셸 명령 인자나 Git에 남기지 마세요.

| 설정 | 의미 |
| --- | --- |
| `COMPOSE_PROFILES=server` | 기본 서버 서비스 활성화 |
| `GREPTIME_DB=datalake`, `GREPTIME_USER=datalake` | DB 이름·계정. 예제의 명시적 값을 유지하세요. |
| `GREPTIME_PASSWORD` | DB 비밀번호, 필수 |
| `GF_ADMIN_PASSWORD` | Grafana 초기 관리자 비밀번호, 필수 |
| `OTLP_USER`, `OTLP_PASSWORD` | OTLP 수집 인증, 둘 다 필수 |
| `GREPTIME_STORAGE_TYPE=S3` | 기본 저장소. 로컬 시험은 `File` |
| `S3_ENDPOINT_URL` | 버킷 이름을 붙이지 않은 HTTPS API endpoint |
| `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | DB 저장 버킷과 접근 자격 증명 |
| `S3_REGION` | 리전. 일부 Backblaze endpoint만 자동 추론 |
| `S3_ROOT=greptime` | DB 객체 prefix |

`File` 모드에서는 S3 설정을 사용하지 않지만 DB·Grafana·OTLP 인증은 여전히 필수입니다. `GREPTIME_PASSWORD`에는 `=`과 개행을 넣을 수 없습니다. dotenv 값에 `$`가 있으면 작은따옴표로 감싸 Compose 변수 치환을 방지하세요.

**사용 중인 볼륨에서 저장소 종류·endpoint·버킷·prefix를 바꾸지 마세요.** 저장소 식별자가 달라지면 preflight가 기동을 거부합니다. 변경은 백업 후 새 볼륨으로 복원하는 방식으로 진행합니다.

### 2. 설정 검사와 기동

```bash
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d
docker compose --env-file .env ps -a
docker compose --env-file .env logs --tail 100 db-init db-schema alloy aggregate
```

`config --quiet`는 Compose 구조 검사입니다. 비밀번호·스토리지 설정의 실제 유효성이나 DB 연결 성공까지 검증하지는 않습니다. `--quiet` 없는 `config` 출력에는 비밀값이 포함될 수 있으므로 공유하지 마세요.

`db-init`, `db-schema`, `backup-deps`는 초기화 작업이므로 **`Exited (0)`이 정상**입니다. 상시 서비스가 실행 중인지와 초기화 로그를 함께 확인하세요. Grafana가 열린다는 사실만으로 실제 데이터 수집이 증명되지는 않습니다.

### 3. 접속

| 용도 | 기본 호스트 주소 | 인증 |
| --- | --- | --- |
| Grafana | `http://127.0.0.1:3000` | `GF_ADMIN_USER` 기본값 `admin` / 설정한 비밀번호 |
| Greptime HTTP / SQL | `http://127.0.0.1:4000` | DB 계정 |
| Greptime MySQL 프로토콜 | `127.0.0.1:4002` | DB 계정 |
| OTLP HTTP | `http://127.0.0.1:4318` | `OTLP_USER` / `OTLP_PASSWORD` Basic 인증 |
| OTLP gRPC | `127.0.0.1:4317` | OTLP 인증 |

호스트 포트는 `.env`의 `*_PORT`로 바꿀 수 있습니다. 기본 바인딩은 모두 loopback이며 **다른 컴퓨터에서 직접 접근할 수 없습니다.** 원격 접속에는 SSH 터널, 암호화된 사설망 또는 HTTPS 인증 경계를 사용하세요. `0.0.0.0`으로 바꾸어 인터넷에 노출하지 마세요. Basic 인증만으로 HTTP 전송이 암호화되지는 않습니다.

Arcane 등 Compose 관리 도구에서도 같은 `compose.yaml`과 환경변수를 사용합니다. 자동 동기화·재배포는 서비스 전체에 영향을 줄 수 있으므로, 단일 서비스 수정이라고 의존 서비스가 재실행되지 않는다고 가정하지 마세요.

## 수집원 연결

### 코딩 에이전트

[플러그인 설치·연결 가이드](plugins/README.md)에 Codex, OMP, opencode2, Claude Code와 AGY CLI 연결 절차가 있습니다. Node.js 22 이상이 필요하며, 설치·설정 명령은 기본적으로 미리보기이고 `--apply`로 적용합니다.

- 플러그인은 세션·토큰·도구 실행 **메타데이터**를 전송합니다. 프롬프트, 답변·추론 원문, 도구 인자·결과, 파일 내용은 전송 대상에서 제외합니다.
- 서버 `.env` 전체를 에이전트 장비에 복사하지 마세요. 수집 주소와 OTLP 인증만 전달합니다.
- OMP와 opencode2는 설치 후에도 저장소 경로를 참조합니다. 저장소를 이동·삭제하지 마세요.
- 플러그인 설치 후 클라이언트를 재시작하고 실제 새 세션을 확인해야 합니다. Codex는 훅 신뢰 승인도 필요합니다.
- 직접 전송 플러그인에는 영속 오프라인 재전송 큐가 없습니다. 서버 Alloy 큐는 서버에 도착하지 못한 데이터를 복구하지 못합니다.

### Home Assistant / MQTT

서버 `.env`의 프로필에 필요한 항목을 추가합니다.

```dotenv
COMPOSE_PROFILES=server,home,mqtt
```

- Home Assistant: Prometheus 연동을 준비하고 `HA_HOST`, `HA_SCHEME`, `HA_TOKEN`을 설정합니다. `HA_HOST`는 **Alloy 컨테이너에서 접근할 주소**여야 합니다. 컨테이너의 `127.0.0.1`은 호스트나 다른 컨테이너가 아닙니다.
- MQTT: 기존 브로커의 `MQTT_BROKER`, 인증, `MQTT_TOPICS` JSON 배열과 `MQTT_DATA_FORMAT`을 설정합니다. 이 저장소는 브로커를 실행하지 않습니다.
- `home`, `mqtt`는 서버 확장 프로필입니다. `server` 없이 단독 실행하지 마세요.
- 홈 집계와 raw 보존 정책은 `HOME_AGG_SOURCES`에 명시한 테이블·컬럼에만 적용됩니다. 기본값 `[]`는 집계 대상 없음입니다.

설정 후 `docker compose --env-file .env up -d`를 실행합니다. Telegraf 상태 메트릭도 수집하려면 `.env.example`의 `TELEGRAF_SCRAPE_TARGETS` 예제를 적용하세요.

**MQTT 재시작 내구성에는 발행자 QoS 1, 브로커 영속성, 고정된 `MQTT_CLIENT_ID`가 필요합니다.** Telegraf 디스크 버퍼만으로 QoS 0 메시지의 재시작 복구를 보장하지 않습니다. QoS 1 재전송은 중복될 수 있습니다.

### 차량 CAN / VSS

차량 장비에 별도 `.env`를 두고 **`COMPOSE_PROFILES=vehicle`만 설정**합니다. 서버 프로필을 함께 켜지 마세요.

필수 준비:

1. Linux SocketCAN 장비와 미리 활성화한 `CAN_INTERFACE`. 이 Compose는 CAN bitrate나 링크 상태를 설정하지 않습니다.
2. `VEHICLE_ID`, `VEHICLE_FIRMWARE`, `VSS_VERSION`, `DECODE_EPOCH`.
3. primary/supplemental DBC의 URL·SHA256·전체 Git commit hash, VSS mapping URL·SHA256. 선택적 override 사용 시 해당 버전·검증값도 설정합니다.
4. 원본 보관용 `RAW_BUCKET`, `RAW_S3_*` 자격 증명.
5. 차량 컨테이너에서 접근 가능한 `GREPTIME_HTTP_URL`과 DB 인증. 기본 `http://greptimedb:4000`은 별도 차량 장비에서 서버 주소로 사용할 수 없습니다.

전체 계약은 [차량 환경변수](.env.example)와 [차량 구성](compose/vehicle-vss.yaml)에 있습니다. 물리 CAN 동작이나 특정 차량 펌웨어 호환성은 이 문서로 검증되었다고 주장하지 않습니다. DBC·매핑은 실제 차량에 맞게 검증해야 합니다.

```bash
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d
```

원본은 MF4, ingress sidecar, manifest를 함께 저장합니다. 객체 경로는 `raw/vehicle/<id>/can/YYYY/MM/DD/<manifest-sha256>/<원래 파일명>`이며, 업로드한 세 파일의 검증이 끝나기 전에는 로컬 spool을 제거하지 않습니다. 디코딩된 신호는 SQLite outbox를 거쳐 DB로 전달합니다.

DBC·VSS·펌웨어 pin 변경 시 recorder와 decoder를 먼저 정지하고 `DECODE_EPOCH`를 갱신한 뒤 재기동하세요. 실행 중인 decoder와 recorder가 서로 다른 정의를 사용하게 만드는 hot swap은 지원하지 않습니다.

오프라인 재해석은 `REDECODE_*`와 해당 DBC·mapping 설정을 준비한 후 실행합니다. 입력은 봉인된 로컬 MF4 또는 URL과 검증 정보이며, live outbox와 다른 outbox를 사용합니다. **오프라인은 CAN을 사용하지 않는다는 뜻이며, DB 전송까지 비활성화한다는 뜻은 아닙니다.**

```bash
COMPOSE_PROFILES=redecode docker compose --env-file .env run --rm redecode
```

## Grafana와 데이터 해석

12개 대시보드를 자동 provisioning합니다.

| 영역 | 대시보드 |
| --- | --- |
| 전체 / 운영 | Overview, Datalake Health |
| AI | AI Usage, AI Tools, AI Sessions |
| 차량 (차주용) | 차량 개요, 배터리 모니터, 충전, 주행 |
| 차량 (수집 진단) | 수집 진단 · CAN / VSS, 수집 진단 · DBC |
| 홈 | Home |

- `No data`는 정상이나 0을 의미하지 않습니다. 수집 미설정·누락·조회 범위를 먼저 확인하세요.
- AI 비용은 도구가 보고한 값과, 비용이 없을 때 적용 가능한 공개 단가의 보충 추정치입니다. 청구서가 아니며 가격을 결정할 수 없는 사용량은 미산정으로 남습니다. 보충 단가는 LiteLLM 공개 가격표를 조회·캐시합니다.
- native telemetry와 플러그인의 사용량은 식별 가능한 범위에서 native 우선 정책으로 처리합니다. 서로 다른 수집 범위를 섞으면 일부 사용량이 누락될 수 있습니다.
- Overview의 raw trace 조사는 선택 범위의 마지막 48시간에 제한됩니다. 관측된 span 수는 사용자 HTTP 요청 수가 아니며, 허용 목록 밖의 HTTP route·SQL 본문·span event를 복원할 수 없습니다.
- 기본 `OTEL_TTL`은 빈 값으로 **raw OTel 무기한 보존**입니다. 30~90일이 자동 적용되는 것이 아닙니다. 보존 기간을 줄이면 기존 데이터도 만료될 수 있습니다.
- AI 집계는 장기 보존합니다. Home raw는 설정된 대상에 기본 `HOME_RAW_TTL=90d`를 사용하며 `0s`로 만료를 끌 수 있습니다. raw CAN의 S3 보존 정책은 별도로 관리하세요.
- AI 요약 INSERT는 최대 500행씩 묶어 전송합니다. 세션의 새 시작점 저장이 모두 성공한 뒤에만 같은 `(client, session_id)`의 이전 시작점 행을 삭제합니다. 배치 실패 시 이전 행은 남으며 재실행이 성공하면 다시 정리합니다. 집계 주기·조회 한도·보존 정책은 바꾸지 않습니다.

## 백업과 복구

**DB 쓰기를 멈춘 뒤 수행하는 오프라인 작업입니다.** 운영 중인 DB를 복사하는 백업이나 주기 실행 스케줄러는 제공하지 않습니다. 외부 생산자도 정지·버퍼링하고, GitOps 자동 재기동이 백업 중 DB를 시작하지 않도록 조정하세요.

### 백업

서버의 기존 Compose 프로젝트와 `.env`로 실행합니다. Home/MQTT를 사용한다면 해당 프로필도 `.env`에 유지하세요.

```bash
docker compose --env-file .env stop
BACKUP_OFFLINE_CONFIRMED=1 docker compose --env-file .env --profile backup run --rm backup backup
# 백업 결과와 종료 코드를 확인한 다음 재기동
docker compose --env-file .env up -d
```

`BACKUP_OFFLINE_CONFIRMED=1`은 실제 정지를 대신하지 않습니다. 스크립트는 DB가 응답하면 작업을 거부합니다.

- 백업 범위는 `greptime-data`, `greptime-etc`와 S3 모드의 SST 객체입니다. **Grafana 사용자 설정, Alloy 큐, 차량 spool/outbox, `.env`는 이 백업에 포함되지 않습니다.** 별도 보관 정책이 필요합니다.
- S3 모드는 SST와 archive를 SHA256·크기로 검증하고 완료 표식을 남깁니다. live prefix와 `S3_BACKUP_PREFIX`는 겹치면 안 됩니다.
- `BACKUP_KEEP=7`은 로컬 백업 세대 수입니다. 원격 백업의 자동 정리 정책이 아닙니다. 복구에 필요한 prefix에 임의 만료 정책을 설정하지 마세요.

### 새 프로젝트로 복원

기존 프로젝트는 정지한 채 두고 **미사용 프로젝트 이름**과 빈 대상 볼륨을 사용하세요. S3 모드에서는 같은 스토리지 설정으로 원격 백업을 가져올 수 있습니다. `BACKUP_FILE`은 백업 ID/이름으로 지정할 수 있으며, 생략하면 최신 완료 백업을 선택합니다.

```bash
COMPOSE_PROJECT_NAME=datalake-recovery BACKUP_OFFLINE_CONFIRMED=1 \
  docker compose --env-file .env --profile backup run --rm backup restore
# 복원 성공을 확인한 뒤 동일한 프로젝트 이름으로 시작
COMPOSE_PROJECT_NAME=datalake-recovery docker compose --env-file .env up -d
```

복원은 비어 있지 않은 대상 볼륨을 거부하며 강제 덮어쓰기 옵션은 없습니다. 같은 S3 live prefix를 쓰는 기존 DB와 복원 DB를 동시에 실행하지 마세요. 복원 후 현재 `.env`로 인증 설정이 다시 생성됩니다.

`File` 모드에서 같은 호스트의 백업을 사용하려면 기존 백업 볼륨의 실제 이름을 `BACKUP_VOLUME_NAME`에, `BACKUP_VOLUME_EXTERNAL=true`를 복구용 환경설정에 지정하세요. 다른 호스트로 옮길 때는 백업 archive와 checksum도 안전하게 이전해야 합니다.

**`docker compose down -v`는 복구 절차가 아닙니다.** 사용 중인 데이터 볼륨을 삭제하지 마세요.

## 개발과 검증

| 경로 | 역할 |
| --- | --- |
| [`compose/`](compose/) | 서비스·수집기·대시보드 원본 조각 |
| [`scripts/`](scripts/) | 초기화, 집계, 차량 기록, 백업·복원 구현 |
| [`plugins/`](plugins/) | 코딩 에이전트 연결과 설치기 |
| [`tools/render.py`](tools/render.py) | 원본을 단일 배포 파일로 생성 |
| [`tools/check_env.py`](tools/check_env.py) | Compose 환경변수와 예제의 일치 검사 |
| [`tests/`](tests/) | 동작·보안·스토리지·렌더링 검증 |
| [`.github/workflows/compose.yml`](.github/workflows/compose.yml) | push 시 플러그인·렌더러·생성물·프로필 검사 |

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

## 문제 해결

| 증상 | 먼저 확인할 항목 |
| --- | --- |
| 서버가 시작되지 않음 | `db-init`, `db-schema` 종료 코드·로그, 필수 인증, S3 HTTPS endpoint·region |
| 초기화 컨테이너가 종료됨 | `Exited (0)`이면 정상. 상시 서비스와 구분 |
| 원격에서 Grafana/수집기에 접속 불가 | loopback 바인딩, SSH 터널, 실제 포트, 방화벽 |
| OTLP 401 | DB/Grafana 계정이 아닌 `OTLP_USER` / `OTLP_PASSWORD` 사용 여부 |
| 플러그인 설치 후 데이터 없음 | 재시작·훅 승인, 에이전트 장비 기준 endpoint, 실제 새 세션, 집계 주기 |
| Home/MQTT 대시보드에 데이터 없음 | 서버 확장 프로필, 생산자 설정, raw 수신, `HOME_AGG_SOURCES` |
| 저장소 변경 후 preflight 거부 | 기존 볼륨에 다른 스토리지를 연결했는지 확인. 강제 우회 대신 새 볼륨 복원 |
| 백업·복원 거부 | DB 실제 정지, 오프라인 확인 플래그, 빈 복원 대상, S3 객체 충돌 |

진단 로그를 공유할 때도 토큰·비밀번호·인증 헤더·개인 데이터가 포함되지 않았는지 확인하세요.
