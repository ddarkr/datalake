# 설치와 운영

[doda_datalake 소개](../README.md) · [개발과 검증](development.md) · [Tesla Fleet Telemetry](tesla_fleet.md)

이 문서의 명령은 별도 안내가 없으면 저장소 최상위 디렉터리에서 실행합니다.

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

`storage-metrics`는 CAN 원본 볼륨을 읽기 전용으로 관찰합니다. 외부 볼륨은 Compose가 만들거나 삭제하지 않으므로 첫 기동 전에 준비하세요. CAN 수집을 사용하지 않으면 빈 볼륨이며 해당 지표는 unknown입니다. 아래는 기본 이름입니다. `.env`에서 `CAN_RECEIVER_RAW_VOLUME`을 바꿨다면 그 이름을 사용하세요.

```bash
docker volume create datalake_can-receiver-raw
```

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

[플러그인 설치·연결 가이드](../plugins/README.md)에 Codex, OMP, opencode2, Claude Code와 AGY CLI 연결 절차가 있습니다. Hermes는 [별도 native 플러그인 가이드](../plugins/hermes/README.md)를 사용하며 공통 설치기가 활성화하지 않습니다. Node.js 22 이상이 필요하며, 설치·설정 명령은 기본적으로 미리보기이고 `--apply`로 적용합니다.

- 플러그인은 세션·토큰·도구 실행 **메타데이터**를 전송합니다. 프롬프트, 답변·추론 원문, 도구 인자·결과, 파일 내용은 전송 대상에서 제외합니다.
- 서버 `.env` 전체를 에이전트 장비에 복사하지 마세요. 수집 주소와 OTLP 인증만 전달합니다.
- OMP와 opencode2는 설치 후에도 저장소 경로를 참조합니다. 저장소를 이동·삭제하지 마세요.
- 플러그인 설치 후 클라이언트를 재시작하고 실제 새 세션을 확인해야 합니다. Codex는 훅 신뢰 승인도 필요합니다.
- 공통 JavaScript 플러그인은 허용된 메타데이터를 로컬 영속 큐에 저장하고 별도 sender로 전송합니다. 로컬 수락은 collector ACK가 아니며, ACK 전까지 레코드는 보류 상태로 남습니다. Hermes의 SQLite outbox는 별도 계약을 따릅니다. 서버 Alloy 큐는 서버에 도착하지 못한 데이터를 복구하지 못합니다.
- Linux native Node에서는 검증된 현재 실행 이미지를 `/proc/self/exe`로 고정해 sender를 시작하므로 도구 캐시의 쓰기 가능한 상위 경로를 신뢰하지 않습니다. 프로젝트 내부 Node와 안전하지 않은 PATH 후보는 사용하지 않으며, 안전한 런타임이 없으면 수락된 데이터는 보류 상태로 유지됩니다.

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

전체 계약은 [차량 환경변수](../.env.example)와 [차량 구성](../compose/vehicle-vss.yaml)에 있습니다. 물리 CAN 동작이나 특정 차량 펌웨어 호환성은 이 문서로 검증되었다고 주장하지 않습니다. DBC·매핑은 실제 차량에 맞게 검증해야 합니다.

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

### 별도 CAN uploader의 서버 receiver

`can-receiver`는 opt-in 서버 프로필입니다. 이 저장소가 receiver·디코더·배포를 관리하며 차량 측 capture/uploader는 `tesla-obd`에서 관리합니다. 기존 `vehicle`의 SocketCAN/MF4 경로와 혼용하지 않습니다. 공개 Git에는 DBC, companion evidence JSON, 실제 원본·샘플·차량 식별자·자격 증명을 넣지 않습니다.

서버의 비공개 `.env`에 `COMPOSE_PROFILES=server,can-receiver`, `CAN_OTLP_USER`, `CAN_OTLP_PASSWORD`, `CAN_VEHICLE`, `CAN_COLLECTOR_ID`를 설정합니다. 기존 uploader의 인증·identity를 그대로 유지하세요. `GREPTIME_HTTP_URL` 기본값은 컨테이너 네트워크의 `http://greptimedb:4000`이며 `GREPTIME_DB/USER/PASSWORD`는 **서버에만** 둡니다. 차량에는 수신 주소와 CAN 인증만 전달합니다. 빈 인증·identity는 inactive profile의 구조 검사를 허용하지만 실제 receiver 기동은 거부됩니다.

#### 새 볼륨 준비

두 볼륨은 external이므로 Compose가 생성하지 않습니다. 다음은 **새 설치의 기본 이름** 예제입니다. `.env`에서 `CAN_RECEIVER_RAW_VOLUME` 또는 `CAN_RECEIVER_DEFINITIONS_VOLUME`을 변경했다면 명령에도 동일한 이름을 사용하세요. 기존 raw 볼륨을 초기화하는 명령이 아닙니다.

```bash
docker volume create datalake_can-receiver-raw
docker volume create datalake_can-receiver-definitions
docker run --rm --user 0:0 -v datalake_can-receiver-raw:/data \
  -v datalake_can-receiver-definitions:/definitions \
  python:3.12.8-slim-bookworm sh -ec \
  'chown 10001:10001 /data /definitions; chmod 0700 /data /definitions'
```

운영자가 외부에서 확보한 private DBC와 companion 정의 JSON의 **정확한 원본 바이트**를 각각 `/definitions/observed.dbc`, `/definitions/observed.json`으로 공급합니다. 아래 `/private/path/...`는 실제 비공개 파일 경로로 바꾸되 Git으로 복사하지 마세요. receiver를 시작하기 전에만 실행합니다.

```bash
docker create --name can-definitions-load --user 0:0 \
  -v datalake_can-receiver-definitions:/definitions \
  python:3.12.8-slim-bookworm sh -ec \
  'chown 10001:10001 /definitions/observed.dbc /definitions/observed.json; chmod 0600 /definitions/observed.dbc /definitions/observed.json'
docker cp /private/path/observed.dbc can-definitions-load:/definitions/observed.dbc
docker cp /private/path/observed.json can-definitions-load:/definitions/observed.json
docker start -a can-definitions-load
docker rm can-definitions-load
```

볼륨 루트는 UID/GID `10001:10001`, `0700`, 정의 파일은 같은 소유자의 `0600`입니다. receiver는 UID/GID `10001:10001`로 실행하며 루트 파일시스템, 정의·코드·venv 볼륨은 read-only입니다. raw SQLite `/data/raw.sqlite3`와 관련 WAL/SHM도 비공개로 보존합니다. 내용 정렬·재직렬화·줄바꿈 변경도 epoch를 바꿀 수 있으므로 운영 중 정의를 덮어쓰지 마세요.

```bash
docker compose --env-file .env --profile can-receiver config --quiet
docker compose --env-file .env --profile can-receiver up -d can-receiver
docker compose --env-file .env --profile can-receiver exec can-receiver \
  /opt/venv/bin/python -m scripts.ingest.can.can_receiver status --database /data/raw.sqlite3
```

초기 `can-receiver-deps`는 inline 코드를 `receiver-code` 볼륨에 준비한 뒤 pinned 의존성을 설치합니다. 읽기 전용 receiver 루트에 inline config를 직접 복사하지 않습니다. PyPI 네트워크는 첫 설치와 의존성 변경 시 필요하며, 코드 변경 시 deps와 receiver **두 서비스의 revision label**을 함께 갱신하세요. host endpoint는 기본 `127.0.0.1:4319/v1/logs`입니다. 기존 SSH 터널과 CAN Basic 인증 경계를 유지하고 공개 인터페이스로 바인딩하지 마세요. Basic 인증만으로 전송이 암호화되지는 않습니다. 기존 Alloy AI privacy ingress는 raw CAN payload용 경로가 아닙니다.

CAN OTLP 요청은 최대 10,000레코드이며, 요청 본문은 gzip 해제 후에도 2 MiB 이하여야 합니다. 레코드 상한과 바이트 상한을 모두 만족해야 수신합니다.

디코딩은 쓰기 잠금 밖에서 최대 1,000청크·2,000출력 행을 준비하고, 청크 사이의 경과 시간이 50ms를 넘으면 다음 commit으로 넘깁니다. 조밀한 청크도 parser 상태와 frame 경계 cursor로 이어서 처리하므로 한 청크의 모든 신호를 메모리에 펼치지 않습니다. commit 전 각 세션의 기존 cursor·상태를 다시 확인하며, parser 상태·decode cursor·outbox는 한 트랜잭션에서 함께 commit하거나 rollback합니다. 개별 frame·commit 시간을 포함한 엄격한 50ms 상한은 아니며, 원본 보존과 `WAL`/`synchronous=FULL`은 유지합니다.

단일 ordered 디코더와 단일 업로드 워커는 같은 영속 outbox를 독립적으로 처리합니다. Greptime 응답이 지연되어도 디코딩은 계속됩니다. 업로드는 최대 20,000행과 URL-encoded HTTP body 4 MiB를 모두 만족하는 가장 긴 순서 보존 prefix를 전송하며, 배치를 채우려고 기다리지 않습니다. `--outbox-limit`은 1–20,000행, `--max-body-bytes`는 body byte 예산입니다. 한 행만으로 byte 예산을 초과하면 `greptime_row_too_large`로 남기고 해당 행을 건너뛰거나 삭제하지 않습니다. DB 요청의 `--greptime-timeout` 기본값은 60초이며, ingress socket의 `--http-timeout` 기본값 15초와 별개입니다. 전체 행 수가 ACK된 배치만 outbox에서 제거하고, 오류·부분 ACK·`greptime_timeout`은 배치 전체를 보존합니다. 정수 nanosecond는 `TIMESTAMP(9)` 대상 column에 정수 literal로 전달하여 정밀도를 유지하면서 GreptimeDB 1.2.1의 literal INSERT fast path를 사용합니다. 정상 종료는 진행 중인 업로드 완료 또는 DB timeout까지 archive 독점 소유권을 유지합니다. SQLite 원본의 수신 ACK는 Greptime 저장 완료를 뜻하지 않습니다.

signal 행의 전체 ACK 뒤 같은 시간창의 `vehicle_signal_dirty` 갱신까지 ACK되어야 outbox에서 제거합니다. 알림 실패는 `dirty_notify_failure`로 남고 signal 재전송은 같은 event ID로 중복 제거됩니다. 집계기는 완료된 시간창을 scope별 영속 sweep cursor로 순회하므로 알림 이전의 과거 CAN도 처리합니다. 시간창의 raw fingerprint·generation·분석 설정과 코드 revision이 이전 checkpoint와 같으면 재계산하지 않으며, 처리 중 입력이 바뀌거나 분석이 실패하면 checkpoint를 완료하지 않습니다. 이 확인은 해당 scope·시간창의 처리 기록이지 물리 교정 성공 증거가 아닙니다.

`status`는 영속 누적 counter와 indexed cursor를 사용하며 원본 전체를 매번 세지 않습니다. `last_receive_ns`, `last_decode_ns`, `last_full_ack_ns`는 각각 raw 수신·해석·signal 및 dirty 알림 전체 ACK 시각입니다. storage-metrics는 raw volume을 read-only로 읽어 raw/WAL/outbox/파일시스템 여유와 제한된 오류 종류를 노출합니다. 누락·읽기 실패는 unknown이고 빈 정상 큐만 0입니다. 수신·디코드 commit 전 기본 64 MiB 여유에 staged write 예산을 더해 확인하며, 부족하면 `archive_disk_reserve`로 거부하고 기존 raw/outbox를 보존합니다. 이미 ACK된 outbox를 지우는 작업은 공간을 회수할 수 있도록 이 reserve 때문에 막지 않습니다.

#### CAN archive 백업과 새 볼륨 복원

DB 오프라인 백업과 별개입니다. `can-backup`은 실행 중인 receiver의 SQLite read transaction을 고정해 backup API로 복사하고 정의 바이트·epoch·cursor·outbox를 함께 검증합니다. live receiver나 DB를 중지할 필요가 없습니다. `CAN_STORAGE_TYPE=File`은 로컬 보관만 하며, off-host 보관에는 `CAN_STORAGE_TYPE=S3`와 기존 S3 설정, live prefix와 겹치지 않는 `CAN_S3_BACKUP_PREFIX`를 지정합니다. tar·SHA256 sidecar·manifest·COMPLETE를 GET으로 검증한 뒤 성공하며 원격 자동 GC는 하지 않습니다.

```bash
docker compose --env-file .env --profile backup run --rm can-backup backup
# 새 복원 대상 두 볼륨에 writer가 없음을 확인한 뒤 실행합니다.
BACKUP_OFFLINE_CONFIRMED=1 \
  docker compose --env-file .env --profile backup run --rm can-backup restore
```

복원 대상은 `CAN_RESTORE_RAW_VOLUME`과 `CAN_RESTORE_DEFS_VOLUME`의 **별도 빈 볼륨**입니다. 기본 이름은 `${COMPOSE_PROJECT_NAME}_can-restore-raw`와 `${COMPOSE_PROJECT_NAME}_can-restore-defs`이며 각각 receiver의 `/data`, `/definitions`에 직접 연결할 수 있습니다. `CAN_BACKUP_FILE`로 완료 백업을 선택할 수 있습니다. CAN 복원의 offline 확인은 이 두 **대상**에 대한 확인이며 live 원본의 WAL·소유권 lock은 그대로 유지할 수 있습니다. 원본과 같은 대상, 기존 파일이 있는 대상, 사용 중인 대상에는 복원하지 않습니다. 원본의 UID/GID와 `0700` 디렉터리·`0600` 파일 모드를 그대로 복원하며 접근을 위해 권한을 넓히지 않습니다.

복원본의 정의 SHA·연속 seq·epoch·cursor·outbox를 확인한 뒤 별도 receiver로 검증하세요. 운영 receiver를 복원본으로 인계하는 것은 별도 승인된 절차이며, 같은 archive에 두 writer를 시작하지 마세요. `CAN_BACKUP_RESERVE_BYTES`와 `BACKUP_RESERVE_BYTES`는 각각 CAN·DB staging 예산에 추가할 여유 byte이며 기본 64 MiB입니다. 백업은 snapshot과 압축 작업 공간, 복원은 archive와 해제될 파일 및 대상 파일시스템별 공간을 사전 검사합니다. 공간 부족을 발견하면 기존 원본·복원 대상 데이터를 변경하지 않습니다.

`python -m tests.test_can_receiver_compose`는 별도 Docker 프로젝트와 합성 정의로 실제 읽기 전용 receiver를 시작하고, 인증된 gzip OTLP의 영속 ACK·재시작·중복 재전송 보존을 검사한 뒤 소유 볼륨만 제거합니다. 운영 원본과 자격 증명을 사용하지 않습니다.

#### 기존 receiver 인계

1. 기존 receiver의 raw volume 이름, identity·인증, 정의 두 파일의 SHA256, 상태·epoch를 비공개로 기록하고 SQLite-consistent 백업을 확보합니다. **온라인 백업은 SQLite backup API 등 일관성 있는 방식으로 수행하세요. WAL이 열린 `.db`만 `cp`하면 안 됩니다.** 오프라인 전체 볼륨 백업도 모든 writer를 먼저 중지해야 합니다.
2. **기존 receiver만** 정상 종료합니다. capture/uploader, DB, Grafana 등 다른 서비스를 중지하거나 재시작하지 않습니다. uploader는 기존 로컬 spool·cursor·immutable 요청을 유지한 채 서버 공백 동안 재시도합니다.
3. `CAN_RECEIVER_RAW_VOLUME`을 기존 raw volume의 정확한 이름으로 지정합니다. 새 빈 raw volume으로 교체하거나 DB를 복사·삭제하지 않습니다. 기존 `/data/raw.sqlite3`와 WAL/SHM, decode cursor·outbox·epoch를 그대로 사용하며 소유자 `10001:10001`와 private 모드를 확인합니다. 필요한 권한 조정은 writer를 멈춘 상태에서만 수행합니다.
4. 동일한 정의 바이트를 private definitions volume에 공급합니다. 서버 이전을 이유로 새 epoch를 만들거나 `re-decode`를 실행하지 않습니다. 기존 worker가 완전히 종료된 뒤 위 명령으로 새 receiver만 시작합니다. **같은 raw archive에 구·신 worker를 병렬 실행하지 마세요.**
5. `status`에서 기존 epoch·cursor가 이어지는지 확인하고, uploader endpoint·인증과 실제 ACK를 확인합니다. 원본은 어느 쪽에서도 삭제하지 않습니다. rollback도 새 receiver만 종료한 뒤 동일 볼륨·정의로 기존 receiver를 단독 기동합니다.

의도적으로 정의를 변경하는 재해석은 receiver를 먼저 중지하고 새 정의를 검토한 뒤 아래처럼 같은 venv·볼륨의 단독 컨테이너에서 실행합니다. 이 작업은 새 decode epoch를 명시적으로 등록하므로 단순 배포 인계와 구분합니다. 정의 파일 교체는 receiver를 중지한 상태에서만 수행합니다.

```bash
docker compose --env-file .env --profile can-receiver stop can-receiver
docker compose --env-file .env --profile can-receiver run --rm --no-deps can-receiver \
  /opt/venv/bin/python -m scripts.ingest.can.can_receiver re-decode \
  --database /data/raw.sqlite3 --dbc /definitions/observed.dbc \
  --definitions /definitions/observed.json
docker compose --env-file .env --profile can-receiver up -d can-receiver
```

완전한 OTLP ACK는 **서버 raw SQLite의 durable 수락**이지 Greptime commit이 아닙니다. downstream 장애 시 raw와 outbox가 남으며, ACK는 차량 원본 삭제 허가가 아닙니다. 자동 raw GC를 추가하지 말고 SQLite archive·차량 원본을 보존하세요. epoch·정수 nanosecond·quality를 유지하며 `reported_unverified` 등을 집계 편의를 위해 `valid`로 바꾸지 않습니다.

다음 해석 대상은 세션별 cursor와 기존 `(session, seq)` 인덱스로 찾고, 후보 중 원본 도착 순서가 가장 빠른 chunk를 처리합니다. 보존한 원본 전체를 chunk마다 재검색하지 않습니다. 이 조회 최적화는 decode epoch·cursor 형식·영속 커밋을 바꾸지 않으므로 재해석이나 archive 마이그레이션이 필요하지 않습니다.

Grafana의 기존 **수집 진단 · CAN / VSS** 대시보드는 `vehicle_signal`의 `source='can'`을 조회하므로 custom `Vehicle.CAN.*` 경로 확인에 별도 대시보드가 필요하지 않습니다. FIFO backlog는 과거 `event_time`으로 들어오므로 실제 수집 세션의 시간 범위로 조회하세요. 최근 24시간이 비어 있다는 사실만으로 ingest 실패를 단정하지 않습니다. 이 정의와 custom 경로는 공식 VSS/Fleet 의미나 물리 교정 검증을 보장하지 않습니다. 기존 trip/charge/battery 분석의 표준 경로·quality 조건을 통과한다고 가정하지 마세요.

### Tesla Fleet Telemetry

기존 Fleet Telemetry ZMQ 발행자가 있다면 서버에 `fleet` 프로필을 추가해 수신 전용 recorder를 실행할 수 있습니다. 이 저장소는 Fleet Telemetry 서버를 설치하거나 Tesla API로 차량 설정·명령을 전송하지 않습니다.

- `FLEET_ZMQ_ENDPOINT`와 `TESLA_HELPER_NETWORK`를 실제 발행자 및 접근 가능한 외부 Docker 네트워크에 맞춰 설정합니다.
- 단일 차량 선택에는 `TARGET_VIN`과 `VEHICLE_ID`를 사용합니다. 대상 VIN을 지정하지 않으면 `VEHICLE_ID_SALT`가 필수입니다.
- 신호·경고·오류·연결 상태를 구독하며, CAN 입력과 다른 `source='fleet'`으로 저장합니다.
- ZMQ는 비영속 전송입니다. SQLite outbox는 recorder가 수신·저장한 이후의 데이터만 보호합니다.

수집 계약과 배터리 분석의 조건·제한은 [Tesla Fleet Telemetry 문서](tesla_fleet.md)를 참고하세요.

### 같은 차량의 Fleet / CAN 식별자 연결

같은 실제 차량이 수집 경로마다 다른 `vehicle`로 저장되었다면 `vehicle_identity`에 두 원본 ID와 공통 표시명을 등록합니다. Grafana의 차량 목록에는 공통 표시명 하나가 나타나며, 선택하면 기존 기록과 이후 같은 원본 ID로 들어오는 기록을 함께 조회합니다. 등록하지 않은 차량은 기존 ID로 표시됩니다. 실제 VIN이나 운영 식별자는 공개 Git에 넣지 마세요.

아래는 합성 ID 예시입니다. 운영 DB에서 각 원본 ID가 같은 차량인지 확인한 뒤 적용하세요. `mapped_at`은 항상 같은 epoch 값을 사용하므로 반복 등록이나 표시명 변경은 같은 키를 갱신합니다. 이 테이블은 보존 기간 만료 대상이 아닙니다.

```sql
INSERT INTO vehicle_identity (mapped_at, vehicle, canonical_vehicle) VALUES
  ('1970-01-01 00:00:00', 'demo-can', 'demo-car'),
  ('1970-01-01 00:00:00', 'demo-fleet', 'demo-car');
```

연결은 조회용입니다. 원본 데이터·수집기 설정·재전송 ID·분석 교정의 `vehicle/source/decode_epoch`는 바꾸지 않습니다. Fleet/CAN과 해석 버전이 다른 에너지 값은 합산하지 않고 출처별로 표시합니다. 상세 행의 원본 차량 ID는 provenance로 남습니다. 서로 다른 차량을 모델명만 같다는 이유로 연결하지 마세요.

### 배터리 교정과 모델을 비공개로 주입하기

공개 기본 설정은 미교정 `{}`이며, [설정 구조 예제](../config/battery-analysis.example.json)의 각 모듈도 비어 있습니다. 개인 차량 식별자·교정값·학습 모델을 이 예제나 `compose.yaml`에 추가해 커밋하지 마세요.

기존 설정 로더는 `/app/battery-analysis.json`을 읽습니다. Compose가 비공개 `.env`의 `BATTERY_ANALYSIS_CONFIG_JSON` 값을 해당 read-only config 파일로 전달하므로 서버 배포는 계속 `compose.yaml`과 `.env` 두 파일만 사용합니다. 값이 없으면 `{}`가 전달됩니다.

```dotenv
BATTERY_ANALYSIS_CONFIG_JSON='{"conditions":{},"energy":{},"electrical":{},"rul":{},"alerts":{}}'
```

위 값은 미교정 예제입니다. 실제 JSON은 확인된 차량·source·epoch·domain·단위에 맞춰 `.env` 안에서만 편집하고 파일 권한을 `600`으로 제한하세요. 작은따옴표로 감싼 dotenv 값은 `$`의 변수 치환을 막습니다. 전체 `docker compose config` 출력에는 교정 정보와 인증값이 포함될 수 있으므로 구조 검사는 `config --quiet`로 수행합니다.

환경변수 대신 비공개 `compose.private.yaml`에서 `configs.battery_analysis_json.content`를 교체할 수도 있습니다. 이 파일도 Git 밖에 보관하며, 아래 명령에는 실제 파일 경로를 사용합니다. 명령은 기본 Compose와 private override를 명시적으로 함께 읽습니다.

```bash
docker compose --env-file .env -f compose.yaml -f /private/path/compose.private.yaml config --quiet
```

Compose 관리 도구가 단일 파일만 읽는다면 `.env` 주입 방식을 사용하세요. 교정값이 없는 기본값이나 잘못된 명시적 설정은 정상·0으로 처리하지 않습니다. 적용 전 설정을 보존하고 설정 파일의 존재·파싱·scope와 필요한 분석 결과를 확인하세요.

## Grafana와 데이터 해석

16개 대시보드를 `Datalake` 아래에 자동으로 공급합니다.

| 폴더 | 대시보드 |
| --- | --- |
| Datalake | 전체 현황 |
| AI | AI 사용량, AI 도구, AI 세션 |
| 홈 | 홈 |
| 차량 | 차량 개요 |
| 차량 / 주행 | 주행, CAN 주행 신호 |
| 차량 / 충전 | 충전 |
| 차량 / 배터리 | 배터리 모니터, CAN 배터리 신호, CAN 셀 전압 |
| 차량 / CAN 신호 | CAN 신호 탐색, 수집 진단 · CAN / VSS, 수집 진단 · DBC |
| 운영 | Datalake 상태 |

`grafana-folders` 초기화 작업은 Grafana가 준비된 뒤 내부 폴더 API로 계층을 맞춥니다. Grafana 12.4.0의 파일 공급 설정에는 상위 폴더 지정이 없어 별도 작업이 필요합니다. 기존 `Datalake` 폴더 UID와 대시보드 UID는 유지하며, 경로 이동 중 기존 대시보드를 삭제하지 않아 즐겨찾기를 보존합니다. 반복 실행해도 같은 폴더를 재사용합니다. `GF_ADMIN_USER`·`GF_ADMIN_PASSWORD`는 Grafana와 같은 값을 사용하며 인증 실패나 API 오류는 초기화 실패로 남습니다.

폴더와 대시보드 UID는 서로 겹치지 않아야 합니다. Grafana 12.4.0의 목록 화면은 두 종류를 UID로 식별하므로 홈 폴더와 홈 대시보드가 모두 `datalake-home`이면 선택 상태 계산이 재귀 호출되어 `Maximum call stack size exceeded`가 발생합니다. 홈 폴더 UID는 `datalake-home`, 홈 대시보드 UID는 `datalake-home-overview`로 구분합니다. 이 변경을 기존 설치에 공급하면 이전 UID의 홈 대시보드를 새 UID로 대체하므로 직접 대시보드 링크와 즐겨찾기는 다시 지정해야 합니다. 폴더 링크는 그대로 유지됩니다.

- `기록 없음`은 정상이나 0을 의미하지 않습니다. 수집 미설정·누락·조회 범위를 먼저 확인하세요.
- AI 비용은 도구가 보고한 값과, 비용이 없을 때 적용 가능한 공개 단가의 보충 추정치입니다. 청구서가 아니며 가격을 결정할 수 없는 사용량은 미산정으로 남습니다. 보충 단가는 LiteLLM 공개 가격표를 조회·캐시합니다.
- native telemetry와 플러그인의 사용량은 식별 가능한 범위에서 native 우선 정책으로 처리합니다. 서로 다른 수집 범위를 섞으면 일부 사용량이 누락될 수 있습니다.
- 전체 현황의 원본 트레이스 조사는 선택 범위의 마지막 48시간에 제한됩니다. 관측된 스팬 수는 사용자 HTTP 요청 수가 아니며, 허용 목록 밖의 HTTP 경로·SQL 본문·스팬 이벤트를 복원할 수 없습니다.
- 기본 `OTEL_TTL`은 빈 값으로 **raw OTel 무기한 보존**입니다. 30~90일이 자동 적용되는 것이 아닙니다. 보존 기간을 줄이면 기존 데이터도 만료될 수 있습니다. 정책·점검·적용 절차는 [OTel 보존](otel-retention.md)을 따릅니다.
- AI 집계는 장기 보존합니다. 홈 원본은 설정된 대상에 기본 `HOME_RAW_TTL=90d`를 사용하며 `0s`로 만료를 끌 수 있습니다. 원본 CAN의 S3 보존 정책은 별도로 관리하세요.
- AI 요약 INSERT는 최대 500행씩 묶어 전송합니다. 세션의 새 시작점 저장이 모두 성공한 뒤에만 같은 `(client, session_id)`의 이전 시작점 행을 삭제합니다. 배치 실패 시 이전 행은 남으며 재실행이 성공하면 다시 정리합니다. 집계 주기·조회 한도·보존 정책은 바꾸지 않습니다.

### 차량 개요에서 CAN 기록 확인하기

- 차량 목록은 `vehicle_identity`에 명시적으로 등록된 별칭만 같은 차량으로 묶습니다. 원본 식별자는 바꾸지 않으며 등록되지 않은 차량은 자동으로 합치지 않습니다. 차량·수집 경로·해석 버전은 필터와 결과에 유지하고, 서로 다른 수집원·해석 버전의 숫자를 합쳐 현재 값으로 표시하지 않습니다.
- **조회 범위 · 관측과 DB 수신** 표는 전체 보존 기간의 마지막 `event_time`과 `ingest_time`을 구분합니다. 기간 내 관측 행이 0이어도 수신 행이 있으면 과거 기록이 들어오는 경우일 수 있습니다. 행 수는 저장 행 수이며 중복 제거된 관측 수나 수집 성공률이 아닙니다.
- 마지막 관측 시각을 누르면 현재 차량·수집 경로·해석 버전을 유지한 채 그 시각 전후 1시간으로 이동합니다. 기본 최근 2일 범위를 자동으로 늘리거나 과거 값을 현재 값처럼 표시하지 않습니다.
- 데이터가 보이지 않으면 먼저 이 표의 **기간 내 관측 행**과 **기간 내 수신 행**을 비교하세요. 관측은 0인데 수신이 늘면 과거 자료의 적재일 수 있으므로 마지막 관측 링크로 이동합니다. 둘 다 0이면 차량·수집원·해석 버전과 실제 수집 경로를 확인하세요. 숫자를 표시하기 위해 조회 기간을 자동으로 넓히거나 품질 조건을 해제하지 않습니다.
- 차량을 선택하면 숨김 `vehicle_ids` 변수가 `vehicle_identity`의 명시적 매핑과 선택한 원본 ID를 한 번 조회합니다. 패널은 그 목록을 SQL 값으로 사용하므로 모든 원본 행에서 별칭 하위 쿼리를 반복하지 않습니다. 차량 선택·시간 범위 변경 시 목록을 다시 조회하며 등록되지 않은 ID도 그대로 조회됩니다. 매핑을 추가한 뒤에는 대시보드를 다시 로드해 차량 목록과 원본 ID 목록을 갱신하세요. 조회 범위 표는 관측/수신 시각과 기간 내 행 수를 한 번의 집계로 계산하고, 차량·수집원·해석 버전별 결과는 계속 분리합니다.
- CAN의 정확한 경로·신호명이 대응하는 잔량, 남은 에너지, 기어, 충전 상태와 외기온은 `reported_unverified`인 **미검증 보고값**으로 표시합니다. 최신 무효 값·단위 불일치·알 수 없는 enum은 과거 정상 값으로 대체하지 않으며, CAN 보고값 표에는 원본 단위·품질·관측 및 수신 시각을 남깁니다. 잔량 그래프는 `%` 단위의 허용된 보고값만 평균에 포함하고 유효 값이 없는 구간은 비웁니다.
- `CP_CHARGE_ENABLED`는 충전 허용 보고이지 실제 충전 중이라는 판정이 아닙니다. CAN 원시 전압·전류가 있어도 교정과 분석 결과가 없는 전력은 판단 불가로 남고, 대응하는 원본 신호가 없는 온도도 만들어내지 않습니다.
- 전력·팩 전압 카드의 미분석 입력 검사는 원본 field 이름 override를 놓치지 않도록 같은 `vehicle/source/decode_epoch`의 전체 raw frontier를 사용합니다. 분석과 무관한 새 신호도 이전 숫자를 가릴 수 있는 보수적 제한이며, 서로 다른 scope의 입력은 섞지 않습니다.
- stat 카드의 값 선택은 표시 이름 기준이므로 `보고값` 표시 필드를 선택합니다. 원본 `reading` 이름 선택은 표시 이름 override와 어긋나 빈 카드가 됩니다. 값이 없으면 기존 `기록 없음`·`판단 불가` 라벨을 유지합니다.

### CAN 원본 신호 대시보드

- **CAN 주행 신호**: 속도·가속 페달·기어·실제 및 명령 토크·축 회전수·출력 상한과 원본 축 출력 보고값입니다.
- **CAN 배터리 신호**: 차량 표시 및 BMS 잔량·팩 전압·팩 전류·남은 에너지·전체 팩 에너지·버퍼·최고 및 최저 온도·브릭 전압 보고값입니다. 전압과 전류를 곱해 미검증 전력을 만들지 않습니다.
- **CAN 셀 전압**: `BMS_brick0`부터 `BMS_brick95`까지 브릭 전압 96개의 추이와 개별 최신 보고값을 봅니다. 원본 번호 0–95를 유지하며 개별 셀 내부 상태를 확인한다는 뜻은 아닙니다. 서로 다른 관측 시각의 값을 동시 전체 배열이나 건강 판정으로 취급하지 않습니다.
- **CAN 신호 탐색**: 저장된 모든 경로를 검색하고 숫자 그래프·최신 보고·품질별 관측 목록을 봅니다. 차체·공조·미러 같은 상태 신호와 `unknown_enum`도 목록에서 숨기지 않습니다.
- 신규 화면은 `source='can'`으로 고정해 Fleet 입력이 섞이지 않게 합니다. 차량 별칭과 해석 버전 필터를 유지하며 최신 카드도 차량·해석 버전별로 분리합니다. 최신 무효 값·단위 불일치는 과거 정상 값으로 대체하지 않습니다.
- 모든 신규 화면의 조회 범위 표는 전체 보존 기간의 마지막 관측과 수신을 구분합니다. 마지막 관측 링크는 해당 화면과 필터를 유지한 채 전후 1시간으로 이동합니다. 기본 최근 2일 범위를 자동으로 늘리거나 과거 값을 현재 값처럼 표시하지 않습니다.


### 운영 health 시각을 구분하기

- Fleet의 `fleet_last_receive_timestamp_seconds{topic=...}`는 대상 차량의 유효 envelope를 로컬에서 받은 시각이며, 중복 재수신도 갱신합니다. VSS의 `vss_last_receive_timestamp_seconds`는 broker update/snapshot의 실제 로컬 수신 시각입니다. 받은 적이 없거나 recorder를 재시작한 뒤 아직 수신하지 않았으면 metric이 없습니다. `datalake_trace_last_received_timestamp_seconds{client=...}`는 trace-privacy가 비어 있지 않은 trace batch를 redaction 후 받은 시각이며 DB 저장 ACK가 아닙니다. 이 시각도 프로세스 재시작 후 첫 수신 전에는 없습니다. AI logs/metrics와 Home raw event 시각은 수신 시각이 아니므로 해당 **마지막 실제 수신은 unknown**입니다.
- `fleet_outbox[_events]_oldest_enqueue_age_seconds`와 `vss_outbox_oldest_enqueue_age_seconds`는 로컬 수신 시 저장한 `ingest_time` 기준 체류 초입니다. `*_oldest_event_age_seconds`는 원본 `event_time`의 age로, 늦은 재전송은 이벤트가 오래되어도 큐 체류는 짧을 수 있습니다. 빈 큐만 0이고, 읽기 실패는 `*_outbox_metrics_success=0` 및 age 없음입니다(VSS 기존 pending/time metric은 실패 시 -1). 통신·조회 실패를 정상 0으로 해석하지 마세요.
- aggregate는 기존 `backup-data` 볼륨의 `/ops/aggregate-status.json`을 pass/section 시작과 pass 완료에 atomic 교체합니다. storage-metrics는 같은 볼륨을 read-only로 읽습니다. `datalake_aggregate_running`, `datalake_aggregate_success`(마지막 완료 pass 결과), `last_start_timestamp_seconds`, `last_success_timestamp_seconds`, `last_failure_timestamp_seconds`를 노출하며 실패해도 이전 성공 시각은 보존합니다. `time() - datalake_aggregate_last_success_timestamp_seconds`는 **실행 성공 freshness**이지 데이터 처리 지연이 아닙니다. section이 오래 실행되거나 프로세스가 죽으면 `status_timestamp_seconds`가 오래된 채 남을 수 있으므로 interval/running과 함께 확인합니다. SQL pass 성공은 배터리 분석의 교정·품질 정상 판정과 다릅니다.
- pass 시작 간격은 monotonic clock으로 유지하며 느린 pass와 다음 pass를 겹치지 않습니다. status의 `section_durations_seconds`, `section_counts`, `vehicle_freshness`를 함께 보면 SQL 실행 성공 시각과 각 section 처리 시간·데이터 frontier를 구분할 수 있습니다. 과거 scope의 분리된 재계산은 일반 lookback 누락 경고를 만들지 않습니다.
- 배터리 조회는 같은 구간·scope의 이벤트를 먼저 읽고 분석에 필요한 `source_field`만 가져옵니다. 경고 이벤트가 없는 오프라인 구간에는 경고 문맥용 전체 신호 조회를 생략하지만, 필드와 무관한 scope 탐색은 유지해 기존 결과의 무효화를 빠뜨리지 않습니다. 실제 경고가 있거나 온라인 가용성 검사가 필요하면 기존 문맥을 보존합니다. `BATTERY_MAX_ROWS`는 필요한 입력의 누적 메모리 한도이며, 초과하면 부분 결과나 dirty checkpoint를 저장하지 않습니다.
- 선택된 신호가 없어도 조회에서 확인한 scope를 경고 분석에 전달하므로, 이전 경고 값은 입력 부재에 맞춰 무효화됩니다. `battery.conditions.diagnostics`의 수치는 분석에 전달된 신호 집합을 설명하며 전체 CAN 원본 행 수나 수집 완전성 지표가 아닙니다.
- `datalake_aggregate_vehicle_window_lag_seconds{source=...}`는 DB의 source별 최신 numeric raw event와 최신 `1m` 요약 window 끝 사이 양의 초 차이입니다. 최신 관측 watermark 간의 실제 event-window gap이지만 모든 차량/path의 처리 완전성이나 ingest/DB ack 지연을 증명하지 않습니다. DB 조회 실패는 `vehicle_window_observation_success=0` 및 lag 없음이고, raw/요약 중 하나가 없으면 unknown입니다. observation timestamp가 오래되면 lag도 stale입니다.
- status/restore 파일이 없거나 파싱 불가하면 `datalake_aggregate_status_known=0` / `datalake_restore_verification_known=0`이며 시각·성공을 만들어내지 않습니다. `datalake_restore_verification_timestamp_seconds`는 마지막 성공한 **오프라인 archive/SST SHA·내용 검증 및 staged 복원 완료** 시각입니다. 이후 DB 재기동·SQL 확인 성공을 뜻하지 않습니다. 오래된 성공, 새 실패, 진행 중, 미관측을 각각 구분하세요.

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
- inventory JSON(`manifest.json`의 archive 내/원격 사본과 `COMPLETE`)은 **각각 비압축 UTF-8 64 MiB 이하**여야 합니다. 백업 생성과 로컬·원격 복원에 같은 한도를 적용하며 초과한 백업은 성공으로 보고하지 않습니다. 8,000개 이상의 SST도 이 바이트 한도 안이면 복원할 수 있습니다. checksum sidecar는 1 MiB 이하입니다. JSON 읽기는 한도와 chunk 크기로 제한하고 원격 응답 스트림은 성공·실패 모두 닫습니다. **archive 전체나 SST 데이터 총량에는 이 JSON 한도를 적용하지 않습니다.**
- `BACKUP_KEEP=7`은 로컬 백업 세대 수입니다. 원격 보존은 기본 `off`이며 별도 승인된 `enforce`에서만 삭제합니다. [원격 보존 정책과 dry-run 절차](backup-retention.md)를 확인하고, 복구에 필요한 prefix에 임의 만료 정책을 설정하지 마세요.

### 새 프로젝트로 복원

기존 프로젝트는 정지한 채 두고 **미사용 프로젝트 이름**과 빈 대상 볼륨을 사용하세요. S3 모드에서는 같은 스토리지 설정으로 원격 백업을 가져올 수 있습니다. `BACKUP_FILE`은 백업 ID/이름으로 지정할 수 있으며, 생략하면 최신 완료 백업을 선택합니다.

```bash
COMPOSE_PROJECT_NAME=datalake-recovery BACKUP_OFFLINE_CONFIRMED=1 \
  docker compose --env-file .env --profile backup run --rm backup restore
# 복원 성공을 확인한 뒤 동일한 프로젝트 이름으로 시작
COMPOSE_PROJECT_NAME=datalake-recovery docker compose --env-file .env up -d
```

복원은 비어 있지 않은 대상 볼륨을 거부하며 강제 덮어쓰기 옵션은 없습니다. 같은 S3 live prefix를 쓰는 기존 DB와 복원 DB를 동시에 실행하지 마세요. 복원 후 현재 `.env`로 인증 설정이 다시 생성됩니다.

성공한 복원은 백업 볼륨의 `BACKUP_DIR/restore-verification.json`에 `{timestamp_seconds, backup_id}`를 atomic 교체로 기록합니다. 이는 archive 내용과 SST의 SHA256·크기 검증 및 대상 파일 복원이 끝났다는 **마지막 오프라인 성공 기록**이며, DB 재기동·SQL 조회·데이터 수집이 정상이라는 증거가 아닙니다. 실패하면 이전 성공 기록을 유지하고 파일이 없으면 검증 이력은 unknown입니다. 최신 복원 시도가 성공했다는 뜻으로 해석하지 말고 종료 코드도 확인하세요.

`File` 모드에서 같은 호스트의 백업을 사용하려면 기존 백업 볼륨의 실제 이름을 `BACKUP_VOLUME_NAME`에, `BACKUP_VOLUME_EXTERNAL=true`를 복구용 환경설정에 지정하세요. 다른 호스트로 옮길 때는 백업 archive와 checksum도 안전하게 이전해야 합니다.

**`docker compose down -v`는 복구 절차가 아닙니다.** 사용 중인 데이터 볼륨을 삭제하지 마세요.

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
