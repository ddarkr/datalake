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

볼륨 루트는 UID/GID `10001:10001`, `0700`, 정의 파일은 같은 소유자의 `0600`입니다. receiver는 UID/GID `10001:10001`로 실행하며 정의 볼륨과 코드 configs는 read-only입니다. raw SQLite `/data/raw.sqlite3`와 관련 WAL/SHM도 비공개로 보존합니다. 내용 정렬·재직렬화·줄바꿈 변경도 epoch를 바꿀 수 있으므로 운영 중 정의를 덮어쓰지 마세요.

```bash
docker compose --env-file .env --profile can-receiver config --quiet
docker compose --env-file .env --profile can-receiver up -d can-receiver
docker compose --env-file .env --profile can-receiver exec can-receiver \
  /opt/venv/bin/python /app/can_receiver.py status --database /data/raw.sqlite3
```

초기 `can-receiver-deps`는 PyPI 네트워크가 필요하며 pinned 의존성이 바뀌면 venv를 다시 만듭니다. 코드 configs를 바꿀 때는 해당 receiver의 revision label도 갱신해 재생성을 유도합니다. host endpoint는 기본 `127.0.0.1:4319/v1/logs`입니다. 기존 SSH 터널과 CAN Basic 인증 경계를 유지하고 공개 인터페이스로 바인딩하지 마세요. Basic 인증만으로 전송이 암호화되지는 않습니다. 기존 Alloy AI privacy ingress는 raw CAN payload용 경로가 아닙니다.

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
  /opt/venv/bin/python /app/can_receiver.py re-decode \
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

### 운영 health 시각을 구분하기

- Fleet의 `fleet_last_receive_timestamp_seconds{topic=...}`는 대상 차량의 유효 envelope를 로컬에서 받은 시각이며, 중복 재수신도 갱신합니다. VSS의 `vss_last_receive_timestamp_seconds`는 broker update/snapshot의 실제 로컬 수신 시각입니다. 받은 적이 없거나 recorder를 재시작한 뒤 아직 수신하지 않았으면 metric이 없습니다. `datalake_trace_last_received_timestamp_seconds{client=...}`는 trace-privacy가 비어 있지 않은 trace batch를 redaction 후 받은 시각이며 DB 저장 ACK가 아닙니다. 이 시각도 프로세스 재시작 후 첫 수신 전에는 없습니다. AI logs/metrics와 Home raw event 시각은 수신 시각이 아니므로 해당 **마지막 실제 수신은 unknown**입니다.
- `fleet_outbox[_events]_oldest_enqueue_age_seconds`와 `vss_outbox_oldest_enqueue_age_seconds`는 로컬 수신 시 저장한 `ingest_time` 기준 체류 초입니다. `*_oldest_event_age_seconds`는 원본 `event_time`의 age로, 늦은 재전송은 이벤트가 오래되어도 큐 체류는 짧을 수 있습니다. 빈 큐만 0이고, 읽기 실패는 `*_outbox_metrics_success=0` 및 age 없음입니다(VSS 기존 pending/time metric은 실패 시 -1). 통신·조회 실패를 정상 0으로 해석하지 마세요.
- aggregate는 기존 `backup-data` 볼륨의 `/ops/aggregate-status.json`을 pass/section 시작과 pass 완료에 atomic 교체합니다. storage-metrics는 같은 볼륨을 read-only로 읽습니다. `datalake_aggregate_running`, `datalake_aggregate_success`(마지막 완료 pass 결과), `last_start_timestamp_seconds`, `last_success_timestamp_seconds`, `last_failure_timestamp_seconds`를 노출하며 실패해도 이전 성공 시각은 보존합니다. `time() - datalake_aggregate_last_success_timestamp_seconds`는 **실행 성공 freshness**이지 데이터 처리 지연이 아닙니다. section이 오래 실행되거나 프로세스가 죽으면 `status_timestamp_seconds`가 오래된 채 남을 수 있으므로 interval/running과 함께 확인합니다. SQL pass 성공은 배터리 분석의 교정·품질 정상 판정과 다릅니다.
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
