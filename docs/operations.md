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
| `GREPTIME_WRITE_CACHE_SIZE=10GiB` | S3 디스크 쓰기 캐시 상한. 서버 디스크 여유에 맞춰 조정하세요. |
| `GREPTIME_PAGE_CACHE_SIZE=512MB` | 메모리 페이지 캐시 |
| `GREPTIME_WRITE_BUFFER_SIZE=128MB` | 전역 쓰기 버퍼 |
| `GREPTIME_MIN_COMPACTION_INTERVAL=0m` | 자동 컴팩션 후보의 최소 간격 제한 없음 |

S3 디스크 쓰기 캐시 상한의 기본값은 10GiB이며, 실제 점유량은 데이터 수요에 따라 증가합니다. 변경은 설정 파일 재생성과 GreptimeDB 재기동 후 적용됩니다.

#### S3 주 저장소에서 다운로드 줄이기

기본 경로는 **S3의 장기 이력과 제한된 로컬 작업 캐시를 함께 사용하면서 정기 요약과 자동 유지보수를 수행하는 것**입니다. stock GreptimeDB 1.2.1, S3 주 저장소, WAL syncwrite는 유지합니다. 로컬 캐시가 전체 보존 이력을 담을 필요는 없으며 전체 이력 mirror를 만들지 않습니다.

- `server`는 `aggregate`를 포함하며 기본 300초 간격으로 AI/log/home/차량 요약과 조건을 충족하는 비-CAN 배터리 분석을 실행합니다. CAN은 기존 dirty/checkpoint 메타데이터에 따라 분·시간 rollup과 주행·충전 요약을 갱신하며 자동 배터리 분석에서는 제외합니다. 긴 기간 차트가 자동으로 raw로 대체된다고 가정하지 마세요.
- `min_compaction_interval=0m`, `schedule_compaction_after_edit=true`로 정상 자동 컴팩션과 flush/compaction 결과 반영 뒤 후속 후보 검사를 허용합니다. 모든 edit가 즉시 병합된다는 뜻은 아니며 실제 병합은 엔진의 후보·자원 조건을 따릅니다. 정기 전체 수동 compaction을 별도로 추가하지 마세요.
- 시작 시 인덱스 선다운로드(`preload_index_cache=false`)와 조회 후 전체 파일 cache refill(`enable_refill_cache_on_read=false`)을 끕니다. 필요한 조회·컴팩션은 S3를 읽을 수 있지만 조회를 계기로 전체 파일을 추가 다운로드하는 refill은 하지 않습니다. 반복 조회의 범위 GET은 남을 수 있습니다.

자동 집계와 컴팩션을 장기간 미루면 **SST 파일·중복 버전·만료 파일의 공간과 조회 비용이 증가할 수 있습니다.** 정상 스케줄은 Home의 `HOME_RAW_TTL` 정리와 aggregate retention watermark 갱신도 수행합니다. 엔진 TTL은 엄격한 삭제 기한을 보장하는 수단이 아니므로 용량·파일 수·조회 지연과 별도로 실제 보존 요구를 확인하세요.

고정 버전의 [자동 컴팩션 간격 검사와 수동 요청 경로](https://github.com/GreptimeTeam/greptimedb/blob/179ff8e5099d35e217a5ffeb6e4096405824dd6c/src/mito2/src/worker/handle_compaction.rs)를 기준으로 한 정책입니다. 이후 엔진 버전으로 올릴 때는 이 경계와 로컬 실증을 다시 확인하세요.

**기존 환경 이전:** `.env`에 명시된 `GREPTIME_MIN_COMPACTION_INTERVAL=36500d`는 새 기본값보다 우선하므로, 별도로 승인된 배포에서 `0m`으로 바꾸세요. 새 배포 파일과 설정 재생성, GreptimeDB 재기동이 필요합니다. 저장소 checkout에서 생성 파일을 갱신할 때는 `python3 tools/render.py`를 사용합니다. 이 문서 변경 자체는 운영 배포나 중지된 서버의 재기동을 승인하거나 수행하지 않습니다. 저장소 식별자·볼륨·런타임 데이터·비공개 배터리 교정은 그대로 보존하세요.

수동 one-shot은 정기 스케줄을 대신 켜는 방법이 아닙니다. **DB와 초기화가 준비되어 있고 정기 aggregate 스케줄러가 실행되지 않는, 별도로 승인된 유지보수 창에서만** 다음 명령을 사용하세요. 정상 `server`가 실행 중이면 정기 pass에 맡기고 별도 작업을 시작하지 마세요. `--no-deps`는 중지된 DB·초기화 서비스를 시작하지 않으며, `AGG_RUN_ONCE=1`은 한 pass 뒤 종료하므로 별도 상주 스케줄러를 만들지 않습니다.

```bash
docker compose --env-file .env --profile server \
  run --rm --no-deps -e AGG_RUN_ONCE=1 aggregate
```

`greptime-data`의 영속 디스크 캐시에는 flush·자동 compaction 출력이 저장되고 S3로 업로드됩니다. 별도 동기화기나 전체 객체 GET 검증은 추가하지 않습니다. `GREPTIME_PAGE_CACHE_SIZE`는 메모리 캐시이므로 디스크 캐시 용량을 대신하지 못합니다.

GreptimeDB 1.2.1의 [디스크 캐시 분할](https://github.com/GreptimeTeam/greptimedb/blob/179ff8e5099d35e217a5ffeb6e4096405824dd6c/src/mito2/src/cache/file_cache.rs#L292-L301)은 Parquet와 Puffin 인덱스에 별도 예산을 배정합니다. 기본 10GiB는 Parquet 약 8GiB와 인덱스 약 2GiB여서 **10GiB SST 전체를 상주시키는 설정이 아닙니다.** 동시에 처리하는 입력과 새 compaction 출력, 다른 테이블 및 잔존 캐시 파일을 고려해 작업 집합에 맞춰 조정하세요. WAL·로그·임시 파일 공간도 별도로 필요하므로 캐시 상한은 전체 로컬 디스크 상한이 아닙니다. 기본 page cache 512MB·write buffer 128MB·디스크 쓰기 캐시 10GiB와 기존 명시적 override를 유지하며, 작은 로컬 실험의 캐시 크기를 운영 권장값으로 적용하지 않습니다.

작업 캐시에 없는 차가운 이력, 과거 입력의 재전송·수정, 집계·컴팩션 및 재시작·복구는 S3 다운로드를 발생시킬 수 있습니다. 캐시 크기가 보존 이력보다 작아도 운영할 수 있지만, 모든 작업의 cache hit나 egress 0·특정 절감률·청구 상한을 보장하지 않습니다.

`greptime-data` 볼륨과 `/data/cache/object/write`를 재배포·재시작 사이에 보존하세요. 캐시를 지우거나 볼륨을 교체하면 집계·compaction만으로도 다시 다운로드할 수 있습니다. 재시작 시 캐시 복구는 비동기이므로 파일 보존만으로 즉시 cache hit를 보장하지 않습니다. 저장소 종류·endpoint·버킷·prefix는 변경하지 않습니다.

적용 전에는 [조회 외 S3 다운로드 실증](development.md#조회-외-s3-다운로드-실증)의 검증 범위를 확인하고, 허용된 운영 재기동 후에는 실제 수집·집계·compaction 중 `opendal_operation_bytes_sum{operation="read",scheme="s3"}` 증가량을 관찰하세요. 캐시 부족·유실은 다시 다운로드하는 정상 복구 경로이므로 이 구성은 비용의 절대 상한이 아닙니다. 서버가 중지된 상태라면 설정 검토만으로 재기동이 허용되지는 않습니다.

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
- JavaScript·Hermes 플러그인은 호스트 또는 훅 프로세스 안에서 직접 HTTP 전송합니다. 영속 전송 큐·별도 sender·자동 재시도는 없으며 실패·오프라인·강제 종료 시 유실될 수 있습니다. 서버 Alloy 큐는 서버에 도착하지 못한 데이터를 복구하지 못합니다.
- 기존 설치본의 큐와 이미 실행 중인 구 sender는 자동 이관·삭제·종료하지 않습니다. Codex·Claude의 복사된 설치본은 같은 설정·홈으로 재설치하고 장기 실행 호스트는 정상 재시작·재로딩하세요. 관측 checkpoint나 훅 종료를 collector ACK·DB 저장 증거로 취급하지 마세요.

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

원본은 MF4, ingress sidecar, manifest를 함께 저장합니다. 객체 경로는 `raw/vehicle/<id>/can/YYYY/MM/DD/<manifest-sha256>/<원래 파일명>`이며, 로컬 manifest의 hash pin을 검사하고 각 PUT의 명시적 `ChecksumSHA256` 응답과 HEAD 크기·checksum·`sha256` metadata가 모두 일치한 뒤에만 로컬 spool을 제거합니다. 기존 객체 재사용도 서버 checksum을 요구합니다. 부재·불일치·미지원·읽기 실패는 덮어쓰지 않고 로컬 파일을 유지하며 검증용 전체 GET fallback은 없습니다. metadata만으로 원격 바이트를 검증하지 않으며 IDrive/B2 호환성은 주장하지 않습니다. 디코딩된 신호는 SQLite outbox를 거쳐 DB로 전달합니다.

DBC·VSS·펌웨어 pin 변경 시 recorder와 decoder를 먼저 정지하고 `DECODE_EPOCH`를 갱신한 뒤 재기동하세요. 실행 중인 decoder와 recorder가 서로 다른 정의를 사용하게 만드는 hot swap은 지원하지 않습니다.

오프라인 재해석은 `REDECODE_*`와 해당 DBC·mapping 설정을 준비한 후 실행합니다. 입력은 봉인된 로컬 MF4 또는 URL과 검증 정보이며, live outbox와 다른 outbox를 사용합니다. **오프라인은 CAN을 사용하지 않는다는 뜻이며, DB 전송까지 비활성화한다는 뜻은 아닙니다.**

```bash
COMPOSE_PROFILES=redecode docker compose --env-file .env run --rm redecode
```

### 별도 CAN uploader의 서버 receiver

`can-receiver`는 opt-in 서버 프로필입니다. 원본 수신(`can-receiver`, `serve`)과 디코딩·적재(`can-worker`, `work`)를 별도 서비스·프로세스로 실행합니다. 이 저장소가 receiver·worker·배포를 관리하며 차량 측 capture/uploader는 `tesla-obd`에서 관리합니다. 기존 `vehicle`의 SocketCAN/MF4 경로와 혼용하지 않습니다. 공개 Git에는 DBC, companion evidence JSON, 실제 원본·샘플·차량 식별자·자격 증명을 넣지 않습니다.

서버의 비공개 `.env`에 `COMPOSE_PROFILES=server,can-receiver`, `CAN_OTLP_USER`, `CAN_OTLP_PASSWORD`, `CAN_VEHICLE`, `CAN_COLLECTOR_ID`를 설정합니다. 기존 uploader의 인증·identity를 그대로 유지하세요. `GREPTIME_HTTP_URL` 기본값은 컨테이너 네트워크의 `http://greptimedb:4000`이며 `GREPTIME_DB/USER/PASSWORD`는 **서버에만** 둡니다. receiver는 CAN 수신 인증·identity만 사용하고 Greptime 자격 증명·DBC 없이 기동하며, worker는 Greptime 연결·DBC 정의만 사용하고 수신 포트·CAN 인증 없이 기동합니다. 차량에는 수신 주소와 CAN 인증만 전달합니다. 빈 인증·identity는 inactive profile의 구조 검사를 허용하지만 실제 receiver 기동은 거부됩니다.

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

볼륨 루트는 UID/GID `10001:10001`, `0700`, 정의 파일은 같은 소유자의 `0600`입니다. receiver는 UID/GID `10001:10001`로 실행하며 루트 파일시스템, 정의·코드·venv 볼륨은 read-only입니다. raw SQLite `/data/raw.sqlite3`는 비공개 처리 캐시이며 기본 상한은 256 MiB입니다. 내용 정렬·재직렬화·줄바꿈 변경도 epoch를 바꿀 수 있으므로 운영 중 정의를 덮어쓰지 마세요.

```bash
docker compose --env-file .env --profile can-receiver config --quiet
docker compose --env-file .env --profile can-receiver up -d can-receiver can-worker
docker compose --env-file .env --profile can-receiver exec can-receiver \
  /opt/venv/bin/python -m scripts.ingest.can.can_receiver status --database /data/raw.sqlite3
```

초기 `can-receiver-deps`는 inline 코드를 `receiver-code` 볼륨에 준비한 뒤 pinned 의존성을 설치합니다. 읽기 전용 receiver 루트에 inline config를 직접 복사하지 않습니다. PyPI 네트워크는 첫 설치와 의존성 변경 시 필요하며, 코드 변경 시 deps·receiver·worker **세 서비스의 revision label**을 함께 갱신하세요. receiver와 worker는 독립적으로 재시작할 수 있습니다. 정상 ingest 중에는 어느 한쪽을 멈추기 위해 다른 쪽을 중지할 필요가 없으며, worker 공백 동안 받은 파일은 `/data/files`에, OTLP 원본은 SQLite에 보존되어 재기동 후 처리됩니다. 두 프로세스는 GIL을 공유하지 않지만 같은 SQLite archive를 공유 writer로 사용하므로 쓰기는 SQLite 잠금으로 직렬화됩니다. receiver·worker는 같은 archive에 공유 수명 lock과 각 역할 lock을 함께 사용하며, 같은 역할 중복 실행·구 combined 실행·`re-decode`와 worker 동시 실행은 시작 전에 거부됩니다. host endpoint는 기본 `127.0.0.1:4319`의 `/v1/files` 또는 `/v1/logs`입니다. 기존 SSH 터널과 CAN Basic 인증 경계를 유지하고 공개 인터페이스로 바인딩하지 마세요. Basic 인증만으로 전송이 암호화되지는 않습니다. 기존 Alloy AI privacy ingress는 raw CAN payload용 경로가 아닙니다.

#### Pi 원본 파일 직접 수신

Pi uploader는 `/v1/files/<session_id>/<filename>`에 원본 bytes를 직접 보냅니다.
허용 파일은 `capture.json`, `serial_chunks.jsonl`, `events.jsonl`이며 동일한 Basic 인증과
`X-CAN-Vehicle`·`X-CAN-Collector` 경계를 적용합니다. `GET`은 내구성 저장 offset과
마지막 최대 64 KiB의 SHA256을 반환합니다. `PUT`은 `Content-Length`,
`X-CAN-Offset`, `X-CAN-SHA256`으로 범위를 검증하며 최대 4 MiB입니다.
`capture.json`은 최대 64 KiB 전체 스냅샷이고, 다른 두 파일은 append-only입니다.
파일·저장 위치를 동기화한 뒤에만 ACK하며 ACK 유실 재전송은 같은 바이트인지 확인합니다.

원본과 메타데이터 버전은 기존 raw 볼륨의 `/data/files` 아래에 저장합니다.
수신 프로세스는 JSONL·CAN을 파싱하지 않습니다. `can-worker`의 importer가 완성된 줄을
정확한 정수 ns로 읽어 기존 `Archive.accept`에 편입하므로, 이전 OTLP로 받은 같은
세션/seq는 중복 생성하지 않습니다. 줄 중간 bytes·실패한 수집 원본도 파일로 보존하며,
시작 시각이 아직 없는 metadata는 해석 준비가 될 때까지 기다립니다.
파일 ACK와 raw DB 편입·디코딩·Greptime ACK는 서로 다른 완료 경계입니다.
기존 `/status`의 raw counter는 **편입된 청크**이며 파일 수신 bytes의 지표가 아닙니다.

전환은 서버를 먼저 배포한 뒤 Pi uploader만 교체합니다. Pi 원본·기존 SQLite spool은
보존하고 별도 `--state`에 작은 진행 정보만 저장합니다. 이전 spool의 import cursor는
서버 ACK가 아니므로 재개 위치로 쓰지 않습니다. endpoint와 사설 HTTP opt-in을 함께
`/v1/files`로 바꾸고 기존 batching 옵션을 제거해야 합니다. capture는 재시작하지 않습니다.
기존 `/v1/logs`는 이전 클라이언트·복구 재전송용 수신 계약으로 계속 지원합니다.

서버 디스크는 전체 원본 JSONL 파일과 제한된 raw DB/outbox를 함께 수용해야 합니다.
Pi의 protobuf 큐 용량을 JSONL 저장 용량으로 간주하지 마세요. `/data/files`의 수신 원본 파일은 자동 삭제하지 않습니다.

#### SQLite 원본 처리 캐시

`serve`와 `work`의 `--raw-cache-mib` 및 Compose의 `CAN_RAW_CACHE_MIB` 기본값은 **256 MiB**입니다. SQLite 본체는 모든 쓰기 연결의 `max_page_count`로 제한합니다. 수신은 디코딩·outbox용 25% 공간을 남기고, 부족하면 `503 raw_cache_full`로 거절하여 클라이언트가 재시도하게 합니다. 디스크 전체 용량과 `/data/files` 크기의 상한은 아닙니다.

worker는 signal·dirty 알림의 전체 ACK로 outbox가 비었을 때, 모든 등록 epoch가 디코딩한 원본 prefix를 호출당 최대 10,000청크씩 정리합니다. ACK 유실 재시도용 tail은 세션별 최대 10,000청크를 유지하되, 전체 raw payload가 DB 상한의 ¼을 넘으면 짧은 세션을 포함한 완료 prefix도 추가 정리합니다. 미처리 원본은 이 예산 때문에 삭제하지 않으며, 한 번에 정리할 수 없는 초과분은 다음 호출에서 계속 회수합니다. parser tail·미완료 청크·실패 outbox와 세션 순번은 유지합니다. 신호를 만들지 않는 원본도 주기적으로 정리합니다.

캐시 모드는 `auto_vacuum=FULL`로 commit 시 파일의 빈 페이지를 회수하고, `DELETE` journal로 WAL의 별도 누적을 피합니다. rollback journal은 트랜잭션 동안 임시 공간을 사용합니다. 기존 DB에 처음 적용하면 `VACUUM`으로 형식을 변환하므로 두 역할을 멈추고 충분한 임시 디스크 공간을 확보하세요. 기존 미처리 DB가 상한보다 크면 시작을 거부하며 자동 삭제하지 않습니다.

`status.raw_chunks`는 누적 수신량, `retained_raw_chunks`는 실제 보존 청크, `pruned_chunks`는 정리량, `raw_bytes`는 보존 중인 payload bytes입니다. `max_database_bytes`는 설정된 본체 상한입니다. 정리된 순번의 인증된 재전송은 새로 저장하지 않고 ACK합니다. 해당 원본은 이미 없어 bytes 충돌 비교를 할 수 없으며, 보존 중인 순번의 충돌 검증은 유지합니다.

정리된 과거의 새 epoch 재해석은 `re-decode_requires_unpruned_raw`로 거절합니다. 장기 원본 아카이브가 꼭 필요한 별도 운영만 두 역할에 `--raw-cache-mib 0`을 명시하세요. 이 경우 크기 제한·자동 정리는 꺼집니다. 기본 `re-decode`와 프로그램에서 직접 만든 `Archive`는 명시적 아카이브 작업을 위한 무제한 모드이며, 캐시에서 없어진 과거를 복구하지는 못합니다.


CAN OTLP 요청은 최대 10,000레코드이며, 요청 본문은 gzip 해제 후에도 2 MiB 이하여야 합니다. 레코드 상한과 바이트 상한을 모두 만족해야 수신합니다.

디코딩은 쓰기 잠금 밖에서 최대 1,000청크·2,000출력 행을 준비하고, 청크 사이의 경과 시간이 50ms를 넘으면 다음 commit으로 넘깁니다. 조밀한 청크도 parser 상태와 frame 경계 cursor로 이어서 처리하므로 한 청크의 모든 신호를 메모리에 펼치지 않습니다. commit 전 각 세션의 기존 cursor·상태를 다시 확인하며, parser 상태·decode cursor·outbox는 한 트랜잭션에서 함께 commit하거나 rollback합니다. 개별 frame·commit 시간을 포함한 엄격한 50ms 상한은 아니며 `synchronous=FULL`은 유지합니다.

단일 ordered 디코더와 단일 업로드 워커는 같은 영속 outbox를 독립적으로 처리합니다. Greptime 응답이 지연되어도 디코딩은 계속됩니다. 업로드는 최대 20,000행과 URL-encoded HTTP body 4 MiB를 모두 만족하는 가장 긴 순서 보존 prefix를 전송하며, 배치를 채우려고 기다리지 않습니다. `--outbox-limit`은 1–20,000행, `--max-body-bytes`는 body byte 예산입니다. 한 행만으로 byte 예산을 초과하면 `greptime_row_too_large`로 남기고 해당 행을 건너뛰거나 삭제하지 않습니다. DB 요청의 `--greptime-timeout` 기본값은 60초이며, `serve`의 ingress socket `--http-timeout` 기본값 15초와 별개입니다. `--greptime-timeout`·`--outbox-limit`·`--max-body-bytes`·outbox watermark·재시도 간격은 `work` 옵션입니다. 전체 행 수가 ACK된 배치만 outbox에서 제거하고, 오류·부분 ACK·`greptime_timeout`은 배치 전체를 보존합니다. 정수 nanosecond는 `TIMESTAMP(9)` 대상 column에 정수 literal로 전달하여 정밀도를 유지하면서 GreptimeDB 1.2.1의 literal INSERT fast path를 사용합니다. 정상 종료는 진행 중인 업로드 완료 또는 DB timeout까지 archive 소유권을 유지합니다. SQLite 원본의 수신 ACK는 Greptime 저장 완료를 뜻하지 않습니다.

signal 행의 전체 ACK 뒤 같은 시간창의 `vehicle_signal_dirty` 갱신까지 ACK되어야 outbox에서 제거합니다. 알림 실패는 `dirty_notify_failure`로 남고 signal 재전송은 같은 event ID로 중복 제거됩니다. 집계기는 dirty 알림이 있는 완료된 CAN 시간창만 처리하며 같은 generation의 checkpoint가 있으면 코드·설정 revision이 달라져도 재계산하지 않습니다. 처리 중 generation이 바뀌거나 분석이 실패하면 checkpoint를 완료하지 않습니다. 알림 없는 과거 데이터와 뒤늦은 무알림 입력은 자동 발견하지 않으며 원본 데이터는 삭제하지 않습니다. 이 확인은 해당 scope·시간창의 처리 기록이지 물리 교정 성공 증거가 아닙니다.

`status`는 영속 누적 counter와 indexed cursor를 사용하며 원본 전체를 매번 세지 않습니다. `last_receive_ns`, `last_decode_ns`, `last_full_ack_ns`는 각각 raw 수신·해석·signal 및 dirty 알림 전체 ACK 시각입니다. storage-metrics는 raw volume을 read-only로 읽어 raw/WAL/outbox/파일시스템 여유와 제한된 오류 종류를 노출합니다. 누락·읽기 실패는 unknown이고 빈 정상 큐만 0입니다. 수신·디코드 commit 전 기본 64 MiB 여유에 staged write 예산을 더해 확인하며, 부족하면 `archive_disk_reserve`로 거부하고 기존 raw/outbox를 보존합니다. 이미 ACK된 outbox를 지우는 작업은 공간을 회수할 수 있도록 이 reserve 때문에 막지 않습니다.
기본 `status`는 트리거 유지 `decode_epoch_state` 한 테이블과 영속 counter(`raw_chunks`, `sessions_total`, `outbox_rows`)만 읽습니다(O(보존 epoch 수), 세션/raw/decode 전체 스캔 없음). `active_epoch`는 `register_epoch`가 선택한 명시적 epoch이며 최신 rowid 추정이 아닙니다. `remaining_chunks`는 영속 전체 raw에서 epoch 처리량을 뺀 파생값이므로 raw 수신은 배치당 한 번의 counter 증가만 수행합니다. 각 epoch는 현재 처리 backlog(`active_backlog_chunks`)·이전 epoch 잔여(`retained_backlog_chunks`)를 구분하며, `decoded_rows_exact`는 완료 행에 미완료 dense 청크의 이미 방출된 `partial_rows`를 더한 값입니다(처리 청크 수는 그대로). 세션별 신호 합계는 명시적 상세 조회(`status --detail`)에서만 집계합니다. exporter 라벨은 고정된 `active`/`retained`, 단계, 오류 종류만 사용하며 epoch·세션·차량 식별자를 노출하지 않습니다.

#### CAN archive 백업과 새 볼륨 복원

DB 오프라인 백업과 별개입니다. `can-backup`은 실행 중인 receiver·worker의 SQLite read transaction을 고정해 backup API로 복사하고 정의 바이트·epoch·cursor·outbox를 함께 검증합니다. live receiver·worker나 DB를 중지할 필요가 없습니다. `CAN_STORAGE_TYPE=File`은 로컬 보관만 하며, off-host 보관에는 `CAN_STORAGE_TYPE=S3`와 기존 S3 설정, live prefix와 겹치지 않는 `CAN_S3_BACKUP_PREFIX`를 지정합니다. tar·SHA256 sidecar·manifest·COMPLETE는 명시적 `ChecksumSHA256` PUT 응답과 HEAD checksum·크기로 검증합니다. 서버 checksum 부재·불일치·미지원은 로컬 archive를 보존하고 실패하며 metadata-only 성공이나 검증용 전체 GET fallback은 없습니다. 원격 자동 GC는 하지 않으며 복원 다운로드와 로컬 SHA256 검증은 유지합니다.

이 DB/definitions 백업 형식에는 `/data/files`의 직접 수신 원본·메타데이터 버전은 포함되지
않습니다. 파일 백업까지 필요하면 수신·worker를 정상 종료한 상태의 raw 볼륨 전체 복사나
일관된 파일시스템 snapshot을 함께 보관하세요. DB 백업 성공만으로 미편입 원본까지
off-host에 보관했다고 판단하면 안 됩니다. Pi 원본을 지우는 근거로도 사용하지 않습니다.

```bash
docker compose --env-file .env --profile backup run --rm can-backup backup
# 새 복원 대상 두 볼륨에 writer가 없음을 확인한 뒤 실행합니다.
BACKUP_OFFLINE_CONFIRMED=1 \
  docker compose --env-file .env --profile backup run --rm can-backup restore
```

복원 대상은 `CAN_RESTORE_RAW_VOLUME`과 `CAN_RESTORE_DEFS_VOLUME`의 **별도 빈 볼륨**입니다. 기본 이름은 `${COMPOSE_PROJECT_NAME}_can-restore-raw`와 `${COMPOSE_PROJECT_NAME}_can-restore-defs`이며 각각 receiver의 `/data`, `/definitions`에 직접 연결할 수 있습니다. `CAN_BACKUP_FILE`로 완료 백업을 선택할 수 있습니다. CAN 복원의 offline 확인은 이 두 **대상**에 대한 확인이며 live 원본의 WAL·소유권 lock은 그대로 유지할 수 있습니다. 원본과 같은 대상, 기존 파일이 있는 대상, 사용 중인 대상에는 복원하지 않습니다. 원본의 UID/GID와 `0700` 디렉터리·`0600` 파일 모드를 그대로 복원하며 접근을 위해 권한을 넓히지 않습니다.

복원본의 정의 SHA·연속 seq·epoch·cursor·outbox를 확인한 뒤 별도 receiver·worker로 검증하세요. 운영 archive를 복원본으로 교체(인계)할 때는 receiver와 worker **둘 다** 먼저 정상 종료하고, 같은 archive에 구·신 writer를 병렬 실행하지 마세요. `CAN_BACKUP_RESERVE_BYTES`와 `BACKUP_RESERVE_BYTES`는 각각 CAN·DB staging 예산에 추가할 여유 byte이며 기본 64 MiB입니다. 백업은 snapshot과 압축 작업 공간, 복원은 archive와 해제될 파일 및 대상 파일시스템별 공간을 사전 검사합니다. 공간 부족을 발견하면 기존 원본·복원 대상 데이터를 변경하지 않습니다.

`python -m tests.test_can_receiver_compose`는 별도 Docker 프로젝트와 합성 정의로 실제 읽기 전용 receiver·worker를 시작합니다. OTLP 영속 ACK·재시작·중복 보존과 파일 수신의 worker 중지·부분 파일 재시작·중복 범위 재전송·정확한 ns의 Greptime 적재를 검사한 뒤 소유 볼륨만 제거합니다. 운영 원본과 자격 증명을 사용하지 않습니다.

#### 기존 receiver·worker 인계

1. 기존 receiver·worker의 raw volume 이름, identity·인증, 정의 두 파일의 SHA256, 상태·epoch를 비공개로 기록하고 SQLite-consistent 백업을 확보합니다. **온라인 백업은 SQLite backup API 등 일관성 있는 방식으로 수행하세요. WAL이 열린 `.db`만 `cp`하면 안 됩니다.** 오프라인 전체 볼륨 백업도 모든 writer(receiver·worker)를 먼저 중지해야 합니다.
2. **기존 receiver와 worker 둘 다** 정상 종료합니다. capture/uploader, DB, Grafana 등 다른 서비스를 중지하거나 재시작하지 않습니다. 파일 uploader는 원본·진행 정보를, 이전 OTLP uploader는 기존 spool을 보존한 채 서버 공백 동안 재시도합니다.
3. `CAN_RECEIVER_RAW_VOLUME`을 기존 raw volume의 정확한 이름으로 지정합니다. 새 빈 raw volume으로 교체하거나 DB를 복사·삭제하지 않습니다. 기존 `/data/raw.sqlite3`와 WAL/SHM, `/data/files`, decode cursor·outbox·epoch를 그대로 사용하며 소유자 `10001:10001`와 private 모드를 확인합니다. 필요한 권한 조정은 writer를 멈춘 상태에서만 수행합니다.
4. 동일한 정의 바이트를 private definitions volume에 공급합니다. 서버 이전을 이유로 새 epoch를 만들거나 `re-decode`를 실행하지 않습니다. 같은 raw·정의 볼륨으로 새 receiver와 worker를 시작합니다. **같은 raw archive에 구·신 receiver·worker를 병렬 실행하지 마세요.**
5. `status`에서 기존 epoch·cursor가 이어지는지 확인하고, uploader endpoint·인증과 실제 ACK를 확인합니다. 인계 자체는 원본을 삭제하지 않으며, 이후 SQLite 정리는 위 캐시 정책을 따릅니다. rollback도 새 receiver·worker를 종료한 뒤 동일 볼륨·정의로 기존 버전을 단독 기동합니다.

의도적으로 정의를 변경하는 재해석은 worker를 먼저 중지하고 새 정의를 검토한 뒤 아래처럼 같은 venv·볼륨의 단독 컨테이너에서 실행합니다. `re-decode`는 worker 역할 lock을 사용하므로 `can-worker` 실행 중에는 동시에 시작하지 마세요. 이 작업은 새 decode epoch를 명시적으로 등록하므로 단순 배포 인계와 구분합니다. 정의 파일 교체는 worker를 중지한 상태에서만 수행합니다. receiver는 계속 수신할 수 있으며 worker 시작 전에 받은 원본도 이후 처리됩니다.

```bash
docker compose --env-file .env --profile can-receiver stop can-worker
docker compose --env-file .env --profile can-receiver run --rm --no-deps can-worker \
  /opt/venv/bin/python -m scripts.ingest.can.can_receiver re-decode \
  --database /data/raw.sqlite3 --dbc /definitions/observed.dbc \
  --definitions /definitions/observed.json
docker compose --env-file .env --profile can-receiver up -d can-worker
```

완전한 OTLP ACK는 **서버 raw SQLite의 durable 수락**이지 Greptime commit이 아닙니다. downstream 장애 시 raw와 outbox가 남으며, ACK는 차량 원본 삭제 허가가 아닙니다. SQLite는 위 처리 캐시 정책에 따라 완료 원본만 정리합니다. epoch·정수 nanosecond·quality를 유지하며 `reported_unverified` 등을 집계 편의를 위해 `valid`로 바꾸지 않습니다.

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

24개 대시보드를 `Datalake` 아래에 자동으로 공급합니다.

| 폴더 | 대시보드 |
| --- | --- |
| Datalake | 전체 현황 |
| AI | AI 사용량, AI 도구, AI 세션, AI 응답 성능 |
| 홈 | 홈 |
| 차량 | 차량 개요, 차량 공조/냉각 보고값, 차량 차체/실내 보고값 |
| 차량 / 주행 | 주행, CAN 주행 신호 |
| 차량 / 충전 | 충전, CAN 충전 입력·누적계 |
| 차량 / 배터리 | 배터리 모니터, CAN 배터리 신호, CAN 셀 전압 |
| 차량 / CAN 신호 | CAN 신호 탐색, 수집 진단 · CAN / VSS, 수집 진단 · DBC |
| 운영 | Datalake 상태, CAN 수집 운영, Greptime 엔진 운영, 수집기 운영, 저장소 운영 |

`grafana-folders` 초기화 작업은 Grafana가 준비된 뒤 내부 폴더 API로 계층을 맞춥니다. Grafana 12.4.0의 파일 공급 설정에는 상위 폴더 지정이 없어 별도 작업이 필요합니다. 기존 `Datalake` 폴더 UID와 대시보드 UID는 유지하며, 경로 이동 중 기존 대시보드를 삭제하지 않아 즐겨찾기를 보존합니다. 반복 실행해도 같은 폴더를 재사용합니다. `GF_ADMIN_USER`·`GF_ADMIN_PASSWORD`는 Grafana와 같은 값을 사용하며 인증 실패나 API 오류는 초기화 실패로 남습니다.

폴더와 대시보드 UID는 서로 겹치지 않아야 합니다. Grafana 12.4.0의 목록 화면은 두 종류를 UID로 식별하므로 홈 폴더와 홈 대시보드가 모두 `datalake-home`이면 선택 상태 계산이 재귀 호출되어 `Maximum call stack size exceeded`가 발생합니다. 홈 폴더 UID는 `datalake-home`, 홈 대시보드 UID는 `datalake-home-overview`로 구분합니다. 이 변경을 기존 설치에 공급하면 이전 UID의 홈 대시보드를 새 UID로 대체하므로 직접 대시보드 링크와 즐겨찾기는 다시 지정해야 합니다. 폴더 링크는 그대로 유지됩니다.

UID를 바꿀 때 provisioning 파일도 `home/home.json`에서 `home/home-overview.json`으로 이동합니다. 같은 경로에서 UID만 바꾸면 Grafana 12.4.0은 이전 대시보드를 남깁니다. 별도 Grafana 12.4.0의 reload 검사에서는 파일 경로 이동으로 새 UID 하나만 남았지만, 기존 설치에는 [이전 provisioning 메타데이터가 남는 문제](https://github.com/grafana/grafana/issues/109229)가 있어 경로 이동만으로 정리를 보장하지 않습니다.

옛 `datalake-home` 대시보드가 남으면 먼저 실제 provisioning 파일이 더 이상 그 UID를 공급하지 않고 새 `datalake-home-overview`가 정상인지 확인합니다. 그 후 관리자 권한으로 `/apis/dashboard.grafana.app/v1beta1/namespaces/default/dashboards/datalake-home`의 리소스를 읽어 `resourceVersion`과 `spec`을 유지한 채 `metadata.annotations`의 `grafana.app/managedBy`, `managerId`, `sourcePath`, `sourceChecksum`, `sourceTimestamp`(각각 `grafana.app/` 접두사)를 제거해 PUT하고, 같은 **대시보드** 리소스를 DELETE합니다. 아직 공급되는 UID나 같은 이름의 폴더는 삭제하지 않습니다. SQL 저장소 직접 수정 없이 이 절차로 옛 UID 제거와 새 홈 화면의 API 복구를 확인했습니다.

- `기록 없음`은 정상이나 0을 의미하지 않습니다. 수집 미설정·누락·조회 범위를 먼저 확인하세요.
- AI 비용은 도구가 보고한 값과, 비용이 없을 때 적용 가능한 공개 단가의 보충 추정치입니다. 청구서가 아니며 가격을 결정할 수 없는 사용량은 미산정으로 남습니다. 보충 단가는 LiteLLM 공개 가격표를 조회·캐시합니다.
- native telemetry와 플러그인의 사용량은 식별 가능한 범위에서 native 우선 정책으로 처리합니다. 서로 다른 수집 범위를 섞으면 일부 사용량이 누락될 수 있습니다.
- 분류별 막대·범례·툴팁에는 도구명·클라이언트명·모델명 또는 차량/수집원/해석 버전/신호를 표시합니다. `호출 수`·`세션 수` 같은 지표명으로 모든 분류를 덮지 않습니다. Grafana의 표 형식 막대는 `${__data.fields.<분류 열>}`, 시계열은 `${__field.labels.<분류 열>}`을 사용하며, 입력·출력 토큰처럼 측정값이 여러 개면 모델명과 지표명을 함께 표시합니다. Compose 원본에서는 `$`를 `$$`로 이스케이프합니다. 합계 카드와 표의 지표명은 유지하고 실제 범례·툴팁을 확인하세요.
- 전체 현황의 원본 트레이스 조사는 선택 범위의 마지막 48시간에 제한됩니다. 관측된 스팬 수는 사용자 HTTP 요청 수가 아니며, 허용 목록 밖의 HTTP 경로·SQL 본문·스팬 이벤트를 복원할 수 없습니다.
- 기본 `OTEL_TTL`은 빈 값으로 **raw OTel 무기한 보존**입니다. 30~90일이 자동 적용되는 것이 아닙니다. 보존 기간을 줄이면 기존 데이터도 만료될 수 있습니다. 정책·점검·적용 절차는 [OTel 보존](otel-retention.md)을 따릅니다.
- AI 집계는 장기 보존합니다. 홈 원본은 설정된 대상에 기본 `HOME_RAW_TTL=90d`를 사용하며 `0s`로 만료를 끌 수 있습니다. 원본 CAN의 S3 보존 정책은 별도로 관리하세요.
- AI 요약 INSERT는 최대 500행씩 묶어 전송합니다. 세션의 새 시작점 저장이 모두 성공한 뒤에만 같은 `(client, session_id)`의 이전 시작점 행을 삭제합니다. 배치 실패 시 이전 행은 남으며 재실행이 성공하면 다시 정리합니다. 집계 주기·조회 한도·보존 정책은 바꾸지 않습니다.
- 배터리 모니터의 `Fleet 분석 입력 원본` 섹션은 분석기의 기본 입력 21개를 최신 원본·단위·품질·관측 시각·DB 수신 시각 표로 보여줍니다. 기존 잔량·브릭 전압·온도·상세 충전 상태 패널에 더해 팩 전류/전압, 충전 전력, 남은/완충 에너지, AC/배터리 측 충전·방전 누적계, 절연 원시값, 최고/최저 온도 모듈 ID, BMS/충전 상태 추이를 표시합니다. 단위 없는 값은 원시 숫자이지 A·V·Ω가 아니며 누적계를 합산하거나 효율을 계산하지 않습니다. 최신 무효 보고를 이전 정상 값으로 대체하지 않고, 숫자 추이는 아래 원본/1분/1시간 전환을 따릅니다. Fleet 입력이 없으면 CAN으로 대신 채우지 않습니다. 사용자 설정으로 추가한 분석 필드는 이 기본 목록에 자동 추가되지 않습니다.

### 차량 그래프의 장기간 조회

차량 개요·주행·충전·배터리·CAN·공조·차체의 숫자 추이 쿼리는 선택 기간에 따라 원본과 기존 `vehicle_agg`를 전환합니다.

| 선택 기간 | 조회 데이터 |
| --- | --- |
| 2일 이하 | 기존 원시 신호 |
| 2일 초과, 30일 이하 | 1분 집계 |
| 30일 초과 | 1시간 집계 |

- 평균은 유효 숫자 표본 수로 가중하고, 최소·최대 그래프는 집계의 극값을 사용합니다. 차량·수집원·해석 버전·원본 필드·품질·단위를 구분하며, 무효나 NULL만 있는 구간을 0으로 채우지 않습니다.
- 출력 간격은 집계 단위의 배수입니다. 선택 범위에 온전히 들어오는 집계 버킷만 포함하므로 가장자리의 일부 구간과 아직 집계되지 않은 구간은 비어 있을 수 있습니다. 이를 채우려고 긴 원시 이력을 다시 읽지 않습니다. 세부 관측은 2일 이하로 좁혀 확인하세요.
- 최신값, 상태 전환, 동일 관측 시각의 값끼리 계산하는 차이, 원시 탐색·진단 표는 이 전환 대상이 아닙니다. 화면의 모든 쿼리가 집계로 바뀌는 것은 아닙니다.
- `db-schema`는 기존 `vehicle_agg`에 `source_field`·`quality`·`unit_key` 키와 `rollup_version`을 추가합니다. `unit` 열은 유지합니다. 새 집계는 버전 2이며, 기존 요약에는 품질 구분이 없어 장기간 그래프가 그대로 재사용하지 않습니다. 스키마·집계기·대시보드를 함께 반영해야 합니다.
- CAN 구간은 새 dirty generation이 있는 시간창만 집계합니다. 코드 변경만으로 기존 CAN 시간창을 다시 집계하지 않습니다. 비-CAN의 기존 시간별 요약은 한 주기당 최대 24개 scope/hour를 원본에서 재집계합니다. 두 해상도 저장이 끝날 때까지 기존 요약을 남겨 중단 후 재시도할 수 있습니다. 업그레이드 직후에는 재집계가 끝나지 않은 과거 그래프가 비어 있을 수 있습니다.
- 원본이 없어진 기존 요약의 숫자는 보존하지만 품질을 복원할 수 없어 새 그래프에는 사용하지 않습니다. 원본 부재를 확인한 비-CAN 구간은 `rollup_version=-1`로 표시하여 반복 재시도를 피합니다. 이후 오래된 원본을 복구했다면 일반 lookback 밖의 해당 구간은 별도 재집계가 필요합니다.
- 집계 갱신은 트랜잭션이 아닙니다. 재계산 중 또는 쓰기 실패 후 다음 성공까지 일부 버킷이 비어 있을 수 있으며, 이를 수집 부재나 관측된 0으로 해석하지 마세요.

### 차량 개요에서 CAN 기록 확인하기

- 차량 목록은 `vehicle_identity`에 명시적으로 등록된 별칭만 같은 차량으로 묶습니다. 원본 식별자는 바꾸지 않으며 등록되지 않은 차량은 자동으로 합치지 않습니다. 차량·수집 경로·해석 버전은 필터와 결과에 유지하고, 서로 다른 수집원·해석 버전의 숫자를 합쳐 현재 값으로 표시하지 않습니다.
- **조회 범위 · 관측과 DB 수신** 표는 전체 보존 기간의 마지막 `event_time`과 `ingest_time`을 구분합니다. 기간 내 관측 행이 0이어도 수신 행이 있으면 과거 기록이 들어오는 경우일 수 있습니다. 행 수는 저장 행 수이며 중복 제거된 관측 수나 수집 성공률이 아닙니다.
- 마지막 관측 시각을 누르면 현재 차량·수집 경로·해석 버전을 유지한 채 그 시각 전후 1시간으로 이동합니다. 그래프·기간 합계는 선택 기간을 유지합니다. 잔량·남은 에너지·충전 상태·전압·온도 등의 마지막 보고값 카드는 선택 기간의 끝 시각 이전 기록에서 최신값을 고르므로, 기간 시작보다 오래된 보고도 관측 시각과 함께 확인할 수 있습니다. 최신 무효 보고를 과거 정상값으로 대체하거나 선택한 끝 시각 이후의 보고를 표시하지 않습니다.
- 데이터가 보이지 않으면 먼저 이 표의 **기간 내 관측 행**과 **기간 내 수신 행**을 비교하세요. 관측은 0인데 수신이 늘면 과거 자료의 적재일 수 있으므로 마지막 관측 링크로 이동합니다. 둘 다 0이면 차량·수집원·해석 버전과 실제 수집 경로를 확인하세요. 숫자를 표시하기 위해 조회 기간을 자동으로 넓히거나 품질 조건을 해제하지 않습니다.
- 차량을 선택하면 숨김 `vehicle_ids` 변수가 `vehicle_identity`의 명시적 매핑과 선택한 원본 ID를 한 번 조회합니다. 패널은 그 목록을 SQL 값으로 사용하므로 모든 원본 행에서 별칭 하위 쿼리를 반복하지 않습니다. 차량 선택·시간 범위 변경 시 목록을 다시 조회하며 등록되지 않은 ID도 그대로 조회됩니다. 매핑을 추가한 뒤에는 대시보드를 다시 로드해 차량 목록과 원본 ID 목록을 갱신하세요. 조회 범위 표는 관측/수신 시각과 기간 내 행 수를 한 번의 집계로 계산하고, 차량·수집원·해석 버전별 결과는 계속 분리합니다.
- CAN의 정확한 경로·신호명이 대응하는 잔량, 남은 에너지, 기어, 충전 상태와 외기온은 `reported_unverified`인 **미검증 보고값**으로 표시합니다. 최신 무효 값·단위 불일치·알 수 없는 enum은 과거 정상 값으로 대체하지 않으며, CAN 보고값 표에는 원본 단위·품질·관측 및 수신 시각을 남깁니다. 잔량 그래프는 `%` 단위의 허용된 보고값만 평균에 포함하고 유효 값이 없는 구간은 비웁니다.
- `CP_CHARGE_ENABLED`는 충전 허용 보고이지 실제 충전 중이라는 판정이 아닙니다. CAN 원시 전압·전류가 있어도 교정과 분석 결과가 없는 전력은 판단 불가로 남고, 대응하는 원본 신호가 없는 온도도 만들어내지 않습니다.
- 전기 분석의 `current_fields`·`voltage_fields`는 명시된 순서에서 물리적으로 사용 가능한 표본이 있는 한 필드만 선택합니다. 미교정 첫 필드가 단위·품질이 유효한 다음 명시 필드를 가리지 않으며, 필드끼리 표본을 합치거나 목록 밖의 대체 필드를 자동으로 사용하지 않습니다. 유효 표본이 없으면 기존 판단 불가를 유지합니다. 용량·전류 부호·SOC 기준점·온도 교정과 건강도 모델이 없는 값은 추정해서 채우지 않습니다.
- 전력·팩 전압 카드의 미분석 입력 검사는 원본 field 이름 override를 놓치지 않도록 같은 `vehicle/source/decode_epoch`의 전체 raw frontier를 사용합니다. 분석과 무관한 새 신호도 이전 숫자를 가릴 수 있는 보수적 제한이며, 서로 다른 scope의 입력은 섞지 않습니다.

- gauge·stat 카드는 표시 이름 기준으로 측정값만 선택합니다(`보고값`, `값`, `실내`·`외기`). 비어 있는 셀 번호나 다른 메타데이터를 별도의 판단 불가 수치로 렌더링하지 않습니다. 셀 전압·배터리 온도의 근거 표는 카드와 같은 시간·유효 조건으로 원본 관측 시각과 교정 버전을 보여줍니다. 측정값 자체가 없거나 무효이면 기존 `기록 없음`·`판단 불가` 라벨을 유지합니다.

### CAN 원본 신호 대시보드

- **CAN 주행 신호**: 속도·가속 페달·기어·실제 및 명령 토크·축 회전수·출력 상한과 원본 축 출력 보고값, 조향각·조향각속도·주행거리계 보고값입니다. 조향각(deg)·조향각속도(deg/s) 부호는 보고 방향 그대로 두며, 주행거리계(km)는 누적값이므로 구간 주행거리 차감·효율을 계산하지 않습니다.
- **CAN 배터리 신호**: 차량 표시 및 BMS 잔량·팩 전압·팩 전류·남은 에너지·전체 팩 에너지·버퍼·최고 및 최저 온도·브릭 전압 보고값입니다. 전압과 전류를 곱해 미검증 전력을 만들지 않습니다.
- **CAN 셀 전압**: `BMS_brick0`부터 `BMS_brick95`까지 브릭 전압 96개의 추이와 개별 최신 보고값을 봅니다. 원본 번호 0–95를 유지하며 개별 셀 내부 상태를 확인한다는 뜻은 아닙니다. 서로 다른 관측 시각의 값을 동시 전체 배열이나 건강 판정으로 취급하지 않습니다.
- **CAN 신호 탐색**: 저장된 모든 경로를 검색하고 숫자 그래프·최신 보고·품질별 관측 목록을 봅니다. 차체·공조·미러 같은 상태 신호와 `unknown_enum`도 목록에서 숨기지 않습니다.
- CAN 원본 화면은 `source='can'`으로 고정해 Fleet 입력이 섞이지 않게 합니다. 차량 별칭과 해석 버전 필터를 유지하며 최신 카드도 차량·해석 버전별로 분리합니다. 최신 무효 값·단위 불일치는 과거 정상 값으로 대체하지 않습니다. 공조·차체 화면의 Fleet 보고는 별도 수집 범위로 표시합니다.
- 조회 범위 표는 전체 보존 기간의 마지막 관측과 수신을 구분합니다. 마지막 관측 링크는 해당 화면의 관측 시각으로 이동하며, 차량·해석 버전 필터는 유지합니다. 기본 시간 범위는 화면별로 다르며 과거 값을 현재 값처럼 표시하지 않습니다. 수신이 최근이라도 관측이 오래되면 재생 적재이며 신규 캡처가 아닙니다.
- **차량 공조/냉각 보고값**: 팬 요구와 실측, 설정 온도와 실측 온도, 증발기·외기·냉각수 관측, 요구 와트를 봅니다. 요구 와트는 측정 전력이 아니며, 요청값과 실제 보고값을 구분합니다. Fleet 실내·외기·HVAC 보고는 출처별로 분리합니다. 쾌적·에너지 효율 판정으로 바꾸지 않습니다.
- **차량 차체/실내 보고값**: 래치·미러 접힘·열선·호출·위치, 시트·컬럼 위치·호출, Fleet 운전석 보고를 봅니다. 미러 위치 전압·시트 각도·거리·틱은 원본 단위 그대로 보며 형상 변환·탑승 인원·문 열림·안전 상태로 해석하지 않습니다.
- **CAN 충전 입력·누적계**: 라인 전류와 보고된 전류 제한, 입력 전력, EVSE·고전압 상태, 종료 설정, 팩·모듈·충전기 누적계를 봅니다. 충전 계열 범위 표는 선택한 충전 패밀리만 보며 전체 신호 커버리지가 아닙니다. 겹치는 누적계는 분리 표시하며 합산·충전 효율·리셋 기준 증감을 계산하지 않습니다. 충전 허용 보고를 실제 충전 판정으로 바꾸지 않습니다.
- **AI 응답 성능**: 첫 청크 지연·실행 시간·서브에이전트 역할과 상태·명시적 결과 범위를 봅니다. 캐시 읽기·쓰기·추론 토큰은 관측된 구성이며, 보고되지 않은 오류·사용량은 성공으로 해석하지 않습니다. 표본이 없으면 기록 없음이며 0이 아닙니다. 원시 스팬 조회는 선택 기간의 최근 48시간을 넘지 않으며, 더 오래된 기록은 조회 기간을 바꾸거나 장기 집계 화면에서 확인합니다. 비용은 보고값 우선·공개 단가 보충의 부분 합계이며 미산정 호출이 있으면 일부분입니다. 청구서가 아닙니다.
  새 설치와 기존 테이블 모두 `db-schema`가 nullable `span_attributes.ttft_ms` 열을 준비합니다. TTFT가 아직 보고되지 않았더라도 조회 오류 대신 기록 없음으로 표시하며, 없는 값을 0으로 채우지 않습니다.
- **CAN 수집 운영**: 수신·해석·ACK 이력과 대기열·오류를 봅니다. 아카이브 mtime 경과는 파일 기록 시각, 수신기 지표 최신 관측 경과는 지표 표본 자체의 신선도, 수신/디코드/전체ACK 경과는 단계별 마지막 시각이며 서로 다르므로 합산하지 않습니다. 어느 쪽도 원본 CAN 발생 시각의 최신성이 아닙니다. 과거 계열이 현재에 없으면 오래된 성공으로 표시하지 않으며, 누락 계열은 unknown입니다. 더티 윈도우 listed/matched/pending은 목록에 오른 항목의 세대 적용 상태이며 전체 히스토리 수렴·백필 완료가 아닙니다. 백로그 감소나 재해석 완료를 주장하지 않습니다.
- **Greptime 엔진 운영**: 처리·버퍼·캐시·쿼리 단계 지연·서버 오류를 봅니다. 쿼리·단계 지연은 물리 초 단위가 확인되지 않아 원시 단위로 보며, 카운터 이름만으로 물리 지연 단위를 단정하지 않습니다. 비공개 진단은 개수·종류 구성만 봅니다.
  지연 쿼리 진단 테이블은 최초 기록 전에는 없을 수 있습니다. 생성 여부를 먼저 확인하고, 미생성 테이블은 오류나 0건으로 바꾸지 않고 결과 없음으로 표시합니다.
- **수집기 운영**: 스크랩 수집량·소요, 원격쓰기 대기·재시도·전송 시차, WAL·구성 요소 상태를 봅니다. 기존 OTel 큐·스팬 단계 차트와 중복하지 않고 연결합니다. 없는 계열은 unknown이며 0을 합성하지 않습니다.
- **저장소 운영**: 로컬 여유·WAL 증가와 결합된 객체 인벤토리·입출력 지연·오류를 봅니다. 결합 인벤토리는 실제 SST·백업·과거 객체를 함께 세므로 백업 전용 용량·성공 이력·복원 검증으로 해석하지 않습니다. 복원 검증은 known 플래그만 보며 검증 시각 지표가 없으므로 복원 성공 시점을 주장하지 않습니다.
- 비용 보고의 `datalake_storage_cost_report_retained_*`는 보관된 실행 보고의 부분 합계 **gauge**입니다. 임시 프로세스의 요청 수·이 보고 gauge에 `rate`/`increase`를 적용하지 않습니다. `report_complete=0`은 항상 전체 월/계정 범위 미증명이며, 누락·오래된 보고·불명 바이트는 0 비용이 아닙니다. native OpenDAL과 상주 SDK의 누적 counter는 출처별로 따로 보며 합산하지 않습니다.
- 비용 창의 숨김 변수 `cost_day_seconds`·`cost_month_seconds`는 UTC DB 시계의 실제 자정·월초에서 계산하고 새로고침 때 갱신합니다(URL·임의 값 재지정 불가). 최근 30일은 별도 rolling 창입니다. 이력 검증은 `INFRA_SCRAPE_INTERVAL=30s`에서만 표본 수·시작/끝 경계·계열별 간격을 확인하며 다른 custom cadence는 unknown입니다. 당월 31일 연장과 초과 USD는 확인된 범위의 조건부 시나리오이며 계정 전체 청구가 아닙니다.
- 비용 카드의 **Greptime S3 native**는 OpenDAL `operation="read",scheme="s3"`로 한정합니다. 읽기·rate·최신성·시작/끝 경계·표본 수 모두 같은 S3 계열만 사용하므로 local cache/fs 읽기는 비용·무료 allowance 전망에 포함하지 않습니다. 이는 S3 계정 전체나 SDK/백업 보고를 합친 값이 아닙니다.
- `greptime-storage-budget`의 native 비교는 각 checkout의 실제 preflight/cache 설정과 같은 합성 입력을 사용합니다. 격리 DB의 flush·실제 compaction 완료 뒤, 그 DB만 정지하고 두 객체 cache를 지운 다음 같은 컨테이너·볼륨을 재시작해 20개 cold 조회와 두 번의 warm 조회를 비교합니다. GreptimeDB 1.2.1은 S3 write cache를 강제로 켜므로 설정값만으로 cold라고 주장하지 않습니다. cold 조회 창의 성공한 SST GET 바이트가 양수이고 독립 oracle 및 cold/warm 결과가 일치해야 합니다. primary SST·WAL·인증·MinIO·meter는 cache 정리 대상이 아니며 운영 cache를 정리하는 절차가 아닙니다.
### 관측·분석·입력 전제 읽기

- 원본 관측과 파생 분석을 구분합니다. 분석 화면의 최신 개정·상태·단위 표가 비어 있으면 조회 범위에 분석 기록이 없다는 뜻이며, 입력 부재나 추정 전제 부족을 단정하지 않습니다. 대체값으로 채우지 말고 집계 실행과 선택 범위를 확인하세요. 주행·충전 요약이 비어 있어도 주행거리·효율·충전 세션을 추정하지 않습니다. 홈 입력이 비어 있으면 집계 대상 지정부터 확인합니다.
- 차량·수집 경로·해석 버전 범위를 결과에 유지합니다. 차량 목록은 명시적 별칭만 묶으며, Fleet과 CAN 보고는 출처별로 함께 보되 합산하지 않습니다. 최신 무효 보고를 과거 정상값으로 대체하지 않습니다.
- 표시 잔량·남은 에너지·최고/최저 셀 전압·배터리 최고/최저 온도·실내외 온도 카드는 관측·수신·전송 묶음 순서의 `LAST_VALUE(... ORDER BY ...)`로 마지막 보고를 고릅니다. 최신 NULL도 유지하며 유효 값만 먼저 걸러 과거 값을 되살리지 않습니다. 표시 잔량 카드는 수집 범위를 다시 읽는 조인 없이 한 번 집계하고, 배터리 최고·최저 온도는 각각 `pack_max`·`pack_min`을 명시적으로 표시합니다. 온도 교정의 후속 입력 울타리는 같은 스냅샷의 최고·최저 관측 시각을 재사용하므로 동일 원시 필드를 두 번 더 읽지 않습니다.
- `source_field`는 일반 컬럼이므로 원시 신호가 많으면 필드 선택에도 전체 이력을 읽을 수 있습니다. 스키마 초기화는 해당 컬럼에 `INVERTED INDEX`를 선언하며, 기존 SST는 운영 변경 허가 후 `ADMIN BUILD_INDEX('vehicle_signal')`로 별도 구축해야 합니다. 구축 후에도 조회 개선은 보장되지 않습니다. GreptimeDB 1.2.1의 기본 `last_row` 병합은 여러 입력 소스가 겹치는 범위에서 일반 컬럼 조건을 병합 전에 적용하지 않습니다([네이티브 구현](https://github.com/GreptimeTeam/greptimedb/blob/v1.2.1/src/mito2/src/read/scan_region.rs)). 최신 덮어쓰기·NULL을 보존하기 위한 동작이므로 속도를 위해 `append_mode`나 `last_non_null`로 바꾸지 마세요. 구축은 저장 이력을 읽는 I/O 작업이므로 초기화마다 자동 실행하지 않으며, 적용 후 전체 수집원 온도 카드의 실제 조회 시간과 최신 무효 값 유지 여부를 다시 확인하세요. 태그인 `path`·`source`로 범위를 줄일 때도 비표준 경로·다른 수집원의 유효 입력과 무효화 울타리를 빠뜨리지 않아야 합니다.
- 배터리 최고·최저 온도 카드(51·52)는 Fleet의 `Vehicle.Powertrain.TractionBattery.ModuleTempMax`·`ModuleTempMin` 표준 경로로 제한합니다. `path`·`source` 태그 조건으로 무관한 CAN 이력 읽기를 줄이되, 지원 경로 안의 최신 NULL·무효 보고와 후속 입력 울타리는 유지합니다. 교정 결과도 Fleet scope만 표시하며 비표준 경로·CAN 입력은 이 카드에 포함하지 않습니다. CAN 온도 보고는 **CAN 배터리 신호** 화면에서 확인하세요. 다른 매핑을 지원하려면 해당 태그 경로와 교정 범위를 함께 확장해야 합니다.
- 수집 경로 선택기의 `CASE`는 알 수 없는 태그 문자열을 `CONCAT(source, '')`로 반환합니다. GreptimeDB 1.2.1에서는 태그를 그대로 반환하는 분기가 `Utf8`·`Dictionary(UInt32, Utf8)` 스트림 스키마 불일치를 일으킬 수 있습니다. 오류를 숨기거나 수집원을 제외하지 않고 문자열 결과 타입을 맞춥니다.
- 배터리 분석 결과는 기존 `vehicle_analysis` 스키마 그대로 조회합니다(새 테이블 없음). 계산 불가 지표도 첫 관측부터 `value=NULL`, `status='unavailable'`, 사유 `reason` + 기존 revision·scope·교정/모델/커버리지 메타데이터 행으로 저장되므로 사유가 조회 가능하며, 건강 값은 저장하지 않습니다. 저장 트레이드오프: 생성된 모든 unavailable identity가 보관되므로 희소 구간·scope의 NULL 행만큼 쓰기량이 늡니다. 최신 뷰는 `(vehicle, source, decode_epoch, analysis_id, metric, window_start)`별 `computed_at DESC, revision DESC`이며, 최신 실패가 과거 건강값을 되살리지 않고 복구는 같은 논리 identity를 갱신합니다. 집계 로그의 `unavailable_stored`는 저장된 NULL 사유 행 수입니다.
- `기록 없음`은 성공한 패널 조회 결과가 0행이라는 뜻이며 0이 아닙니다. NULL 값 + `unavailable` 상태 + 사유 텍스트가 있는 행은 이유가 명시된 계산 불가이므로 빈 셀을 0·정상으로 읽지 말고 사유 열을 확인하세요. 분석 표가 통째로 비어 있어도 입력 부재·분석 대기·교정 부재 중 어느 하나를 단정할 수 없습니다. 조회 실패는 Grafana 오류로 표시하며 `기록 없음`으로 바꾸지 않습니다.
- 현재 전력·전압(110·111)은 선택 끝까지 완료되어야 하는 최신 시간 구간과 입력 울타리를 만족할 때만 수치를 표시합니다. 최신 완료 구간이 오래되거나 그 뒤 원시 입력이 있으면 NULL이며, 선택 끝 이후의 미래 원시는 판정에서 제외합니다. 마지막 완료 구간(112)은 신선도와 무관한 이력으로 관측·구간·계산 시각을 함께 표시합니다. 최신 실패를 과거 유효값으로 대체하지 않습니다.

- 근거 표(113)는 분석 identity별 관측·구간·계산 시각을 구분합니다. 원본 잔량·에너지·실내외 온도는 선택 끝 이내의 최종 보고를 보며, 선택 범위 이전의 오래된 보고도 관측 시각과 함께 표시합니다. CAN 별칭은 미검증 보고값이지 교정된 건강 지표가 아닙니다.
- 이유 상태(114)는 저장된 `status`와 `reason`에서만 도출합니다. 교정 부재·최신 무효·관측 부족·모델 부재를 구분하며, 시각이 맞지 않는 `unmatched`·`unsynchronized`를 보고 충돌로 단정하지 않습니다. 계산 불가 수치는 항상 NULL입니다.
- 완료 구간과 후속 입력(115)은 선택 범위에 실제 원시 관측이 있을 때만 비교합니다. 분석 기록이 없으면 `분석 기록 없음 · 확인 필요`, 최신 완료 구간 뒤에 선택 끝 이내의 원시 관측이 있으면 `재분석 필요`입니다. 이미 최신 완료 구간으로 덮인 관측을 오래된 구간과 비교해 대기로 표시하지 않으며, 기록 부재만으로 실행 대기나 설정 부족을 단정하지 않습니다.
- 관측·수신 표(116)는 `event_time`과 `ingest_time`을 구분합니다. DB 수신 시각이 최근이라는 이유로 오래된 관측을 신선한 값으로 읽지 않습니다. NULL·기록 없음·조회 오류는 서로 다른 상태이며 건강 0으로 바꾸지 않습니다.
- 현재 전력·전압의 후속 입력 조회는 수치를 표시할 수 있는 최신 완료 분석의 scope로 제한합니다. 관측 시각 이후의 입력뿐 아니라 계산 후 늦게 수신된 이전 관측도 포함하므로 재분석 울타리는 유지합니다. 표시 조건을 이미 만족하지 못하는 분석 때문에 무관한 수집원의 원시 이력을 전부 읽지 않습니다.
- 펼친 행의 카드는 대시보드 최상위 `panels`에, 접힌 행의 카드는 해당 행의 `panels`에 둡니다. 펼친 행 안에 중첩하면 Grafana가 카드를 찾지 못하므로 SQL 성공만으로 화면 노출을 검증하지 말고 해당 카드의 `viewPanel` 화면도 확인하세요.


### 운영 health 시각을 구분하기

- Fleet의 `fleet_last_receive_timestamp_seconds{topic=...}`는 대상 차량의 유효 envelope를 로컬에서 받은 시각이며, 중복 재수신도 갱신합니다. VSS의 `vss_last_receive_timestamp_seconds`는 broker update/snapshot의 실제 로컬 수신 시각입니다. 받은 적이 없거나 recorder를 재시작한 뒤 아직 수신하지 않았으면 metric이 없습니다. `datalake_trace_last_received_timestamp_seconds{client=...}`는 trace-privacy가 비어 있지 않은 trace batch를 redaction 후 받은 시각이며 DB 저장 ACK가 아닙니다. 이 시각도 프로세스 재시작 후 첫 수신 전에는 없습니다. AI logs/metrics와 Home raw event 시각은 수신 시각이 아니므로 해당 **마지막 실제 수신은 unknown**입니다.
- `fleet_outbox[_events]_oldest_enqueue_age_seconds`와 `vss_outbox_oldest_enqueue_age_seconds`는 로컬 수신 시 저장한 `ingest_time` 기준 체류 초입니다. `*_oldest_event_age_seconds`는 원본 `event_time`의 age로, 늦은 재전송은 이벤트가 오래되어도 큐 체류는 짧을 수 있습니다. 빈 큐만 0이고, 읽기 실패는 `*_outbox_metrics_success=0` 및 age 없음입니다(VSS 기존 pending/time metric은 실패 시 -1). 통신·조회 실패를 정상 0으로 해석하지 마세요.
- aggregate는 기존 `backup-data` 볼륨의 `/ops/aggregate-status.json`을 pass/section 시작과 pass 완료에 atomic 교체합니다. storage-metrics는 같은 볼륨을 read-only로 읽습니다. `datalake_aggregate_running`, `datalake_aggregate_success`(마지막 완료 pass 결과), `last_start_timestamp_seconds`, `last_success_timestamp_seconds`, `last_failure_timestamp_seconds`를 노출하며 실패해도 이전 성공 시각은 보존합니다. `time() - datalake_aggregate_last_success_timestamp_seconds`는 **실행 성공 freshness**이지 데이터 처리 지연이 아닙니다. section이 오래 실행되거나 프로세스가 죽으면 `status_timestamp_seconds`가 오래된 채 남을 수 있으므로 interval/running과 함께 확인합니다. SQL pass 성공은 배터리 분석의 교정·품질 정상 판정과 다릅니다.
- 기본 `server`는 aggregate 스케줄러를 포함합니다. 보관된 status만으로 현재 프로세스가 동작한다고 판단하지 말고 컨테이너 상태와 status 시각·실행 간격을 함께 확인하세요. 오래된 성공이나 `running` 값은 현재 pass의 정상 동작을 증명하지 않습니다.
- pass 시작 간격은 monotonic clock으로 유지하며 느린 pass와 다음 pass를 겹치지 않습니다. status의 `section_durations_seconds`, `section_counts`는 section 처리 시간과 행 수를 나타냅니다. 상태 관측만을 위한 전체 raw `MAX(event_time)` 조회와 데이터 frontier/공백 지표는 제거했습니다.
- CAN scope 목록은 pass당 한 번 작은 dirty/checkpoint 메타데이터 테이블의 DISTINCT 합집합에서만 구합니다. 자동 과거 발견·bootstrap·reconciliation·raw checklist·fingerprint 재검사와 sweep 테이블의 런타임 읽기·쓰기는 제거했습니다. 기존 테이블·기록과 원본은 삭제하지 않습니다. 수집 알림 없는 과거 CAN 데이터와 뒤늦은 무알림 입력은 자동으로 찾지 않습니다.
- 일반 vehicle/trip/battery pass는 registry 등록 여부와 무관하게 `source='can'`을 SQL에서 제외합니다. CAN dirty 시간창은 분·시간 rollup과 주행·충전 요약만 갱신하며 자동 배터리 분석은 실행하지 않습니다. 비-CAN 배터리 분석은 `server`의 정기 aggregate pass에 포함되며 명시적 standalone 분석도 유지합니다.
- 이 변경은 egress 0이나 과금 안전을 보장하지 않습니다. 새 dirty generation의 집계와 다른 section의 조회, DB compaction은 여전히 raw/SST를 읽을 수 있습니다.
- AI/log/home section은 `vehicle_discovery`와 독립적으로 실행합니다. discovery 실패 시 이에 의존하는 차량 section은 fail-closed로 실행하지 않으며, 빈 결과의 성공으로 바꾸지 않고 전체 pass를 실패로 기록합니다. 실패한 pass도 같은 monotonic 시작 간격으로 다음 주기에 재시도하며 crash-loop하지 않습니다. `AGG_RUN_ONCE=1`은 실패 시 nonzero로 종료합니다. 잘못된 시작 설정(예: 필수 인증 누락, DB 미준비)은 여전히 시작 실패입니다.
- 차량 checkpoint는 scope·시간별 rollup과 주행·충전 요약이 성공하고 dirty generation 재확인이 일치한 뒤에만 저장합니다. 배터리 분석은 checkpoint의 완료 조건이 아닙니다. 뒤의 작업이 실패해도 앞서 성공한 checkpoint는 남습니다. 주행 raw 조회는 1시간씩 나누되 episode와 열린 주행의 시작 anchor 문맥은 유지합니다.
- SQL 진단은 느린 조회(1초 이상)와 실패에 대해 허용된 operation·table·fingerprint·실제 UTC 조회 경계·경과 시간만 기록합니다. raw SQL, 인증 정보, 차량·scope 식별자는 로그에 남기지 않습니다.

- 비-CAN 정기 분석과 명시적 standalone 배터리 조회는 같은 구간·scope의 이벤트를 먼저 읽고 분석에 필요한 `source_field`만 가져옵니다. 제외 조건은 SQL의 `LIMIT` 전에 적용하며, 정확한 나노초 경계의 최대 1시간 청크로 조회해 기존 24시간 문맥과 누적 `BATTERY_MAX_ROWS` 한도를 유지합니다. scope는 고유한 전체 태그 조합으로 구분합니다. 경고 이벤트가 없는 오프라인 구간에는 경고 문맥용 전체 신호 조회를 생략하지만, 필드와 무관한 scope 탐색은 유지해 기존 결과의 무효화를 빠뜨리지 않습니다. 실제 경고가 있거나 온라인 가용성 검사가 필요하면 기존 문맥을 보존합니다. 누적 한도 초과나 조회 실패 시 부분 결과를 저장하지 않습니다.
- 선택된 신호가 없어도 조회에서 확인한 scope를 경고 분석에 전달하므로, 이전 경고 값은 입력 부재에 맞춰 무효화됩니다. `battery.conditions.diagnostics`의 수치는 분석에 전달된 신호 집합을 설명하며 전체 CAN 원본 행 수나 수집 완전성 지표가 아닙니다.
- status/restore 파일이 없거나 파싱 불가하면 `datalake_aggregate_status_known=0` / `datalake_restore_verification_known=0`이며 시각·성공을 만들어내지 않습니다. `datalake_restore_verification_timestamp_seconds`는 마지막 성공한 **오프라인 archive SHA·SST identity 검증 및 staged 복원 완료** 시각입니다(legacy SHA256 SST는 hash 검증). 이후 DB 재기동·SQL 확인 성공을 뜻하지 않습니다. 오래된 성공, 새 실패, 진행 중, 미관측을 각각 구분하세요.

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
- S3 모드는 SST를 `<S3_BACKUP_PREFIX>/_sst/` 공유 pool에 증분 저장합니다. 첫 실행에는 기존 SST를 모두 서버 측 COPY하고, 이후에는 bucket·live root/key·크기·원본 ETag가 같은 SST를 재사용해 신규 identity만 복사합니다. 각 세대의 manifest/COMPLETE는 전체 복원 목록을 담으므로 이전 세대 archive를 순차 적용하지 않습니다. 기존 full SST 백업도 복원할 수 있습니다. WAL·설정 tar와 DB offline 조건은 그대로입니다.
- S3 SST는 소스 identity로 주소화한 공유 pool에 HEAD → 조건부 서버 측 COPY → HEAD 검증으로 보관합니다. 비교 가능한 서버 checksum은 대조하지만 primary SST의 checksum 부재는 조건부 identity COPY를 허용하며 내용 digest 증명은 아닙니다. 새 per-SST `.json` 없이 검증된 COMPLETE inventory를 guard당 한 번 읽어 재사용하고, 정규 pool 키로 orphan·GC 재시도를 발견합니다. 원본/복사본 전체 GET은 하지 않습니다. archive·sidecar·manifest·완료 표식 PUT은 응답과 HEAD의 명시적 `ChecksumSHA256` 일치를 요구하며 미지원은 로컬을 보존하고 실패합니다(GET fallback 없음). live prefix와 `S3_BACKUP_PREFIX`는 겹치면 안 됩니다.
- backup/retention은 `_locks/` claim으로 직렬화하고 복원은 세대 `refs/`로 참조를 보호합니다. 승인된 retention만 남은 세대가 참조하지 않는 pool 객체를 정리합니다. 강제 종료 시 남은 claim/ref는 자동 만료시키지 않으며, 소유 작업의 종료를 확인하기 전에는 제거하지 마세요. 공급자의 강한 LIST/GET 일관성이 전제입니다.
- inventory JSON(`manifest.json`의 archive 내/원격 사본과 `COMPLETE`)은 **각각 비압축 UTF-8 64 MiB 이하**여야 합니다. 백업 생성과 로컬·원격 복원에 같은 한도를 적용하며 초과한 백업은 성공으로 보고하지 않습니다. 8,000개 이상의 SST도 이 바이트 한도 안이면 복원할 수 있습니다. checksum sidecar는 1 MiB 이하입니다. JSON 읽기는 한도와 chunk 크기로 제한하고 원격 응답 스트림은 성공·실패 모두 닫습니다. **archive 전체나 SST 데이터 총량에는 이 JSON 한도를 적용하지 않습니다.**
- `BACKUP_KEEP=7`은 로컬 세대 수입니다. 원격 보존은 기본 `off`이고 별도 승인된 `enforce`만 세대 retirement와 미참조 pool GC를 수행합니다. 강한 LIST 일관성에 의존하는 고유 claim 직렬화에 TTL 탈취는 없으며, unknown/unreadable inventory와 검증 불가 legacy 참조는 삭제를 차단합니다. pin·reader 참조는 보존합니다. [원격 보존 정책](backup-retention.md)을 확인하고 복구 prefix에 임의 만료 lifecycle을 설정하지 마세요.
- `storage-cost-budget`은 합성 backup+RAW 비용 모델의 CI 게이트이며 운영 청구·배포 승인이 아닙니다. 정책(`config/cost-budget.json`)은 1200GiB, 월 4회, RAW 200GiB/월을 가정합니다. 가격은 decimal GB(1GB=10^9 bytes)로 계산하고 저장료는 `max($6/월, GB-month×$0.006)`, egress는 고정 1200GiB 기준 3배 allowance 뒤 `$0.01/GB`를 가정합니다. query·restore·other egress=0은 관측된 0이 아닌 모델 입력이며 CAN은 별도 게이트입니다. 계정의 다른 사용량과 실제 요금제·무료 allowance 범위를 확인해야 합니다([가격 출처](https://www.idrive.com/s3-storage-e2/faq-pricing#egress-policy), 정책 확인일 2026-10-14).

### 새 프로젝트로 복원

기존 프로젝트는 정지한 채 두고 **미사용 프로젝트 이름**과 빈 대상 볼륨을 사용하세요. S3 모드에서는 같은 스토리지 설정으로 원격 백업을 가져올 수 있습니다. `BACKUP_FILE`은 백업 ID/이름으로 지정할 수 있으며, 생략하면 최신 완료 백업을 선택합니다.

```bash
COMPOSE_PROJECT_NAME=datalake-recovery BACKUP_OFFLINE_CONFIRMED=1 \
  docker compose --env-file .env --profile backup run --rm backup restore
# 복원 성공을 확인한 뒤 동일한 프로젝트 이름으로 시작
COMPOSE_PROJECT_NAME=datalake-recovery docker compose --env-file .env up -d
```

복원은 비어 있지 않은 대상 볼륨을 거부하며 강제 덮어쓰기 옵션은 없습니다. 같은 S3 live prefix를 쓰는 기존 DB와 복원 DB를 동시에 실행하지 마세요. 복원 후 현재 `.env`로 인증 설정이 다시 생성됩니다.

성공한 복원은 백업 볼륨의 `BACKUP_DIR/restore-verification.json`에 `{timestamp_seconds, backup_id}`를 atomic 교체로 기록합니다. 이는 archive SHA256·크기, SST HEAD identity·크기(기존 SHA256 형식의 SST 백업은 종전 hash 검증)와 대상 파일 복원이 끝났다는 **마지막 오프라인 성공 기록**이며, DB 재기동·SQL 조회·데이터 수집이 정상이라는 증거가 아닙니다. 실패하면 이전 성공 기록을 유지하고 파일이 없으면 검증 이력은 unknown입니다. 최신 복원 시도가 성공했다는 뜻으로 해석하지 말고 종료 코드도 확인하세요.

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

## CAN decoded-outbox backpressure (issue #11)

- decoded outbox 행 수와 canonical UTF-8 바이트에 high/low watermark를 둡니다. 기본값은 행 20000/15000, 바이트 32/24 MiB이며 `CAN_OUTBOX_HIGH_ROWS/LOW_ROWS/HIGH_BYTES/LOW_BYTES`(`work` 플래그 `--outbox-high-rows` 등)로 조정합니다. high에서 **decode만 pause**하고 raw 수신은 기존 reserve/인증/원자적 commit 정책 그대로 계속 받습니다.
- 경계 진동 방지: pause 중에는 행·바이트가 모두 low 이하로 내려가야 재개합니다. 이미 staged된 batch는 최대 한 `DECODE_ROW_BUDGET` batch만큼 high를 초과할 수 있습니다(문서화된 최대 overshoot). ACK 삭제·checkpoint 여유는 disk reserve가 그대로 담당하며, 이 기능은 raw 전체 저장량을 무한히 안전하게 만들지 않습니다.
- commit→wake 힌트(`accept→decode`, `decode→upload`, full-ACK 삭제→decode)는 idle sleep만 건너뛰는 최적화입니다. 분리 실행에서는 `accept→decode` 힌트가 프로세스를 넘지 않으므로(수신 프로세스 내 threading.Event), worker는 주기적 영속 상태 재조회로 새 raw를 발견합니다. `decode→upload`와 full-ACK→decode 힌트는 worker 내부에서 그대로 동작합니다. SQLite 상태가 최종 근거이므로 notify 유실·restart·race에서도 주기적 테이블 재조회로 진행합니다. 실패한 downstream은 자체 retry 간격으로만 재시도하고 신규 raw 알림마다 busy retry하지 않습니다.
- 바이트 카운터(`archive_meta.outbox_bytes`)는 decode-insert와 delete(flush full-ACK) 각각과 같은 transaction에서 유지되며, rollback 시 함께 취소됩니다. 기존 archive는 기동 시 outbox 테이블에서 1회 backfill합니다(`Archive` init의 `_migrate_outbox_bytes`). 핫패스 사용량(`outbox_usage()`)은 archive_meta 카운터를 읽고, 정합 검증용 전체 스캔은 `outbox_table_usage()`를 사용합니다.
- 측정 harness(부모 실행): `python -m unittest discover -s tests -p test_can_backpressure.py`, 저유량 accept→decode→full ACK p50/p95/p99와 장애 중 DB/WAL/outbox 증가·회복 drain rate·raw 수신 지연 비교는 `tools/benchmark_can_pipeline.py` harness로 수행합니다.
