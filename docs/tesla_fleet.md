# Tesla Fleet Telemetry Pipeline

## 1. 아키텍처 및 ZMQ 프레이밍 규격

```
[Tesla Vehicle] -> [Tesla Fleet Telemetry / tesla-helper]
                            | (ZMQ PUB tcp://0.0.0.0:5555)
                            |  Frame 0: allowlisted topic
                            |    (tesla_V, tesla_alerts, tesla_errors, tesla_connectivity)
                            |  Frame 1: protojson payload
                            v  사설망: tesla-helper_default
               [tesla-fleet-recorder] (profile: fleet, single-file stdlib)
                - receive-only 2-frame ZMQ SUB (5555), per-topic strict check + dispatch
                - strict protojson oneof unwrapping
                - official unit scaling (mph->km/h, miles->km, bar->kPa)
                - 9-digit nanosecond createdAt preservation
                - deterministic event_id (isResend excluded, source_field included)
                - per-message quality/envelope provenance, operator CONFIG_VERSION only
                - non-destructive bounded SQLite outbox, total pending bound (MAX_OUTBOX_ROWS)
                            |
                            v (HTTP SQL POST /v1/sql)
                     [GreptimeDB]
                tables: vehicle_signal + vehicle_event (source='fleet')
```

- **엄격한 2프레임 프로토콜**:
  - `Frame 0`: 구독 allowlist(`FLEET_ZMQ_TOPICS`, 기본 full4) 중 하나와 정확히 일치해야 합니다.
  - `Frame 1`: 디코딩된 protojson 페이로드
  - 1프레임 또는 flat 임의 형식의 페이로드는 엄격히 거부(fail-closed)됩니다.
  - 실제 수신은 서버 `records` 라우팅이 zmq로 보내는 토픽에 한정됩니다 (서버 설정은 tesla-helper 소유).
- **안전 원칙**: 테슬라 API 호출 및 차량 명령 송신은 일절 배제되며 수신 전용(receive-only)으로 동작합니다. 정기 `vehicle_data` 폴링은 없습니다.
- **네트워크 격리**: Fleet ZMQ 5555는 `tesla-helper_default` Docker 사설망 내부에만 위치하며, datalake 스택은 같은 Compose 호스트의 프로젝트 디렉터리에서 GreptimeDB(`http://greptimedb:4000` 또는 loopback 바인드 포트)로 전달합니다.

---

## 2. 데이터 스키마, 단위 변환 및 Provenance

`vehicle_signal`(`tesla_V` 토픽)과 `vehicle_event`(`tesla_alerts`/`tesla_errors`/`tesla_connectivity` 토픽)에 `source='fleet'`으로 저장되며, CAN 데이터와 혼합되지 않고 완전히 격리됩니다. ZMQ는 비영속 전송이므로 outbox는 수신 이후의 기록만 보호합니다.

| 컬럼 (`vehicle_signal`) | 출처/설명 |
|------|-----------|
| `event_time` | 원본 `createdAt` 타임스탬프 (9자리 나노초 정밀도 보존, 수집시각과 다름) |
| `vehicle` | 가명화된 차량 ID (`TARGET_VIN` 매칭 시 `VEHICLE_ID` 사용, 미지정 시 필수 salt 기반 `v-<hash>`) |
| `path` | VSS 표준 경로 (예: `Vehicle.Speed`, `Vehicle.Powertrain.TractionBattery.StateOfCharge.Current`) |
| `source` | 고정 문자열 `'fleet'` (CAN 출처는 `'can'`) |
| `event_id` | 결정론적 SHA-256 해시 (`isResend` 제외, `source_field` 포함하여 재전송 시 동일 ID 보장) |
| `decode_epoch` | `fleet-v1` (환경변수 `DECODE_EPOCH`) |
| `value_num` / `value_text` / `value_bool` | 단위 변환 및 물리적 범위 검증된 값. 디코딩 불가/invalid/non-finite는 NULL tombstone으로 보존(0/false로 채우지 않음) |
| `unit` | 물리 단위 (`km/h`, `%`, `km`, `celsius`, `kW`, `A`, `V`, `kPa` 등) |
| `vss_version` | VSS 매핑 버전 (미지정 시 NULL) |
| `vehicle_firmware` | 차량 펌웨어 버전 (미지정 시 NULL) |
| `dbc_primary_commit` / `dbc_supplemental_commit` | CAN DBC 핀 커밋 (해당 시) |
| `dbc_override_version` / `dbc_override_commit` | DBC 오버라이드 버전/커밋 (해당 시) |
| `mapping_revision` | 매핑 리비전 (`MAPPING_REVISION`, 기본 `fleet-v1`) |
| `collector_version` | 수집기 코드 버전 (`COLLECTOR_VERSION`) |
| `ingest_time` | datalake 최초 수집 시각 (나노초, `event_time`과 다름) |
| `source_system` | `'tesla_fleet_telemetry'` |
| `source_field` | 원래 Tesla 필드명 (예: `VehicleSpeed`, `Soc`, `Odometer`, `InsideTemp`, `Gear` 등) |
| `collector_id` | 배포 수집기 식별자 (`COLLECTOR_ID`, 기본값 `fleet-collector-1`) |
| `source_is_resend` | 원본 메시지의 `isResend` 불리언 플래그 (신호에만 존재, 이벤트에는 없음) |
| `quality` | `'invalid'`(디코딩 불가/invalid/non-finite) 또는 `'range_rejected'`(물리 범위 초과), 정상은 NULL |
| `envelope_id` | `sha256hex(topic + 0x1F + raw payload)` 메시지 봉투 해시 |
| `config_version` | 운영자 지정 `CONFIG_VERSION` (미지정 시 NULL, 추정 금지) |
| `connectivity` | 연결 상태 문자열 (해당 시, 아니면 NULL) |

`vehicle_event` 행은 OEM/연결 에피소드 관찰을 불변으로 보존합니다 (정확한 이름 + 차량 가명 + 유효 `startedAt`이 실용적 에피소드 키이며 Tesla UID 보장은 아님):
- `tesla_alerts`/`tesla_errors` → `event_type` `'alerts'`/`'errors'`. 시작 시각이 없거나 파싱 불가해도 경고를 유지하고 `quality='unknown_start'`로 표시하며 duration/recurrence에서 제외합니다.
- `tesla_connectivity` → `event_type` `'connectivity'`, `connectivity` 컬럼에 상태 문자열. 연결 끊김은 경고를 닫지 않습니다. 차량은 Wi-Fi와 셀룰러 소켓을 동시에 유지할 수 있으므로 `episode_id`에 연결 ID의 해시를 넣어 연결별로 구분합니다. 한 연결의 `DISCONNECTED`는 같은 `episode_id`의 연결만 닫으며, 차량 전체 오프라인은 열린 연결이 하나도 없을 때입니다. 연결 ID 원문과 통신 방식은 저장하지 않습니다.
- 종료 후보가 여럿이거나 충돌해도 관측을 유지하고 권위 있는 duration을 확정하지 않습니다. 부재/연결 끊김만으로 종료하지 않습니다.
- 관측 본문은 저장하지 않으며 `body_redacted` 여부만 기록합니다. 원시 VIN/secret/tag는 저장하지 않습니다.

`vehicle_event` 컬럼: `event_time`, `vehicle`, `event_type`, `name`, `source`, `event_id`, `ingest_time`, `envelope_id`, `started_at`, `ended_at`, `duration_s`, `audience`, `is_active`, `body_redacted`, `source_system`, `decode_epoch`, `collector_id`, `episode_id`, `quality`, `config_version`, `connectivity`.

### 공식 단위 변환 정책:
- **`VehicleSpeed`**: Tesla 원본은 `mph`이므로 `* 1.609344`로 변환하여 VSS 표준인 `km/h`로 저장합니다. (물리적 한계: 0~350 km/h)
- **`Odometer` & `EstRange`**: Tesla 원본은 `miles`이므로 `* 1.609344`로 변환하여 VSS 표준인 `km`로 저장합니다.
- **`InsideTemp` & `OutsideTemp`**: Tesla 원본이 섭씨(`celsius`)이므로 그대로 보존합니다.
- **`Soc`**: 백분율(`%`)이므로 그대로 보존합니다.
- **`Gear`**: `shiftStateValue`의 `ShiftState` 접두사를 제거하여 `"P"`, `"D"`, `"R"`, `"N"`으로 정규화합니다.
- **`Experimental_1` 등 `invalid` oneof kind**: 드롭하지 않고 NULL tombstone(`quality='invalid'`)으로 보존됩니다.
- **범위 초과 값**: 드롭하지 않고 NULL tombstone(`quality='range_rejected'`)으로 보존됩니다.

---

## 3. 무결성 및 내결함성 (Fail-Closed & Bounded Outbox)

1. **타임스탬프 9자리 나노초 정밀 보존**:
   - `createdAt`의 fraction이 9자리 나노초 그대로 정밀 보존됩니다.
   - 타임존 오프셋(Z 또는 +/-HH:MM)이 없거나 불리언 타입인 경우 즉시 거부(fail-closed)됩니다.
2. **희소 데이터(Sparse) 보존**:
   - 페이로드의 `data` 배열에 수신되지 않은 필드는 절대로 `0`이나 `false`로 기본값을 채우지 않으며, 수신된 필드에 한해서만 행을 생성합니다.
3. **차량 혼합 방지 (Multi-vehicle isolation)**:
   - `vin`이 없거나 빈 문자열인 레코드는 거부됩니다.
   - `TARGET_VIN`이 설정되어 있으면 일치하지 않는 VIN의 메시지는 자동 드롭되어 다수 차량의 신호 혼합을 방지합니다.
   - `TARGET_VIN`이 없을 때는 `VEHICLE_ID_SALT`가 필수이며, unsalted fallback은 금지됩니다.
4. **재전송 중복 방지 (Deduplication)**:
   - `event_id` 계산에서 `isResend`를 제외하므로 원본과 재전송 메시지는 동일한 `event_id`를 가집니다.
   - SQLite outbox의 `seen_ids` 테이블 및 `INSERT OR IGNORE`를 통해 로컬 중복을 차단하고, GreptimeDB의 기본 키로 최종 중복을 방지합니다.
5. **디스크 고갈 방지 (비파괴 Bounded Outbox)**:
   - `MAX_OUTBOX_ROWS`(기본 50,000행, 약 10~20MB) 한도를 신호+이벤트 전체 대기 행(total pending)에 엄격히 적용합니다. 과거 디스크 부족 사례가 이 한도의 동기였으며, 현재 디스크 상태는 배포 시점에 재확인할 것.
   - **기존 미전송(unacked) 행을 삭제하지 않으며**, 큐 초과 시 신규 유입 신호를 거부하고 `fleet_dropped_signals_total` 및 `fleet_outbox_overflow_drops_total` 카운터를 증가시킵니다.
   - 거부된 신호는 `seen_ids`에 기록되지 않아, 큐가 드레인된 후 재전송 시 정상 수신될 수 있습니다.
   - `seen_ids`는 24시간 보존 주기 및 최대 50,000건의 하드 리밋으로 무한 증가가 방지됩니다.
6. **장애 복구 (DB Down & Restart)**:
   - GreptimeDB 다운 시 outbox 행을 영구 보존하며, 프로세스 재시작 시에도 큐의 데이터를 이어서 전송합니다.
   - 배치 전송 성공 및 `affectedrows == batch_size` 검증 후에만 outbox에서 트랜잭션으로 삭제합니다. 기존 대기 행은 마이그레이션으로 보존되며 삭제되지 않습니다.

## 3b. 수집(Collection) vs 진단(Diagnostics) 한계

- 수집: change+interval 게이팅된 희소 관측을 있는 그대로 보존합니다. 미수신 필드는 0/false로 채우지 않고, `invalid:true`는 0이 아니라 측정 불가 tombstone입니다. 동일한 타임스탬프라도 페이로드가 다르면 다른 샘플이며, 동일 페이로드라도 동시 물리 샘플을 보장하지 않습니다.
- 진단 한계: min/max(+ID)만으로 전체 셀 벡터나 물리 저항을 복원할 수 없고, `Soc`는 BMS 보고 usable 값이며, 에너지 카운터의 단위/기준이 문서화되지 않은 필드는 원시값 + 미검증으로 유지합니다. 분석 출력은 `reported`/`derived`/`estimated`/`unavailable`/`error` 상태로 데이터 부족과 실행 실패를 구분합니다. 실행 실패 시 해당 모듈의 기존 수치는 같은 identity의 NULL/error 리비전으로 무효화하고 과거 이력은 보존합니다.

---

## 4. 환경 변수

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `FLEET_ZMQ_ENDPOINT` | `tcp://fleet-telemetry:5555` | Tesla Fleet Telemetry ZMQ 엔드포인트 |
| `FLEET_ZMQ_TOPICS` | `tesla_V,tesla_alerts,tesla_errors,tesla_connectivity` | ZMQ 구독 allowlist (콤마 구분, 기본 full4) |
| `TARGET_VIN` | `""` | 단일 차량 수집 대상 VIN (다수 차량 혼합 방지) |
| `TESLA_HELPER_NETWORK` | `tesla-helper_default` | Docker 외부 네트워크 이름 |
| `FLEET_OUTBOX_PATH` | `/data/outbox.sqlite` | SQLite outbox 저장 경로 |
| `MAX_OUTBOX_ROWS` | `50000` | 신호+이벤트 전체 대기 행 수 상한 (total pending) |
| `FLEET_BATCH_N` | `500` | GreptimeDB 배치 INSERT 단위 (신호+이벤트 공유) |
| `FLEET_FLUSH_SEC` | `5` | GreptimeDB 플러시 주기 (초) |
| `FLEET_METRICS_PORT` | `9105` | Prometheus 메트릭 포트 |
| `COLLECTOR_ID` | `fleet-collector-1` | 수집기 인스턴스 ID |
| `VEHICLE_ID` | `""` | 고정 차량 가명 ID (`TARGET_VIN`과 함께 사용) |
| `VEHICLE_ID_SALT` | `""` | VIN 가명화 해시 솔트 (`TARGET_VIN` 미지정 시 필수) |
| `DECODE_EPOCH` | `fleet-v1` | 플릿 디코드 에포크 |
| `CONFIG_VERSION` | `""` | 수집 설정 provenance (운영자 지정, 미지정 시 NULL, 추정 금지) |
| `BATTERY_ANALYSIS_CONFIG` | `/app/battery-analysis.json` | 분석 JSON read-only 경로 (§6). 미지정 기본 파일 부재/빈 경로는 `{}`; 명시한 파일 부재는 `config_missing` 오류 |
| `BATTERY_LOOKBACK_HOURS` | `""` | 비어 있으면 `AGG_LOOKBACK_HOURS`(그마저 없으면 30)로 대체 |
| `BATTERY_MAX_ROWS` | `""` | 비어 있으면 `AGG_MAX_ROWS`(그마저 없으면 200000)로 대체 (초과 시 거부, 잘라서 실행 금지) |
| `BATTERY_BACKFILL_START` / `BATTERY_BACKFILL_END` | `""` | 명시적 재계산 구간: both-or-neither, 정수 ns 또는 UTC `YYYY-MM-DD HH:MM:SS[.fffffffff]` (빈 값 = 없음) |

---

## 5. 배포 프로필 (Opt-in)

이 서비스는 기본 `server` 프로필에 자동 기동되지 않는 `fleet` 전용 프로필로 구성되어 있습니다:
```bash
docker compose --profile server --profile fleet up -d
```

---

## 6. 배터리 분석 (setup → record → analyze → Grafana)

1. **setup**: Fleet ZMQ 토픽 allowlist(`FLEET_ZMQ_TOPICS`, 기본 full4)와 `TARGET_VIN`/`VEHICLE_ID_SALT`, `DECODE_EPOCH`(`fleet-v1`), `CONFIG_VERSION`(운영자 지정, 미지정 시 NULL)을 확정합니다. 네 토픽(`tesla_V`, `tesla_alerts`, `tesla_errors`, `tesla_connectivity`)이 전체 입력입니다.
2. **record**: `tesla-fleet-recorder`(profile `fleet`, 수신 전용)가 `tesla_V` → `vehicle_signal`, 나머지 세 토픽 → `vehicle_event`에 `source='fleet'`으로 기록합니다. ZMQ는 비영속 전송이므로 outbox는 수신 이후 기록만 보호합니다. 정기 `vehicle_data` 폴링은 없습니다.
3. **analyze**: 기존 `aggregate` 서비스(profile `server`)가 `BATTERY_ANALYSIS_CONFIG`(기본 `/app/battery-analysis.json`) 경로의 분석 JSON을 읽어 `battery_common`/`battery_reference`(stdlib 부분)/5개 분석 모듈/`battery_runtime`을 실행하고 `vehicle_analysis`에 씁니다. 컨테이너의 해당 파일 내용은 네이티브 Compose `content` 보간으로 렌더링되며, 소스는 공개 기본값 `BATTERY_ANALYSIS_CONFIG_JSON='{}'`(uncalibrated, suffix별 전부 빈 dict)입니다. 실제 운영 교정·모델 dict는 owner 전용 비공개 `.env`의 같은 변수나 비공개 Compose 오버라이드 파일로만 공급하고 커밋하지 않습니다. 경로 미지정 상태의 기본 파일 부재 또는 빈 경로는 uncalibrated `{}`이며, 환경 변수나 CLI로 명시한 파일이 없으면 `config_missing` 오류입니다. 실행 범위는 `BATTERY_LOOKBACK_HOURS`(빈 값 → `AGG_LOOKBACK_HOURS`, 그마저 없으면 30)/`BATTERY_MAX_ROWS`(빈 값 → `AGG_MAX_ROWS`, 그마저 없으면 200000)가 묶고, `BATTERY_BACKFILL_START`/`BATTERY_BACKFILL_END`(기본 빈 값 = 없음, both-or-neither, 정수 ns 또는 UTC `YYYY-MM-DD HH:MM:SS[.fffffffff]`)가 오래된 윈도우의 명시적 재계산 구간입니다.
4. **Grafana**: `vehicle_analysis`를 조회합니다. 최신 리비전은 `ROW_NUMBER() OVER (PARTITION BY vehicle, source, decode_epoch, analysis_id, metric, window_start ORDER BY computed_at DESC, revision DESC)`으로 선택한 뒤 상태를 필터링합니다. revision 해시의 사전순으로 최신 실행을 추정하지 않습니다. HTTP SQL의 `"metric"`/`"value"`와 달리 Grafana/MySQL에서는 식별자를 백틱으로 인용합니다. 화면용 시각 투영은 `CAST(... AS TIMESTAMP(6))`을 사용하되 원시 필터와 identity는 ns를 보존합니다.

### 배터리 카드 읽는 법 (상단 카드와 전문가 기록)
- 상단 카드는 마지막 기록과 핵심 지표만 먼저 보여줍니다. `마지막 차량 데이터`는 기간 필터와 무관하게 보관된 원시 신호의 가장 최근 시각이고, `마지막 분석 구간`은 선택한 기간 안에서 분석된 가장 최근 구간의 시작입니다. 값보다 각 카드의 관측 시각(`observed_at`)을 먼저 확인하십시오.
- `판단 불가`(빈 카드·NULL)는 모른다는 뜻이며 0·정상·고장 판정이 아닙니다. 최신 리비전이 `unavailable`·`error`이거나 단위·품질이 맞지 않으면 이전 정상값을 되살리지 않고 NULL로 둡니다. 원시 잔량 카드는 `%` 단위의 유효 보고만, 분석 카드는 해당 지표의 단위와 `reported`/`derived`/`estimated` 상태일 때만 숫자를 표시합니다.
- `사용한 에너지·최근 구간`과 `충방전 환산 횟수·최근 구간`은 가장 최근 분석 한 구간의 값이며 차량 평생 누적이나 기간 합계가 아닙니다. `사용 가능한 용량`·`배터리 건강도`는 계산 조건을 만족할 때의 추정치로 잔량(%)과 다르며, 조건이 부족하면 판단 불가입니다.
- 기존 상세 표·그래프는 접힌 전문가 행(분석 기록·전압/온도·에너지/용량·전기 특성·경고·근거) 아래에 그대로 있으며, 펼치면 계산 근거·버전·경고 생명주기를 볼 수 있습니다.
- 헤더에서는 차량만 직접 고릅니다. 수집 경로·해석 버전·지표·상태는 숨겨진 고급 필터로 URL(`var-source`, `var-epoch`, `var-metric`, `var-status`)로 지정합니다.

### 입력 품질과 스코프
- `invalid:true`는 측정 불가이며 0이 아닙니다. 디코딩 불가/`invalid`/non-finite는 NULL tombstone(`quality='invalid'`)으로, 물리 범위 초과는 NULL tombstone(`quality='range_rejected'`)으로 보존됩니다. 미지의 단위는 원시값 + `quality='unit_unverified'`(unit NULL)로 유지되며 물리량으로 주장하지 않습니다.
- `PackVoltage`/`PackCurrent`는 현재 원시값 + unit NULL + `unit_unverified`이며 권위 있는 A/V로 주장하지 않습니다.
- `time`/`source` 품질: `event_time`(원본 `createdAt` ns)과 `ingest_time`(수집 시각)은 다르며, 분석은 `(vehicle, source, decode_epoch)` 스코프를 절대 혼합하지 않습니다. 동일 타임스탬프라도 페이로드가 다르면 다른 샘플이며, 동일 페이로드라도 동시 물리 샘플을 보장하지 않습니다.
- 최신 극값의 invalid/동시각 충돌은 이전 정상 값으로 대체하지 않습니다. 해당 raw 값·전압 편차·조건별 기준선은 `unavailable`로 내려갑니다. `NumBrick*`는 공식 1-based 정수만 위치 귀속에 사용하고 `NumModule*`에는 문서에 없는 인덱스 시작값을 가정하지 않습니다. 절연 추세가 과거의 유효 구간을 사용하면 `coverage_ratio`와 reason의 `as_of_ns`로 그 범위를 표시합니다. 온도 편차·절연의 시간당 변화율은 같은 극값 ID 구간이 `conditions.min_slope_span_ns`(기본 10분) 이상일 때만 계산합니다. 몇십 초의 변화를 시간당으로 늘리지 않으며, 짧으면 `sparse:*min_span`으로 `unavailable`입니다.
- 스코프/버전/재처리: `analysis_id` + 결정론적 `revision`(영향 입력/설정/버전 해시) + `computed_at`이 최신 뷰를 정합니다. 모듈/설정/dispatcher 오류는 영향받은 기존 identity에 NULL/error 리비전을 기록하므로 이전 성공 수치를 최신 값으로 복구하지 않습니다. 성공적으로 재실행된 모듈은 자신의 실행 오류 표시를 `reported`, `value=NULL`, `value_text=execution_recovered`로 갱신합니다. 이는 실행 복구 기록이며 배터리 정상 판정이나 수치 0이 아닙니다.
- 희소 결과 저장: 입력 없음·교정/모델 부족으로 처음부터 계산할 수 없는 지표는 `vehicle_analysis`에 `unavailable` 행을 만들지 않습니다. 생략 건수와 사유 분류는 aggregate의 `unavailable_not_stored` 로그에 남습니다. 같은 `(vehicle, source, decode_epoch, analysis_id, metric, window_start)`에 이미 결과가 있을 때만 NULL/unavailable 리비전을 저장하여 과거 값이 유효한 것처럼 남지 않게 합니다. 실행 오류와 복구 기록은 생략하지 않습니다. 대시보드에 행이 없거나 오류 수가 0이어도 모든 지표가 계산 가능하다는 뜻은 아닙니다.
- 기존 결과를 초기화할 때는 집계 writer를 중지하고 새 runtime 배포를 확인한 뒤 **`vehicle_analysis`만** 비웁니다. `vehicle_signal`·`vehicle_event`는 보존하고 원시 데이터의 가장 오래된 시각부터 sealed hour까지 `BATTERY_BACKFILL_START/END`로 재계산한 후 writer를 재개합니다. 단순 `WHERE status='unavailable'` 삭제는 필요한 무효화 리비전까지 지워 과거 정상값을 되살릴 수 있으므로 사용하지 않습니다.
- 배터리 전용 한도 설정 오류는 `battery` 집계 구간에서 거부하며 AI·로그·차량·충전·홈 집계를 중단시키지 않습니다. 배터리 로그는 `ok`/`partial_errors`/`all_error`를 구분하고, 외부 집계기가 실패 결과에 중복 `ok` 메시지를 붙이지 않습니다.
- 실행 비용: 집계는 매 실행마다 최근 30시간(기본)의 sealed hour를 재계산한 뒤 `AGG_INTERVAL_SECONDS`(기본 300초)만큼 쉽니다. 신호 정규화는 실행 내 스코프별 한 번만 수행하고, 분석기·시간창마다 독립 사본에 기존 시간·수집 시각 필터를 적용합니다. 조건 분석의 극값·ID·SOC 조인은 secondary별 일괄 처리하여 관측점마다 다시 정렬하지 않습니다. 전압·전류 조인과 invalid 경계 확인은 정렬된 시각의 이진 탐색을 쓰고, 신호 정렬의 전체 행 직렬화는 같은 시각·필드·값의 충돌에만 수행합니다. 조회 범위·이전 신호 문맥·재전송 선택·분석 리비전은 유지하며, 지연 도착 데이터가 반영되도록 변경 없는 구간도 재계산합니다.
- 최신 판정은 `computed_at` 순서이므로 aggregate 실행 호스트의 시계를 동기화해야 합니다. 과거 시각으로 실행한 성공 재계산은 더 최신 시각의 오류 리비전을 덮지 못합니다. Docker VM과 호스트의 시계도 함께 확인하십시오.

### 에너지·용량과 전기적 추정의 해석
- 에너지 분석 1.3.0은 전류/전력의 0 교차를 나누어 충전·방전 총량을 각각 적분합니다. invalid·단위 미확인·동시각 충돌을 건너뛰어 적분하지 않으며, `decision_time_ns`가 있으면 수집 시각이 없는 관측도 제외합니다.
- `battery.energy.latest_power_kw`·`latest_pack_voltage_v`는 각 완료된 시간 구간의 **마지막 물리 관측**이며 실시간 값·시간 평균·적분 kWh가 아닙니다. V/A 단위가 확인되거나 `energy.field_calibration`의 정확한 `vehicle/source/decode_epoch` 범위에 맞는 `unit_scale`·`unit_offset`이 있어야 하며, 전력은 `current_sign`(+ 충전/− 사용 convention)도 필요합니다. `fleet/fleet-v1`이라는 이름 자체는 교정 증거가 아닙니다.
- 최신 전압/전류 각 leg가 같은 관측 시각이어야 순간 V×I를 계산합니다. 최신 invalid·단위 미확인·동시각 충돌·시각 불일치는 NULL이며 더 오래된 정상 pair를 찾지 않습니다. 관측이 구간 끝에서 `energy.max_gap_ns`(기본 10분)보다 오래됐어도 NULL입니다. `value_text`는 실제 관측 시각의 UTC 문자열(나노초 9자리), reason의 `asof_ns`는 원본 ns이고 `window_start/end`는 분석 시간 구간입니다.
- 전력·팩 전압 카드는 선택 기간 끝 직전 완료된 시간 구간만 보여 주고, 그 구간의 결과가 없으면 과거 구간 숫자로 대체하지 않습니다. 더 최근 원시 leg나 분석 완료 후 늦게 도착한 leg도 재분석 전까지 판단 불가입니다. 전력 그래프는 시간 구간당 마지막 교정 샘플을 실제 관측 시각에 점으로 표시합니다. 셀 전압·모듈 온도의 원시 물리 카드/그래프는 V/celsius 단위 메타데이터가 확인된 샘플만 표시하며 unit NULL은 판단 불가입니다. 셀 번호 보고 빈도는 전압 교정과 별개인 식별자 관측입니다.
- `DCChargingEnergyIn`은 배터리 유입 AC+DC, `ACChargingEnergyIn`은 충전기 측 AC입니다. 서로 더하지 않습니다. `LifetimeEnergyUsed`는 방전 누적량이며, EFC는 Ah·누적계·V×I 중 한 종류만 선택합니다.
- 충전·방전 세션은 부호가 교정된 전류, 충전 전력 또는 누적계 변화를 사용합니다. 양쪽의 관측된 유휴 경계와 연속된 누적계 끝점이 있어야 완료 에너지를 인정합니다. 시간 경계를 넘은 세션은 종료 시간 구간에 한 번만 기록합니다. 시간별 누적계 차분(`*_energy_in_kwh`, `discharge_energy_kwh`)은 직전 구간의 마지막 유효 값이 `max_gap_ns` 안에 있으면 그 값을 시작점으로 삼아(`anchor=prior_window`) 인접 구간의 합이 누적계 전체 증가량과 같게 나눕니다. 직전 값이 무효·충돌이거나 너무 멀면 구간 안 값만 씁니다. 적분값에는 구간 밖 에너지를 포함하지 않습니다.
- `parked_discharge_kwh`는 차량이 오프라인·수면이라 보고가 `max_gap_ns`보다 길게 끊긴 동안의 `LifetimeEnergyUsed` 증가량입니다. 보고가 재개된 시간 구간에 한 번만 기록하므로 `discharge_energy_kwh` 합계와 더하면 누적계 전체 증가량과 같습니다. 공백이 `energy.max_offline_gap_ns`(기본 24시간)보다 길거나 끝점이 무효면 계산하지 않습니다. runtime은 이 공백을 포함하도록 윈도우 앞 24시간의 원시 신호를 함께 읽습니다.
- `interval_capacity_kwh`는 부분 SOC 구간의 등가 용량이며 절대 SOH가 아닙니다. `full_usable_capacity_kwh`는 완료된 단조 100→0 SOC 방전과 독립 누적계가 필요합니다. `soh_pct`·`capacity_trend_kwh`는 이 측정에 비교 가능한 버전·domain·조건의 신품 기준이 있어야 계산합니다. 과거 완료 측정을 사용하면 reason의 `asof_ns`를 확인하십시오. `EnergyRemaining/Soc`는 별도의 BMS 순환 추정이며 독립 용량으로 승격하지 않습니다.
- `soh_estimated_pct`는 SOC가 `energy.soh_min_soc_span_pct`(기본 30%p) 이상 단조로 변한 완료 세션의 `interval_capacity_kwh`를 기준 용량으로 나눈 **추정치**입니다. 기준 용량은 `energy.reference`가 없으면 차량이 보고한 최신 `NominalFullPackEnergyKwh`(`bms_nominal_full_pack`)를 씁니다. 오차는 설정된 SOC·에너지 표준편차, 없으면 끝점당 SOC 0.5%p·0.1 kWh로 전파해 `uncertainty_lower/upper`에 남깁니다. 대시보드 건강도 카드는 `soh_pct`가 있으면 그것을, 없으면 이 추정치와 ±오차를 표시합니다.
- 오늘·이번 주 사용·충전 카드는 KST 기준 기간 안의 시간 구간별 최신 `discharge_energy_kwh`+`parked_discharge_kwh`, `dc_charging_energy_in_kwh` 리비전 합계입니다. 진행 중인 현재 시간은 다음 집계 후 반영됩니다.
- Coulomb/EKF의 초기 SOC에는 정확한 관측 시각 앵커가 필요하며, 공백 뒤를 임의로 재시작하지 않습니다. OCV 휴지 판정에는 실제 관측 구간이 필요합니다. 겉보기 DC 저항의 기본 step 범위는 10–60초이며 `electrical.dcr`로 명시적으로 조절할 수 있습니다. ICA/DVA는 요청 시간 구간의 적격 정전류 곡선만 사용합니다.

### 오프라인 NASA 참조와 실제 RUL
- 전체 참조 데이터는 B0005/B0006/B0007/B0018 네 랩 셀입니다. B0007은 right-censored이며 종단 사이클을 발명하지 않습니다. 랩 셀 방법 검증용일 뿐 Tesla 팩 수명 검증이 아니며, Tesla 정확도 주장은 없습니다.
- 오프라인 로더(선택적 scipy는 `.mat` 읽기에만 한정, aggregate에는 scipy 없음):
  ```bash
  python3 -m scripts.analytics.battery.battery_reference <input.mat|dir> <output.json> [--eol-ah 1.4] [--pretty]
  ```
- 실제 RUL 학습/평가/추론(`scripts/analytics/battery/battery_rul.py`, JSON만, pickle 없음):
  ```bash
  python3 -m scripts.analytics.battery.battery_rul train REFERENCE_JSON MODEL_JSON [--test-battery ID ...] [--min-history N] [--slope-window N] [--ridge FLOAT]
  python3 -m scripts.analytics.battery.battery_rul evaluate REFERENCE_JSON MODEL_JSON [--test-battery ID ...]
  python3 -m scripts.analytics.battery.battery_rul predict --model MODEL_JSON --history HISTORY_JSON
  ```
  aggregate 설정의 `rul.model`은 파일 경로가 아니라 학습 산출 dict를 인라인으로 붙여넣은 것이며, `rul.domain`은 모델의 dataset_domain(`nasa-pcoe-lab`)과 일치해야 합니다. `rul.history_scope {vehicle, source, decode_epoch}`는 선택 사항이지만 aggregate 신호에 스코프가 있으면 필수이며 일치해야 합니다. 실차 스코프(fleet/tesla/vehicle_signal/vss 포함)는 NASA 모델에 항상 거부됩니다. history 항목의 `observed_ns`는 선택 사항이지만 `decision_time_ns`가 설정되면 전 항목에 필수입니다. `model_version`은 적합 내용에 대한 결정론적 `sha256:<hex>`입니다(형식 `battery-rul-model/1` 유지). 예시의 `"rul": {}`는 그대로 유효합니다(→ `unavailable missing_model`). CLI 입력(`REFERENCE_JSON`/`MODEL_JSON`/`HISTORY_JSON {domain, history}`)은 오프라인 전용이며 aggregate 마운트가 아닙니다. `evaluate`는 학습 배터리와 겹치는 `--test-battery`를 거부하고(`train_test_overlap`) 참조 domain/EOL을 모델과 대조합니다(`domain_mismatch`/`eol_mismatch`).

### 실차 적용에 필요한 외부 전제
- 물리량 교정은 차량·source·decode epoch·적용 domain과 버전, 단위·전류 부호가 명시되어야 합니다. 초기 SOC와 그 시각, 팩 용량, 온도 조건에 맞는 OCV 곡선, 독립적으로 확인한 신품 기준 용량을 임의로 채우지 않습니다. 전제 부족은 `unavailable`이며 고장이나 정상 판정이 아닙니다.
- 기준 용량은 `energy.reference_source=bms_first`로 차량 BMS의 `NominalFullPackEnergyKwh`를 우선 쓰고, 차량이 이 필드를 보내지 않는 동안은 설정된 `energy.reference`를 씁니다. 공개 예제에는 합성 placeholder 기준값만 두며, 특정 팩·커뮤니티 추정치·Tesla 공식 수치 주장은 넣지 않습니다. 이 기준의 EFC·건강도는 참고용이며, 차량이 공칭값을 보내면 reason의 `ref=`가 `bms_nominal_full_pack`으로 바뀝니다. 실제 운영 기준값은 비공개 운영 설정에 둡니다.
- 단위 교정은 실데이터 대조로 정합니다: 팩/브릭 전압 비율 분포로 두 값의 단위를, 충전 구간 V×I 적분과 에너지 카운터의 일치로 전류 단위·부호를, 모듈 온도와 외기 온도의 상대 분포·충전 중 추세로 온도 단위를 판정합니다. 단위 근거가 없는 신호는 교정하지 않습니다. 교정은 해당 차량 가명·`fleet`·decode epoch 스코프에만 한정되며, 다른 차량·epoch에는 적용되지 않습니다. 실제 운영 교정 식별자·수치는 비공개 운영 설정에 둡니다.
- 실제 Tesla SOC·저항의 독립 기준값, 고장/EOL 라벨 및 검열된 수명 이력은 아직 검증 자료로 확보되지 않았습니다. 합성 회로는 수치 계산을, NASA 랩 셀은 학습·평가 경로를 검증할 뿐입니다. 적분 SOC를 독립 SOC 정답으로, EIS `Re`/`Rct`를 펄스 DC 저항 정답으로 사용하지 않습니다.
- CB-R은 **구현 차단 상태**입니다. [공급사 특허 목록](https://www.battermachine.ai/en/patents)과 공개 출원 식별자는 존재하지만, CB-R과 특정 방법의 대응·산식·교정·검증 사례·이용 조건은 확인되지 않았습니다. 공개 특허가 구현 허가를 뜻하지 않으며, DCIR을 CB-R로 이름만 바꾸지 않습니다. 공급사의 정확한 정의/검증 자료와 적용 가능한 권리 확인이 필요합니다.
- 공식 경고 설명 사전이 없으면 원래 경고 이름을 보존합니다. 확인된 출처·버전을 가진 사전만 설정하고, 동시 발생 조건을 원인이나 진단으로 단정하지 않습니다.

### 실행 경계·버전·오류와 배포 번들 검증

- 경계: 시간 단위 sealed 윈도우만 씀. `seal=floor_hour(now)`, `cutoff=seal-lookback`, 열린 시간 `[seal, seal+1h)` 미기입. 윈도우 `[ws, ws+3600e9)` 끝점 포함 `ws+3600e9-1`로 경계 샘플 이중집계 방지. backfill은 `[start, end]` 시간윈도우 추가 후 dedup. 일반 윈도우 1년·backfill 합산 2년 초과 거부. fetch 범위 밖 이전 데이터는 untouched + 필요 backfill 범위 경고만 남김. fetch 시작 이전에 시작된 에피소드는 backfill start 확장 필요.
- 버전: 동일 입력+설정+버전 → 동일 `revision`(동일 PK 덮어씀, wall clock 미포함). 변경 → 기존 옆에 새 revision 병존. 최신은 `computed_at DESC, revision DESC`(사전순 단독 금지), 파티션 `(vehicle, source, decode_epoch, analysis_id, metric, window_start)`. `episode_id`는 revision 불변으로 `analysis_id`에 접합. 동일 PK·상이 payload 충돌은 거부, exact 복사만 dedup. `vehicle/metric/source/analysis_id/revision/window_start` 결측 행은 미기입 + 경고 계수.
- 오류: scope×window 격리 실행. 실패 모듈은 해당 identity에 NULL/error 리비전 무효화(과거 보존, 타 모듈/epoch 불변). 복구 모듈은 동일 indicator identity에 `reported`/`value=NULL`/`value_text=execution_recovered`(건강값 아님). missing 모듈·malformed 설정·dispatch 전체 실패도 scope/window identity 오류행(빈 성공 금지). 로그 `ok`/`partial_errors`/`all_error` 구분, 외부 중복 `ok` 금지.
- 임베딩: analysis JSON은 네이티브 Compose `content` 보간으로 렌더링되며 소스는 공개 기본값 `BATTERY_ANALYSIS_CONFIG_JSON='{}'`(uncalibrated)입니다. 실제 운영 교정·모델 dict는 owner 전용 비공개 `.env`나 비공개 Compose 오버라이드 파일로만 공급하고 커밋하지 않습니다. inline `content`는 그대로 전달하므로 리터럴 `$`는 `$$`로 작성합니다. 중복 이름, `build`/`env_file`/`secrets`, configs `file:`, 호스트 바인드·익명 볼륨, `/run/secrets/`·`/var/run/secrets/` 참조는 렌더러가 거부합니다. 기본 aggregate 이미지는 stdlib-only `python:3.12.8-slim-bookworm`이며 scipy가 없습니다. 공개 예제 모듈은 합성 `{}` 상태입니다.
- 로컬 검증 명령(운영 자격 증명이 아닌 격리 DB와 합성 교정값을 사용):
  ```bash
  python -m pip install PyYAML==6.0.2
  python tools/render.py compose/core.yaml compose/database.yaml compose/ingest.yaml compose/grafana.yaml compose/backup.yaml compose/vehicle-raw.yaml compose/vehicle-vss.yaml compose/tesla-fleet.yaml compose/redecode.yaml compose/can-receiver.yaml --out compose.yaml
  python tools/check_env.py
  python -m tests.test_render
  for profile in server home mqtt vehicle fleet backup redecode can-receiver '*'; do docker compose --env-file .env.example --profile server --profile "$profile" config --quiet; done
  for name in common reference conditions energy electrical alerts rul runtime; do python -m "tests.test_battery_$name" || exit; done
  AGG_RUN_ONCE=1 python -m scripts.analytics.aggregate  # 격리 GREPTIME_* 필수; 운영 DB로 실행하지 않음
  ```
  배포 번들 재생성 순서는 CI(`.github/workflows/compose.yml`)와 같은 10개 조각을 사용합니다. 배포 전에는 생성물의 aggregate configs를 격리 컨테이너에 마운트하여 실제 프로세스 실행과 소스 해시 일치도 확인합니다.

### 포트/폴링/outbox 한계와 배포 승인
- 사설 포트: Fleet ZMQ 5555는 `tesla-helper_default` 사설망 내부이며, Greptime HTTP/MySQL·Grafana 바인드는 기본 loopback입니다. 원격은 Tailnet IP로만 열고 `0.0.0.0` 기본은 사용하지 않습니다.
- 폴링 없음: 정기 `vehicle_data` 폴링은 없으며(수신 전용), Tesla API 호출·차량 명령 송신은 일절 배제됩니다.
- outbox 한계: `MAX_OUTBOX_ROWS`(기본 50000, 신호+이벤트 total pending)가 수신 이후 기록만 묶으며, 초과 시 신규 유입을 거부하고 미전송 행을 삭제하지 않습니다.
- outbox 실행 비용: 기동 시 기존 행을 보존하며 스키마를 마이그레이션하고 신호·이벤트의 `event_time` 인덱스를 만듭니다. 기록 경로는 확정된 컬럼을 사용하여 매 행 스키마 조회를 생략하고, 업로드는 기존 시각순 선택을 유지합니다. 행별 durable commit(`WAL`, `synchronous=FULL`), total 한도 검사, 부분 ACK 시 행 보존은 바꾸지 않습니다. 인덱스는 조회 정렬을 줄이는 대신 저장 공간과 INSERT 비용을 추가합니다.
- 로컬 `compose.yaml` 재생성과 격리 검증은 준비 작업입니다. 운영 파일 반영, `docker compose up -d`, 차량 Fleet Telemetry 설정 POST는 각각 별도 승인이 필요합니다. 기존 `.env`·인증 파일·볼륨을 보존하고 실제 대상·현재 설정을 먼저 확인하며, 이 예시만으로 운영 설정을 덮어쓰지 않습니다.
