# OTel 원본 보존 정책과 안전한 TTL 적용 절차

[설치와 운영](operations.md) · [개발과 검증](development.md)

수치는 합성 환경에서 측정하고 운영 용량이나 절감액으로 일반화하지 않습니다.
운영 TTL 변경·데이터 삭제는 별도 작업입니다.

## 요구 보존 기간과 예산

| 항목 | 정책 |
| --- | --- |
| 디버깅 필요 기간 | 미정 — 운영자가 선언 (기본 무제한이므로 만료 없음) |
| 재집계 필요 범위 | `ai_session_summary`·`ai_daily_summary` 등 파생 집계는 TTL 없음. 원본 만료 구간은 재집계 불가 |
| 늦은 이벤트 허용 | `ingest_time`(서버 수신 시각, `DEFAULT CURRENT_TIMESTAMP`)으로 발견. `NULL`(구행)은 순서 불명 → 전체 재빌드, 스킵 금지 |
| 저장 예산 | 디스크 기준 `retention_days ~= disk_budget_bytes / disk_bytes_per_day`(아래 Main 시나리오로 측정). wire 추정치는 전송 크기라 물리 용량으로 부르지 않음 |

## 기본값 결정 근거

- 현재 기본값은 **무제한 유지**(`OTEL_TTL=` 빈 값 → `ttl='0s'`, 만료 없음).
- 신규 설치에도 유한한 권장값(30일/90일 등)을 **자동 적용하지 않습니다**.
- 근거: 파생 집계가 원본 만료 구간을 불완전한 결과로 덮어쓰지 않는다는
  계약(`fully_retained`·워터마크)이 무제한 원본을 전제로 검증됐고,
  유한 TTL은 기존 행을 되돌릴 수 없이 만료시키기 때문입니다.
- 유한 TTL이 필요하면 운영자가 예산을 선언한 뒤 아래 절차로 별도 승인 하에 적용합니다.

## Read-only 점검

```bash
python tools/inspect_otel_retention.py --base-url http://127.0.0.1:4000 \
    --db datalake --user datalake --password-env GREPTIME_PASSWORD \
    [--daily-days 30] [--sample-rows 20] [--budget-bytes N] \
    [--disk-bytes-per-row-traces F --disk-bytes-per-row-logs F \
    --disk-measurement TEXT --disk-budget-bytes M]
```

- `information_schema.tables`의 실제 `create_options` 문자열에서 테이블별
  effective TTL을 파싱하고, `COUNT(*)`·`MIN/MAX(timestamp)`로 누적 행과 기간을 읽습니다.
- 일별 증가율은 요청한 현재 시각 기준 최근 N UTC 일(`--daily-days`)로 묶고
  `WHERE timestamp >= <window_start>`로 제한합니다. 테이블별 일별 행을
  공통 달력 일자에 합산(combined)하고 무입력 일은 0으로 계산합니다.
  `DATE` 그룹값이 숫자 일수(days-since-epoch)로 반환되면 `YYYY-MM-DD`로 포맷합니다.
- 측정값: 테이블별 rows/oldest/newest, 창 내 일별 rows 시리즈(합산+테이블별),
  샘플 `SELECT * LIMIT N` wire bytes/row(전송 크기, 디스크가 아님), 쿼리별
  result rows·server ms·client s·request bytes.
- 바이트율 합산은 실제 행율 가중합(`Σ 테이블 일평균행 × 해당 테이블 bytes/row`)이며
  평균끼리의 곱(평균행×평균크기)을 쓰지 않습니다.
  `NaN`/`inf`/음수율은 거부하고 `unknown`에 별도 보고합니다.
- 물리 용량은 디스크 기준만 씁니다. `information_schema.tables`의
  `data_length`(SST 바이트, 근사) + `index_length`(인덱스 파일 바이트, 근사)만이
  실제 사용 바이트입니다. `max_data_length`/`max_index_length`는 용량(capacity),
  `avg_row_length`는 평균이라 합산에 쓰지 않고 라벨만 따로 표기합니다.
  셋 중 실제 노출된 지표가 없으면 `null`로 보고하고 추정하지 않습니다.
- `data_length`+`index_length`가 없으면 운영자 실측
  `--disk-bytes-per-row-{traces,logs}` + `--disk-measurement`(측정 근거) 입력으로
  `--disk-budget-bytes` 대비 `retention_days_at_disk_budget`을 계산합니다.
  근거 없는 디스크 추정은 하지 않습니다.
- `--budget-bytes` 지정 시 wire 전송 추정치(`retention_days_at_budget`)만 별도
  표기하며 물리 용량이라 부르지 않습니다.
- 창 내 입력이 없거나 시리즈를 못 읽으면 `unknown`·`empty_window`로 따로 보고하고
  낡은 히스토리를 현재 증가율처럼 쓰지 않습니다.
- `--daily-days`·`--sample-rows`는 1 이상, 예산·디스크 바이트 입력은 0 이상이어야 합니다.
- 쓰기·변경·삭제 없음. 정확한 물리 바이트는 소유 스냅샷
  모드(아래 owned-store 비교)에서만 측정합니다.

## 점검 시나리오

- 불균등 볼륨·크기: traces 100행/일·10B/행, logs 300행/일·100B/행 합성 입력에서
  combined 400행/일, `disk_bytes_per_day` 가중합 31000B/일이 나오는지 위 점검
  명령(`--disk-bytes-per-row-traces 10 --disk-bytes-per-row-logs 100
  --disk-measurement ... --disk-budget-bytes ...`)으로 확인합니다.
  구식(평균행×평균크기=11000)은 오답입니다.
- 최근 창 무입력: 빈 최근 창에서 `combined` 0, `empty_window: true`,
  `retention_days_*` 미계산인지 확인합니다.

## 합성 비교

```bash
SYNTH_PASS=... python3 tools/benchmark_otel_retention.py \
    --base-url http://127.0.0.1:4000 --db otel_ret7_bench \
    --user issue_synthetic --password-env SYNTH_PASS \
    --traces 2000 --logs 2000 --finite-ttl 7d
```

- 공유 모드: 격리 DB(`otel_ret7_bench`)의 벤치 테이블에만 같은 합성 입력·기간을
  적재하고 무제한(`0s`)·유한 시나리오의 측정 보존 rows, 쿼리 result rows/server
  ms/client s/request bytes를 비교합니다. 테이블 바이트는 1.2.1 SQL 방언에
  카운터가 없어 `null`로 보고하며 추정하지 않습니다.
- 소유 저장소 모드(정확한 물리 바이트·오프라인 백업 증명):

```bash
python3 tools/benchmark_otel_retention.py \
    --greptime-binary /path/to/greptime --data-home /tmp/ret7-store \
    --traces 2000 --logs 2000 --finite-ttl 7d
```

- 시나리오마다 소유 standalone Greptime을 기동해 같은 fixture(sha256 고정, 같은
  행 모양·기간)를 적재하고, 정상 종료 뒤 data-home 실측 바이트와 오프라인 압축
  스냅샷(tar.gz bytes+sha256)을 측정합니다. 기본적으로 스냅샷을 새 디렉터리에
  풀고 재기동해 행 수를 재확인(restore proof)합니다. 모든 포트는 루프백에
  바인딩하며 기존 런타임은 건드리지 않습니다. 유한 TTL의 비동기 만료는
  `ADMIN FLUSH_TABLE`과 `ADMIN COMPACT_TABLE` 이후 보존 행 수로 확인합니다.
- 운영 테이블·데이터를 건드리지 않으며 공유 모드는 기본적으로 벤치 테이블을 삭제합니다.

### 합성 측정 예시(합성 고정 fixture, 생산 예측 아님)

- 고정 입력 4000행(traces 2000 + logs 2000, 절반 historic 절반 recent),
  native Greptime 1.2.1, post-flush+compact 후 실측.
| TTL | 보존 행 | 저장소 bytes | 오프라인 백업 bytes | 복원 행 |
| --- | ---: | ---: | ---: | ---: |
| 무제한(`0s`) | 4000 | 122923 | 38679 | 4000 |
| 7일 | 2000 | 115706 | 38446 | 2000 |

두 시나리오 모두 복원 행 수가 일치했습니다. 수치는 이 고정 합성 조건에서만
유효하며 운영 용량·절감액으로 일반화하지 않습니다.

## 검증

```bash
python -m tests.test_otel_retention
```

신규 초기화 무제한·기존 재초기화 TTL 반영·잘못된 TTL 거부·유한→무제한 복귀,
TTL 경계 장기 세션·부분 만료 날짜·늦은 도착 시 기존 집계 보존을 확인합니다.

## TTL 변경 절차와 되돌릴 수 없는 위험

1. 적용 전 백업/복구 검증과 별도 승인을 받습니다. 대상 테이블
   (`opentelemetry_traces`, `opentelemetry_logs`), 보존 기간, 영향 범위를 확인합니다.
2. `.env`의 `OTEL_TTL`을 바꾸고 `db-init`을 재실행합니다.
   **초기화 재실행은 기존 테이블 TTL도 갱신합니다**
   (`CREATE IF NOT EXISTS`는 스키마를 유지하지만 이후 `ALTER ... SET 'ttl'`이 적용됨).
   환경 변수 변경이 신규 테이블 기본값 변경으로 오해되지 않게 합니다.
3. 적용 후 effective TTL을 위 점검 명령으로 확인합니다.
4. **이미 만료된 원본은 TTL을 늘려도 복원되지 않습니다.**
   TTL 축소는 기존 데이터 만료를 일으킬 수 있습니다(되돌릴 수 없음).
5. 원본 일부가 만료된 세션/날짜의 집계는 불완전한 결과로 덮어쓰지 않습니다
   (집계기의 TTL 경계·late arrival·워터마크 계약과 호환).
6. 장기 집계 테이블·차량 CAN archive·RAW provenance·원격 백업 세대 정책은
   OTel TTL 변경에 섞지 않습니다.
