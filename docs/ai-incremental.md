# AI 증분 집계 (sequence fence)

`ai_section`은 전체 재계산(oracle)을 그대로 두고, 펜스가 증명될 때만
fenced delta로 건너뜁니다. 타임스탬프 HOLD는 없습니다: 서버 DEFAULT
스탬프는 가시성보다 먼저 찍히므로 유한 hold는 임의 지연을 보장하지
못합니다.

소스 핀: GreptimeDB v1.2.1 (커밋 `179ff8e`,
`src/operator/src/insert.rs` L394-448 `fill_reqs_with_impure_default`,
`src/mito2/src/worker/handle_write.rs` L79-95, Flight DoGet
`src/servers/grpc/flight.rs`, terminal 워터마크
`src/servers/grpc/flight/stream.rs`, Rust Ticket 구성
`src/client/src/database.rs` L739-751/778-797). 런타임 동일 버전 +
캐시 venv `grpcio==1.84.0`/`pyarrow==25.0.1`/`protobuf==6.33.6`
(내부 `GREPTIME_GRPC_URL=grpc://greptimedb:4001`, TLS 헬퍼 포함;
https/grpcs는 `grpc.secure_channel`, 평문 downgrade 없음).

## 발견 경로

- Arrow Flight DoGet + `x-greptime-flow-extensions`:
  `flow.return_region_seq=true` 항상, 펜스 읽기에는
  `flow.incremental_mode=memtable_only` +
  `flow.incremental_after_seqs={region_id: seq}` 추가.
- 응답은 끝까지 drain한 뒤 terminal metadata의
  `region_watermarks=[{region_id, watermark}]`를 upper로 쓴다.
  watermark 0은 유효(빈 테이블), 누락/null/`UNPROVED`는 미증명이다.
- fenced 쿼리는 `(lower, upper]` 구간의 변경 키 최신 행만 돌려준다
  (누적 메트릭 delta가 아님).
- 저장 펜스에 없는 반환 리전이 **하나라도 있으면** generation
  reset(신규/재분할/재생성 리전)으로 보고 전체 복구한다. 전부
  새 리전인 경우만이 아니다. seq 0으로 가정하지 않는다.
- `lower == flushed` noop는 행 0 + 같은 upper로 정상이다.
  `lower < flushed`는 서버가 `STALE_CURSOR ... retry_hint:
  FALLBACK_FULL_RECOMPUTE`로 돌려준다(실측 gRPC status는 INTERNAL;
  status 코드가 아니라 본문 마커로 판정). 이 경우 명시적 전체 복구다.
- HTTP `/v1/sql`은 펜스를 돌려주지 않으므로 **발견**에는 쓰지
  않는다. 단, dirty 세션/날짜의 scoped 재조회와 전체 oracle은 HTTP
  SQL 그대로다(아래 재계산 절).
- 스톡 pyarrow Flight 클라이언트는 Greptime terminal NONE 프레임을
  읽지 못하므로 raw grpcio + 공식 protobuf + pyarrow IPC를 쓴다
  (`scripts/database/greptime_flight.py`). IPC는 스풀 파일 +
  `open_stream`으로 dictionary 배치까지 디코드하고, max-row 초과는
  부분 쓰기 없이 실패한다. 타임스탬프는 int64 ns 정밀도로 읽는다.

## 재계산 (global merge + cohort)

- dirty 세션은 전체 이벤트 히스토리 + 해당 날짜 전체를 기존
  summarize/merge oracle로 재계산한다. 날짜별 재merge가 아니라
  세션 히스토리와 dirty-day 행을 합친 뒤 **한 번만** 전역 merge한다:
  날짜별 재merge는 날짜를 넘나드는 native 매칭(Aug trace + Sep native
  log)을 깨뜨린다.
- cohort: dirty day에 행이 있는 모든 세션은 날짜가 달라도 전체
  히스토리를 가져온다(native-over-trace는 세션 단위, 날짜 단위가
  아님). 같은 날의 무관 세션도 cohort에 포함되지만, 쓰기는 dirty
  세션(세션 요약)과 dirty day 버킷(일별/도구별)으로만 제한하므로
  무관 날짜를 부분 컨텍스트로 덮어쓰지 않는다.
- 세션 ID가 없는 행도 유효한 raw다(일별/도구별 집계 대상): dirty
  day는 전체 delta 타임스탬프에서 시드하고, 세션 키가 비어도
  day/cohort 재계산을 수행한다. 행이 정말 하나도 없을 때만 noop이다.

## 메트릭

- 논리 테이블별 펜스로 읽는다(같은 물리 테이블의 두 논리 테이블도
  논리 리전 ID가 다르며 물리 시퀀스를 공유한다). 어느 테이블에서든
  STALE·신규 리전·전송 실패가 나면 raw delta가 비어도 전체 복구
  후에만 커밋한다.
- 펜스 delta는 변경 행만 담으므로, dirty 세션의 전체 히스토리와
  dirty day의 전체 계기를 scoped로 다시 읽어 누적 카운터의 기준점과
  손대지 않은 계기를 보존한다. 세션/날짜 범위 한정이며 무제한 전체
  스캔이 아니다.

## 상태

`ai_aggregate_state` scope='ai' 단일 행:

- `fence`: `{table: {region_id: seq}}` upper JSON (커밋된 펜스).
- `price_digest`: LiteLLM 가격표 sha256('none' 포함). 변경 시 전체
  비용 셀 재빌드.
- `config_revision`: `'ai:fence-v1'` 불일치 시 전체 복구.
- `input_digest`: 메트릭 레지스트리 `[(table, instrument)]` JSON.
  테이블 추가/삭제/개명 시 전체 복구(COUNT+MAX 스캔 없음).
- `committed_at`: 커밋 wall 시각. 파생 상태 TTL 없음.

커밋은 파생 쓰기 전부 성공 뒤에만 upper를 기록한다. 전송 실패는
체크포인트를 전진시키지 않는다(실패하면 펜스가 뒤에 남아 다음
패스가 같은 구간을 재발견한다). deps/endpoint 미증명으로 펜스
upper 자체를 증명하지 못하면 빈 펜스로 upper를 단정하지 않는다:
oracle로 복구하고 증명된 upper가 있을 때만 커밋한다.

## 복구

- `AGG_AI_FULL_REBUILD=1`: 매 패스 전체 복구(명시적 복구 절차).
- Flight deps/grpc endpoint 미증명, terminal watermark 없음,
  STALE, generation reset, digest/revision 불일치: 전체 복구 후
  증명된 upper 커밋.
- max-row 초과(`RowCapExceeded`): 부분 쓰기 없이 섹션 실패하고
  체크포인트를 전진시키지 않는다. scoped 메트릭 재조회도 같은
  row-cap 가드 아래 있으며, 초과는 전체 복구로 숨기지 않고 그대로
  전파한다.
- 메트릭 단위 변환(`claude_code.active_time.total` 등)은 oracle과
  fenced 경로가 같은 `_ai_metric_unit` 헬퍼를 공유하므로
  단위/temporality 해석이 갈라지지 않는다.
- 로그 세션 조건은 네이티브 검증된 `json_get_string` JSONB
  predicate다(`->>` 미지원). 세션 키는 attrs 기준이며 body-only
  별칭은 주장하지 않는다.

## 한계

- memtable-only 펜스는 flush되지 않은 컨텍스트를 스캔한다. 상수/O(1)
  조회를 주장하지 않는다. dirty 날짜·세션 자체가 크면 재계산도 커진다.
- TTL로 원본 일부가 만료된 세션/날짜는 기존 집계를 덮어쓰지 않는다
  (`fully_retained` 그대로).

## 격리 런타임 측정

macOS arm64, Python 3.13.14, GreptimeDB 1.2.1 File 저장소에서
`tools/benchmark_ai_incremental.py`로 원래 전체 재계산과 비교했다.
과거 trace 200/2,000개와 그 5% 로그를 같은 과거 한 시간에 고정하고,
신규 입력은 별도 날짜에 0개, 5개 세션의 trace/log 10행,
한 장기 세션의 trace/log 40행으로 독립 변경했다.
각 조건은 새 DB에서 3회 반복했고, 각 DB의 최초 재빌드·입력·두 번의
무변경 패스마다 네 파생 테이블의 모든 셀이 원래 구현과 일치했다
(18개 DB 쌍, 72개 패스 비교).

아래 값은 baseline → 변경 후 중앙값이다. 무입력은 6개 반복,
입력은 3개 독립 반복이다. 각 패스는 별도 Python 프로세스로 실행해
Flight/Arrow 최초 import 비용을 포함한다. 최초 전체 재빌드는 제외했다.
응답 bytes는 HTTP 응답 body와 serialized FlightData payload 합계이며
HTTP/2·TLS·헤더 비용은 포함하지 않는다. RSS는 전체 프로세스 peak다.

| 과거 trace | 신규 행 | HTTP raw 조회 행 → (+Flight 행) | 응답 bytes | wall ms | CPU ms | peak RSS MiB |
| ---: | ---: | --- | --- | --- | --- | --- |
| 200 | 0 | 210 → 0 (+0) | 95,217 → 8,852 | 62.64 → 115.81 | 18.28 → 99.64 | 41.5 → 79.9 |
| 2,000 | 0 | 2,100 → 0 (+0) | 902,491 → 8,856 | 425.81 → 120.45 | 102.25 → 98.02 | 52.5 → 79.4 |
| 200 | 10 | 220 → 20 (+10) | 98,782 → 66,397 | 73.61 → 166.77 | 19.96 → 110.44 | 41.8 → 81.0 |
| 2,000 | 10 | 2,110 → 20 (+10) | 906,066 → 73,722 | 454.60 → 175.73 | 108.04 → 111.43 | 52.6 → 81.0 |
| 200 | 40 | 250 → 80 (+40) | 108,690 → 87,839 | 69.06 → 161.96 | 20.79 → 112.66 | 41.9 → 81.5 |
| 2,000 | 40 | 2,140 → 80 (+40) | 916,004 → 95,224 | 418.41 → 185.07 | 104.64 → 122.31 | 52.9 → 81.8 |

변경 후 쿼리는 무입력에서 SQL 9개 + Flight 2개, 10행 입력에서
SQL 26개 + Flight 2개, 40행 입력에서 SQL 22개 + Flight 2개였다.
이 세 값은 과거 크기에 따라 늘지 않았다. 원래 SQL 수는 같은 순서로
27/32/28개에서 207/212/208개로 증가했다.
상태 관련 SQL 7개(스키마 확인·펜스 읽기·저장 포함)는 무입력에서도
발생했으며 요청 1,270–1,272 bytes, 응답 730–732 bytes,
서버 실행 합계 중앙값 1 ms였다. 레지스트리·파생 상태 조회 비용은
이와 별도이며 전체 응답 bytes에는 모두 포함했다.

raw 재조회는 제거됐지만 작은 fixture에서는 cold-start wall/CPU가
나빠졌고 Arrow 의존성 때문에 RSS도 늘었다. 2,000 trace fixture의
wall 감소를 운영 처리량·warm 장기 프로세스 성능으로 확대하지 않는다.
메트릭 단독 수정은 실제 native OTLP 논리 테이블에서 추가로 실행해,
동일 key/time의 commit 카운터 변경이 미변경 PR 카운터 2와
delta-temporality LOC 2를 보존하며 전체 재계산과 일치함을 확인했다.
session ID 없는 실제 trace도 일별 77/33 토큰을 보존했다.

재현에는 운영 DB가 아닌 별도 File 저장소와 고유한 새 DB prefix,
검증할 원래 구현의 별도 checkout, 같은 Python 의존성이 필요하다.
`--repeats 3`은 한 DB에서 입력 패스 한 번과 무변경 두 번을 뜻하므로,
독립 입력 중앙값을 얻으려면 새 prefix로 명령 전체를 세 번 실행한다.

```bash
python tools/benchmark_ai_incremental.py \
  --base-url http://127.0.0.1:4000 --grpc-url grpc://127.0.0.1:4001 \
  --db ai_bench_unique_run --user "$BENCH_USER" \
  --password-env BENCH_PASSWORD --baseline-root /tmp/ai-full-baseline \
  --matrix --history-sizes 200,2000 --repeats 3
```

