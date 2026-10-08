# 합성 성능 비교

[개발과 검증](development.md) · [기여 규칙](../CONTRIBUTING.md) ·
[CI workflow](../.github/workflows/performance.yml)

이 검사는 **같은 runner에서 PR base와 head를 비교하는 가벼운 오프라인 진단**입니다.
성능 변화는 advisory이며 merge 실패 기준이 아닙니다. 정답 불일치, 비결정적 결과,
실행 오류와 비교 조건 불일치는 실패합니다. 운영 endpoint, 차량 데이터, 비밀 정보,
Docker, GreptimeDB 서버는 사용하지 않습니다.

## 언제 실행하나

- 모든 `pull_request`에 `Paired synthetic benchmark` job을 만듭니다. workflow 수준의
  path filter가 없으므로 required check가 unrelated PR에서 pending으로 남지 않습니다.
- job 안에서 `scripts/`, `tests/`, `tools/`, `compose/`, `config/`,
  `.github/workflows/`, 생성물 `compose.yaml` 변경을 확인합니다. 해당 변경이 없으면
  이유를 summary와 artifact에 기록하고 benchmark를 건너뜁니다.
- PR은 작은 `quick` profile만 실행합니다. 정기 schedule은 없습니다.
- Actions의 **Synthetic performance → Run workflow**에서 `quick`, `large`, `soak`,
  `fault` 중 하나를 선택할 수 있습니다. candidate는 선택한 workflow revision의
  `github.sha`, base는 checkout 시점의 `master`입니다. 두 full SHA를 `plan.json`에
  고정하여 남깁니다. 임의 base ref나 외부 baseline artifact는 입력받지 않습니다.
- `duration`은 1–600 정수, 기본 60입니다. `soak`에서 revision 하나의 목표 시간을
  workload 수로 나누어 반복합니다. 다른 profile은 고정 sample 수를 사용하며 duration은
  비교 metadata에만 남깁니다. 최소 sample 수와 진행 중인 작업 때문에 soak 목표 시간은
  엄격한 wall-time 상한이 아닙니다. worker 하나는 120초 timeout, 전체 job은 30분 timeout입니다.

## 측정과 정답 검사

두 checkout을 같은 Ubuntu 24.04 job에서 base → candidate 순서로 실행합니다.
Python은 `3.12.14`, 직접 설치하는 dependency는 다음 세 가지로 제한합니다.

```text
cantools==40.7.1
opentelemetry-proto==1.38.0
PyYAML==6.0.2
```

전이 dependency도 [`tools/benchmark-requirements.txt`](../tools/benchmark-requirements.txt)에
버전을 고정합니다. 설치 결과는 `environment.txt`와 JSON runtime metadata에 기록합니다. CPU 정보와 runner image도 evidence에 남깁니다. 같은 job의 두
revision은 같은 설치 환경을 공유하지만, 서로 다른 날짜의 runner 상태까지 같다는 뜻은 아닙니다.

candidate의 `tools/benchmark.py`와 합성 fixture 구현을 **두 revision에 동일하게**
사용합니다. application import만 `--repo`의 checkout에서 읽습니다. 따라서 base에
benchmark 도구가 아직 없어도 비교할 수 있습니다. 두 checkout의 기존 CAN receiver,
CAN decoder, OTLP wire, Fleet, VSS, activity 회귀를 먼저 실행합니다. 새 harness의
`tests.test_benchmark`와 `tests.test_bench_workloads`는 candidate에서 실행합니다. workload 자체의
assertion과 각 sample의 golden 결과 digest도 검사합니다. 성공한 회귀만으로 실제
서비스 전체 경로가 검증됐다고 해석하지 마세요.

각 workload는 fresh Python process에서 한 번 warm-up하고, 그 결과를 제외한 최소
3개 sample의 median을 보고합니다. fixture 준비와 import, dependency 설치는 timed
operation 밖입니다. operation의 correctness 검사 시간은 측정에 포함됩니다.
`quick`은 작은 fixture, `large`는 더 큰 fixture, `soak`은 반복 sample입니다.
`fault`는 통제된 SQLite reopen, 중복·순서 변경 replay와 로컬 ACK 삭제 모사입니다.
SIGKILL, 전원 손실, downstream outage, 실제 network ACK나 저장장치 오류를 재현하지 않습니다.

| 지표 | 의미와 한계 |
| --- | --- |
| `wall_seconds` | timed operation 경과 시간. process 시작과 fixture 준비 제외 |
| `units_per_second` | 고유 입력 record/frame 수를 wall time으로 나눈 값. 중복 replay와 검증 비용 포함; network 처리량이 아님 |
| `cpu_seconds` | 해당 process의 timed operation CPU 시간 |
| `cpu_percent` | `100 × CPU / wall`. 시스템 전체 utilization이나 core 수로 정규화한 수치가 아님 |
| `peak_rss_bytes` | process 수명 전체의 peak RSS. import와 fixture 준비 포함; timed operation만의 peak가 아님 |
| `setup_peak_rss_bytes` | operation 직전의 RSS high-water mark. idle RSS나 현재 RSS가 아님 |
| `disk_before_bytes`, `disk_after_bytes` | 임시 fixture 디렉터리의 파일 논리 크기 합계. 존재하는 SQLite WAL 포함 |
| `disk_pending_bytes` | SQLite 연결 종료·모사 ACK 삭제 전의 pending DB/WAL 논리 크기. 연속 감시 peak가 아니며 AI workload는 0 |
| `disk_growth_bytes` | operation 전후 논리 크기 차이. 물리 할당량, peak disk, I/O bytes, container image 크기가 아님 |

RSS는 Linux의 KiB 반환값을 bytes로 바꾸며 macOS에서는 반환 bytes를 그대로 씁니다.
자식 서비스·컨테이너 메모리, DB 서버 비용, 운영 network/ACK latency는 측정하지 않습니다.

각 결과 JSON은 revision, profile, duration, Python/platform/dependency 정보,
harness와 fixture 소스의 SHA-256, workload별 golden digest, raw sample과 median을
담습니다. 비교 시 환경·fixture·harness·profile·duration·workload 집합·golden 출력과
작업량이 일치해야 합니다. 기준값이 0이면 변화율은 `n/a`입니다.

### 합성 workload 범위

- `can`: 생성한 DBC와 dense CAN chunk를 decode하고 SQLite archive/outbox의 값·event ID·cursor·중복 제거를 검증
- `fleet`: 합성 Fleet frame의 nanosecond와 값을 SQLite outbox에 보존하고 replay 중복을 검증
- `vss`: 합성 VSS 값의 숫자·false·빈 문자열·Unicode 구분과 영속 outbox 중복 제거를 검증
- `ai`: 합성 span replay를 dedupe하고 session/daily 합계와 미보고·0 token, 미산정 cost의 NULL을 검증

profile별 고유 입력 수는 workload마다 `quick`/`fault` 100, `large` 3,000,
`soak` 1,000입니다. seed와 fixture version은 `tools/bench_workloads.py`에 고정됩니다.

## 로컬 재현

이미 준비한 두 checkout과 격리된 Python 환경을 사용하세요. 아래 `HEAD_SHA`와
`BASE_SHA`에는 각 checkout의 실제 full SHA를 넣습니다. 두 실행 모두 같은 candidate
harness를 사용해야 합니다.

```bash
python -m pip install --only-binary=:all: \
  -r /path/to/candidate/tools/benchmark-requirements.txt

python /path/to/candidate/tools/benchmark.py run \
  --repo /path/to/base --output /tmp/performance/base.json \
  --profile quick --duration 60 --revision BASE_SHA
python /path/to/candidate/tools/benchmark.py run \
  --repo /path/to/candidate --output /tmp/performance/candidate.json \
  --profile quick --duration 60 --revision HEAD_SHA
python /path/to/candidate/tools/benchmark.py compare \
  --base /tmp/performance/base.json --candidate /tmp/performance/candidate.json \
  --output /tmp/performance
```

`summary.md`는 사람용 비교, `results.csv`는 metric별 수치,
`comparison.json`은 기계용 advisory 결과입니다. CI는 plan, 환경, 회귀 로그,
개별 benchmark JSON과 로그도 보관합니다. 실패해도 생성된 evidence를 upload하며
artifact 보존 기간은 7일입니다. job 시작 전 실패·강제 종료처럼 upload 자체가
불가능한 경우에는 artifact가 없을 수 있습니다. 실패나 skip을 측정 통과로 해석하지 마세요.

## 해석과 보안 경계

shared runner noise, base-first 실행 순서, OS/file cache, 작은 fixture와 짧은 sample은
변화율을 흔들 수 있습니다. 큰 변화는 같은 조건으로 재실행하고 raw sample을 확인하세요.
이 결과는 처리량 SLA, 운영 용량, 물리 CAN, 실제 차량, S3 호환성, 장기 안정성,
end-to-end DB 처리 성능이나 production 효율을 보증하지 않습니다. application 기본값을
이 숫자 하나만으로 바꾸지 마세요.

실제 합성 데이터가 Alloy와 GreptimeDB를 통과하고 queued replay와 fresh restore를
검증하는 검사는 기존 Compose workflow의 `synthetic-runtime` job입니다.
[합성 런타임 통합 검사](development.md#합성-런타임-통합-검사)를 참조하세요.
이 benchmark의 golden 검사는 그 Docker 통합 검사를 대체하지 않습니다.

workflow는 `pull_request`를 사용하며 `pull_request_target`을 사용하지 않습니다.
권한은 `contents: read`, checkout credential 저장은 꺼져 있습니다. 운영 secrets,
쓰기 token, baseline cache 승격, PR comment 쓰기, 외부 baseline artifact 재사용은
없습니다. fork의 코드와 생성 artifact는 신뢰할 수 없는 입력으로 취급해야 하며,
후속 privileged workflow에서 실행하거나 공식 baseline으로 자동 승격하면 안 됩니다.
