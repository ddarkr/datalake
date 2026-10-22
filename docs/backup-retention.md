# 원격 백업 보존(Remote retention)

기존 `BACKUP_KEEP`은 **로컬 archive 세대 수**이며 원격 정리와 무관합니다.
원격 보존은 기본 비활성(`off`)이며, `backup` 성공 뒤 자동 평가됩니다.
삭제는 `retention enforce` + `BACKUP_REMOTE_RETENTION_APPROVE=1`에서만 일어납니다.
운영 삭제는 검증된 dry-run 검토와 별도 명시적 승인 뒤에만 진행하세요.

관련 문서: [설치와 운영](operations.md)의 백업/복원 절차와 Compose `backup` 서비스.

## 설정

| 변수 | 기본 | 의미 |
|---|---|---|
| `BACKUP_REMOTE_RETENTION_MODE` | `off` | `off\|dry-run\|enforce` |
| `BACKUP_REMOTE_RETAIN_COUNT` | `7` | 최신 N세대 유지 |
| `BACKUP_REMOTE_RETAIN_DAYS` | `0` | N일 이내 유지(`0`이면 끔; NaN/inf 거부) |
| `BACKUP_REMOTE_MIN_RECOVERY` | `1` | 최소 복구 세대 |
| `BACKUP_REMOTE_PINNED` | `` | 쉼표 구분 보존 ID (`/`·`..` 거부) |
| `BACKUP_REMOTE_INCOMPLETE_GRACE_SEC` | `86400` | 미완료 유예(동시 실행 보호) |
| `BACKUP_REMOTE_RETENTION_APPROVE` | `` | `enforce` 삭제에 `1` 필요 |

## 복구 세대

tar + sidecar + 내장 manifest + SST snapshot + **검증된 COMPLETE**를 한 세대로 취급합니다.
미완료 백업은 복구 세대로 세지 않습니다. 삭제 후보는 스키마 유효 COMPLETE와
정확한 최상위 tar/sidecar/manifest, COMPLETE 목록 전체 SST(크기 일치)가 모두
있는 세대에 한합니다. 손상된 COMPLETE(파싱 불가·미래 시각 포함)는
fail-closed(건너뜀, 삭제·집계 제외)입니다. 미래 시각 COMPLETE가 있다고 해서
age 정책으로 복구 세대를 보호하는 것이 아닙니다.

새 백업(`shared-v1`)은 `<prefix>/_sst/<identity>`에 SST를 한 번 보관하고,
새 per-SST `.json`을 쓰지 않습니다. 각 세대의 검증된 manifest와
COMPLETE는 **해당 시점의 전체 SST 목록**과 공유 객체 key를 담습니다.
manifest는 전체 목록과 무결성 필드를 유지한 compact JSON으로 저장해 불필요한 공백 I/O를 줄입니다.
이전 백업을 차례대로 적용하는 증분 체인이 아니므로, 이전 세대의 archive를
삭제해도 보존 세대가 참조하는 SST만 남아 있으면 복원할 수 있습니다.
기존 `<prefix>/<id>/sst/` 형식의 HEAD/SHA256 백업도 그대로 복원·보존합니다.

- 이미 retirement이 시작된 세대(RETIRING + COMPLETE 부재 → `incomplete/created_ns=None`)
  는 grace gate 대상이 아닙니다. 재개 전 마커 본문이 해당 세대를 정확히 가리키는지
  검증하고 활성 `refs/`가 없을 때만 `COMPLETE` 부재 확인 → payload 삭제 → `RETIRING`
  마지막 삭제 순으로 재개합니다. 마커 본문이 다르거나 읽히지 않으면 재개를 거부합니다.
  `RETIRING` 없는 새 incomplete prefix(진행 중 백업)는 기본 grace=86400 안에서
  enforce를 계속 차단합니다. grace=0 우회는 운영 권고가 아닙니다.

삭제하지 않는 대상: live root, 백업 prefix 밖, pin/보존 세대, 방금 쓴 세대,
진행 중 backup/restore 참조(유예 내 미완료는 enforce 거부), 손상 세대,
활성 `refs/` 리더가 있는 세대, `RETIRING` 중인 세대(재계획이 아닌 재개 대상).

dry-run은 `resume <id>`와 `candidate <id>`를 구분하고, 공유 객체 정리도
`retention: pool-gc objects=N bytes=B`로 보고합니다. 요약의 `bytes`는 삭제
후보 세대의 자체 객체와 공유 pool에서 해제할 바이트의 합입니다.
중단된 retirement의 남은 바이트는 `resume` 행에 별도로 표시합니다.

## 상호 배제(backup/restore/retention)

- pool writer와 GC는 `<prefix>/_locks/<uuid>`의 고유 claim으로 직렬화합니다.
  claim 부재 LIST → 검증된 claim PUT → LIST 결과가 자신의 claim 하나인지
  재확인한 뒤에만 진행합니다. guard는 세대 COMPLETE 발행까지 유지합니다.
  이 절차와 reader handshake는 **강한 LIST/GET read-after-write 일관성**을
  전제로 하며 CAS나 TTL lease가 아닙니다. 충돌은 실패하고 활성 claim을
  탈취하지 않습니다. 강제 종료로 남은 claim은 소유 프로세스의 종료를
  독립적으로 확인한 뒤에만 수동 제거하고 dry-run을 다시 실행하세요.
- reader(원격 다운로드 또는 로컬 tar+원격 COMPLETE 대조)는 two-phase handshake로
  `<prefix>/<id>/refs/<uuid>` 리더 ref를 등록합니다: 사전 확인(COMPLETE 존재 +
  `RETIRING` 부재) → 고유 ref PUT → 사후 재확인(마커 + COMPLETE) 후 사용.
  재확인이 실패하면 방금 쓴 ref를 해제하고 거부하므로, 경쟁에서 이긴 writer가
  진행할 수 있고 reader는 retiring/retired 세대를 소비하지 않습니다.
  검증된 로컬 tar 확보 뒤에만 해제하며, 모든 실패 경로에서도 해제합니다.
- 로컬 tar 복원도 SST snapshot 검증·사용이 끝날 때까지 ref를 유지합니다(조기
  해제 없음). SST 충돌을 포함한 모든 S3 복원 오류 경로에서 ref를 해제합니다.
- 같은 prefix의 모든 backup/retention 작업이 이 claim 절차를 사용하는
  버전이어야 합니다. 실패·정상 종료 시 자기 claim만 제거합니다.
- 복원 대상은 세대 reader ref와 별도로
  `.datalake-restore-locks/<정규화 live root의 SHA256>/_locks/<uuid>`에서
  직렬화합니다. 동일 버킷·live root이면 backup prefix와 무관하게 충돌하며
  로컬 설치와 최종 rollback까지 claim을 유지합니다. 이 예약 경로는 SST와
  백업 inventory 밖에 둡니다. 강한 PUT/LIST 일관성이 필요하고, 강제 종료로
  남은 claim은 소유자가 종료됐음을 확인한 뒤 수동 회수합니다. 서로 겹치는
  별도 root나 이 프로토콜을 따르지 않는 DB writer는 보호 대상이 아닙니다.
  복원 계정에는 이 control 경로의 LIST/PUT/DELETE 권한이 필요합니다.
  claim 내용은 진단용이며 배타성은 고유 key의 존재로 판단하므로 이 PUT에는
  서버 checksum을 요구하지 않습니다. SST·archive의 기존 검증은 그대로입니다.
- retention 삭제(writer)도 two-phase입니다: 사전 확인(refs/`RETIRING` 부재,
  COMPLETE 존재) → `RETIRING` 마커 기록 → 마커 뒤 refs 사후 재확인 →
  `COMPLETE` 단독 삭제 → `COMPLETE` 부재 재확인 → payload 삭제(재확인) →
  `RETIRING` 제거. `COMPLETE`가 남아 있으면 payload를 건드리지 않습니다.
  마커 뒤 늦은 reader가 보이면 중단하고 marker+COMPLETE를 남깁니다(안전한 재개
  상태). 이 절차는 대상 스토리지의 GET/LIST read-after-write 강한 일관성을
  전제로 합니다. S3 호환이라는 이름만으로 보장하지 않으므로 공급자의 보장을
  확인하세요. 강제 TTL 회수나 활성 리스 탈취는 없습니다. 활성 ref가 있으면
  해당 세대는 삭제 후보에서 제외되고 삭제 직전 재확인에서도 거부됩니다.
- `RETIRING` 마커는 중단된 삭제의 durable identity입니다. `cmd_retention`은
  재계획이 아니라 이름으로 재개(`resume_retiring`)하며, ref가 있으면 재개도
  거부됩니다. 재개는 COMPLETE 단독 삭제 → 부재 확인 → payload 삭제 순서를
  지키며, COMPLETE 삭제 전에 payload를 multi-delete하지 않습니다. 부분 삭제
  오류(`DeleteObjects`의 `Errors`)는 그대로 실패로 보고되며 조용히 성공
  처리하지 않습니다.

An abruptly killed reader can leave a durable ref. Automatic expiry is
intentionally disabled: it could delete a generation still being read.
Before manually removing a stale ref, stop or independently confirm all
readers of that generation are gone; then rerun dry-run before enforcement.

pool claim이 남아서 `shared SST pool busy`가 나오면 같은 prefix를 사용하는
모든 backup/retention 실행이 종료됐음을 독립적으로 확인한 뒤에만 해당 claim을
수동 정리하세요. 살아 있는 작업의 claim을 제거하면 참조 보호가 깨질 수 있습니다.

## 공유 SST 정리

승인된 `enforce`만 세대 정리 후 pool GC를 수행합니다. 보존·pin·활성 reader가
있는 세대의 COMPLETE 목록 전체를 참조 집합으로 삼고, 어떤 남은 세대도
참조하지 않는 정규 SST와 검증된 기존 보조 `.json`만 삭제합니다. 알 수 없는
보조 record는 삭제하지 않고 GC를 차단합니다. backup과 같은 pool claim을 잡으므로
새 세대가 SST를 재사용·등록하는 도중에는 GC가 실행되지 않습니다.
복원은 기존 `refs/` handshake로 COMPLETE를 SST 사용 완료까지 보호합니다.

세대 삭제가 끝난 뒤 목록을 다시 읽어 GC를 재계획합니다. 남은 incomplete/
corrupt 세대가 있거나 pool에 예상하지 않은 key가 있으면 참조를 추측하지 않고
**pool 삭제를 차단**합니다(`pool-gc blocked`). `grace=0`도 이 보호를 끄지 않습니다.
세대 정리가 성공해도 이 경우 공유 객체는 남습니다. 부분 삭제 오류는 실패로
보고하며, 승인된 retention을 재실행하면 남은 미참조 객체를 다시 수집합니다.
실패한 백업은 자신의 세대 객체와 그 실행에서 새로 소유한 SST key만
되돌리며, 다른 세대가 재사용하는 객체는 지우지 않습니다. publication
guard 안에서 소유 key만 정리하고 남은 orphan는 승인된 GC가 회수합니다.


## 순서와 재시도

세대 정리는 `RETIRING` 마커 기록 → 마커 뒤 refs 사후 재확인 → `COMPLETE` 단독
삭제 → `COMPLETE` 부재 확인 뒤에만 payload 삭제 → `RETIRING` 제거 순입니다.
마커 뒤 늦은 reader가 보이면 `COMPLETE`를 건드리기 전에 중단하고
marker+COMPLETE를 남겨 재개가 안전하게 이어받습니다. 중단되면 `RETIRING`
prefix가 남아 restore가 선택하지 않고, 재실행이 이름 기준으로 나머지를
끝냅니다(멱등). 부분 삭제 뒤 재실행도 같은 순서로 완료됩니다. 새 세대 검증
전에 기존 복구 세대를 정리하지 않습니다. `backup` 뒤 후크 경고는 이미 검증된
백업을 실패로 바꾸지 않습니다.
## 검증 유지와 비용 범위

불변식: `[bucket, 정규화된 live root, 상대 key, size, 원본 ETag]` 배열을
`ensure_ascii=True`, `separators=(",", ":")`의 정규 JSON으로 인코딩한 뒤
SHA256으로 주소화한 `<prefix>/_sst/<64자리 hex>` 하나에 매핑합니다.
manifest의 `sst_snapshot.layout`과 COMPLETE의 `sst_layout`은 `shared-v1`,
`snap_prefix`는 `<prefix>/_sst/`, 각 SST의 전체 공유 key는 `snapshot_key`입니다.
이는 SST 내용의 SHA256이 아니라 **소스 identity 주소**입니다.
첫 백업은 각 live SST를 한 번씩 pool에 COPY하고, 동일한 두 번째 백업은
SST COPY/PUT 없이 bounded control bytes(tar + sidecar + manifest +
COMPLETE, 의미 있는 fixture에서 live SST bytes의 ≤.02)만 추가합니다.
같은 키의 변경된 소스는 새 pool entry가 됩니다. pool 객체는 live
compaction/GC와 무관하게 유지되며, 한 세대를 retire해도 다른 live/보존/
pin/참조/retiring 세대가 참조하는 pool 키는 절대 삭제하지 않습니다.
실패한 백업은 자신의 세대 객체와 그 실행에서 새로 소유한 SST key만
롤백합니다. 기존 재사용 객체는 지우지 않으며, guard가 경쟁 참조를 막는
동안만 소유 key를 정리합니다. 남은 orphan는 승인된 GC가 회수합니다.
기존 세대별 `sst/` snapshot과 SHA256 형식 백업은 그대로 복원됩니다.

새 per-SST `.json` record는 쓰지 않습니다. checksum 검증으로 발행한 COMPLETE
inventory가 원본·복사본 ETag/size와 pool 참조의 ledger이며, 재사용 경로는
guard당 한 번 로드해 각 pool HEAD와 대조합니다. LIST의 정규 pool data
키 자체로 unpublished orphan와 부분 삭제 뒤의 재시도 대상을 찾습니다.
세대 분류에서는 한 번 조회한 LIST로 만든 크기 인덱스를 모든 SST에
재사용합니다. 객체 누락과 0바이트 객체는 구분하며, 크기 불일치의
incomplete 판정은 유지합니다. 이 검사에 추가 S3 요청은 없습니다.
GC는 세대 retirement 뒤 생존 inventory를 다시 읽고 참조 없는 키만
삭제합니다. pin·reader ref·retiring 참조를 보존하며, 알 수 없는 pool 키,
읽을 수 없거나 손상·미완료인 inventory, 검증 불가 legacy 참조는 GC를
차단합니다. grace=0도 이 참조 검증을 생략하지 않습니다. 기존
`_sst/<identity>.json` control은 SST payload와 구분해 실제 요청·바이트를
모두 계수합니다. 유효한 소유 identity와 생존 inventory의 무참조가
증명된 보조 record만 회수하며, 알 수 없거나 손상된 record는 GC를 차단합니다.

백업 시 SST는 HEAD(`ChecksumMode="ENABLED"`) → 조건부 서버 측 COPY
(`CopySourceIfMatch` ETag precondition) → HEAD 크기·metadata·반환 identity
확인으로 처리하며 비교 가능한 서버 checksum도 대조합니다. Greptime이
쓴 primary SST에 서버 checksum이 없어도 조건부 소스 identity COPY는
허용합니다. 이는 내용 digest 증명이 아닙니다. ETag는 identity 토큰이며
multipart ETag를 전체 바이트 hash로 해석하지 않습니다.

도구가 쓰는 RAW·CAN archive·control 객체는 명시적 `ChecksumSHA256`
PUT과 응답 echo·HEAD checksum 일치를 요구합니다. 부재·불일치·미지원은
실패하며 로컬 파일을 보존합니다. metadata-only 성공이나 검증용 전체
GET fallback은 없습니다. `sha256` metadata만으로 원격 바이트를 검증했다고
보지 않습니다. IDrive/B2의 이 checksum 계약 호환성은 주장하지 않습니다.

백업 검증용 SST body GET이나 archive 재다운로드는 하지 않습니다.
새 형식의 SST 복원은 HEAD/COPY를 사용하고, legacy SHA256 SST와 필요한
archive 다운로드는 기존 hash 검증을 유지합니다. benchmark는 별도로
보관한 **원본 bytes의 독립 SHA256 oracle**과 빈 대상의 복원 결과를
비교합니다. 이 검사용 GET은 백업 비용 계수나 런타임 scrub이 아닙니다.

세대마다 SST 전체를 복사하는 독립 full snapshot 구조가 아닙니다. 공유
pool + 참조 기반 GC가 누적 저장 바이트를 줄이고, retention은 세대
control 객체를 줄입니다. 세대 디렉터리에는 tar/sidecar/manifest/
COMPLETE/refs와 retirement 중의 RETIRING이 있습니다. 세대 dict의
`pool_keys`는 `snapshot_key`의 pool 상대 suffix이며, `pool_bytes`와
`control_bytes`는 공유 SST와 세대 자체 바이트를 구분합니다. WAL·설정
tar는 여전히 세대별 full archive이며 DB offline 조건도 유지합니다.
compaction으로 source identity가 바뀌면 새 객체로 취급합니다. 기존 full
snapshot을 자동으로 pool로 옮기거나 삭제하지 않으며 RAW의 manifest
content-addressed 경로도 바꾸지 않습니다.

## 운영 정책

- 백업 prefix에 만료 lifecycle을 설정하지 마세요. versioned bucket의 noncurrent
  versions/delete markers, 미완료 multipart는 별도 운영 정책이 필요하며, 이 도구는
  세대 객체와 미참조 shared pool 객체만 명시적으로 삭제합니다.
- lifecycle 설정과 충돌 시 lifecycle이 이 도구의 삭제를 앞당기거나 복구 세대를
  깨지 않도록 prefix를 분리하고, 운영 삭제 전 dry-run 목록/예상 bytes를 검토하세요.
- `_sst/`와 `_locks/`는 도구 전용 namespace입니다. 공유 SST를 세대 폴더와 함께
  일괄 삭제하거나 pool/claim에 임의 만료 규칙을 적용하지 마세요.
- 복구: 한 세대 retire가 다른 세대를 깨지 않습니다. 공유 pool 키는 모든
  live 세대가 참조를 잃을 때만 삭제되며, pin/보존/참조/retiring 세대의 키는
  항상 유지됩니다. 중단된 retire는 `RETIRING` 마커로 이름 기준 재개하며, 세대
  삭제 뒤 생존 inventory를 다시 읽어 pool GC를 계획합니다.

## 격리 실측

`tools/benchmark_backup_retention.py --baseline-root /path/to/pristine-checkout`
은 합성 File-store/SST fixture를 사용합니다. `--endpoint`, `--bucket`,
`--user`, `--password-env`를 지정하면 같은 경로를 실제 S3 호환 서버에서
실행합니다. 전용 bucket/prefix만 사용하세요.

최적화 후 결정적 합성 비교는 공개 기준 `75bed47` 대비 절대·상대 비용
게이트를 모두 통과했습니다. 월간 전망은 정책의 backup+RAW workload와
단가 가정에 따른 **범위 한정 모델**이며 실제 요금·계정 전체 사용량이
아닙니다. 소프트웨어 fixture와 원본 hash 복원 oracle의 결과이지 운영
bucket·공급자 호환성·운영 배포 검증은 아닙니다.

아래는 **HEAD 전환 전** 로컬 MinIO에서 측정한 보존 정책 검증 기록입니다.
현재 구현의 요청 수나 검증 읽기 bytes를 나타내지 않습니다.

| 입력 | LIST | GET | COPY | PUT | 검증 읽기 bytes (변경 구현) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 동일 데이터 | 2 | 8 | 2 | 4 | 12,098 |
| 동일 데이터 재백업 | 2 | 8 | 2 | 4 | 12,105 |
| 소량 변경 | 2 | 8 | 2 | 4 | 15,251 |
| 대량 변경 | 2 | 108 | 52 | 4 | 441,058 |

보존 3세대와 중간 세대 pin 정책에서 dry-run 예상 삭제 9,538 bytes가
실제 감소량과 일치했습니다(366,848 → 357,310 bytes). 정리 후 최신,
가장 오래된 보존 세대, pin 세대를 각각 빈 격리 디렉터리와 live prefix로
복원했고 모든 파일 및 SST hash가 일치했습니다. 운영 데이터나 운영
복구 가능성을 검증한 결과는 아닙니다.

## 재개 회귀 smoke(격리 fake, 운영 데이터 없음)

전체 검사는 `python3 -m tests.test_backup`입니다. 재개 경계만 별도 확인용:

```sh
python3 - <<'EOF'
import tests.test_backup as t
t.test_retirement_interrupt_resumes_and_partial_delete_surfaces()
t.test_retiring_identity_mismatch_and_active_reader_refuse_resume()
t.test_retention_dryrun_matches_enforce_selection()
t.test_retention_refuses_active_reader_and_concurrent_backup()
print("retention resume smoke: ok")
EOF
```

## 이슈 #6 재검증 기록(공유 pool 전환 전)

기존 구현을 변경하지 않고 `python3 -m tests.test_backup`과
`python3 tools/benchmark_backup_retention.py --baseline-root <현재 변경 전 checkout>`
을 실행했습니다. 격리된 counting-fake에서 동일 데이터 2회·소량 변경·대량
변경의 백업을 만들고, 최신·가장 오래된 보존·pin 세대를 빈 대상에 복원해
파일과 SST SHA-256이 모두 일치했습니다. dry-run의 삭제 예상 9,474 bytes는
실제 저장량 감소(366,524 → 357,050 bytes)와 같았습니다.
이는 기존 보존 구현의 재검증이며 새로운 복사 I/O 개선이나 실제 S3 공급자,
운영 bucket 삭제·복구 검증이 아닙니다.
