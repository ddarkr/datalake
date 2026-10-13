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
같은 경로의 `.json`에 크기·원본/복사본 ETag를 기록합니다. 각 세대의 manifest와
COMPLETE는 **해당 시점의 전체 SST 목록**과 공유 객체 key를 담습니다.
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

- reader(원격 다운로드 또는 로컬 tar+원격 COMPLETE 대조)는 two-phase handshake로
  `<prefix>/<id>/refs/<uuid>` 리더 ref를 등록합니다: 사전 확인(COMPLETE 존재 +
  `RETIRING` 부재) → 고유 ref PUT → 사후 재확인(마커 + COMPLETE) 후 사용.
  재확인이 실패하면 방금 쓴 ref를 해제하고 거부하므로, 경쟁에서 이긴 writer가
  진행할 수 있고 reader는 retiring/retired 세대를 소비하지 않습니다.
  검증된 로컬 tar 확보 뒤에만 해제하며, 모든 실패 경로에서도 해제합니다.
- 로컬 tar 복원도 SST snapshot 검증·사용이 끝날 때까지 ref를 유지합니다(조기
  해제 없음). SST 충돌을 포함한 모든 S3 복원 오류 경로에서 ref를 해제합니다.
- backup 생성과 retention 계획·삭제는 `<prefix>/_locks/<고유 token>` claim으로
  직렬화합니다. claim 부재 사전 확인 → 고유 claim PUT → 자기 claim만 있는지
  LIST 사후 확인을 통과한 작업만 pool을 사용합니다. 늦게 들어온 contender는
  기존 claim을 보고 거부하며, 실패·정상 종료 시 자기 claim만 제거합니다.
  프로세스가 강제 종료되면 claim이 남습니다. TTL이나 자동 탈취는 없습니다.
  같은 prefix의 모든 backup/retention 작업이 이 절차를 사용하는 버전이어야 합니다.
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
참조하지 않는 SST와 `.json`만 삭제합니다. backup과 같은 pool claim을 잡으므로
새 세대가 SST를 재사용·등록하는 도중에는 GC가 실행되지 않습니다.
복원은 기존 `refs/` handshake로 COMPLETE를 SST 사용 완료까지 보호합니다.

세대 삭제가 끝난 뒤 목록을 다시 읽어 GC를 재계획합니다. 남은 incomplete/
corrupt 세대가 있거나 pool에 예상하지 않은 key가 있으면 참조를 추측하지 않고
**pool 삭제를 차단**합니다(`pool-gc blocked`). `grace=0`도 이 보호를 끄지 않습니다.
세대 정리가 성공해도 이 경우 공유 객체는 남습니다. 부분 삭제 오류는 실패로
보고하며, 승인된 retention을 재실행하면 남은 미참조 객체를 다시 수집합니다.
실패한 백업은 그 실행에서 새로 쓴 pool 객체와 세대 객체만 되돌리며, 다른
세대가 재사용하는 객체는 지우지 않습니다.


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

새 SST만 HEAD → 조건부 서버 측 COPY → HEAD 크기·metadata 확인으로
처리하고, 기존 identity는 작은 `.json` 기록과 HEAD로 재사용합니다.
원본·복사본의 전체 SHA256 GET과 archive 업로드 후 재다운로드는
하지 않습니다. 업로드 객체는 로컬 SHA256을 metadata로 기록하고 HEAD에서
크기와 함께 확인합니다. metadata는 원격 바이트 checksum이 아니며, 동일 크기의
비트 손상까지 검출하는 종전 full GET 검증과 동등하지 않습니다.

새 SST manifest/COMPLETE에는 전체 목록, 공유 key, 크기와 원본·복사본 ETag를
기록합니다. ETag는 identity/precondition 토큰이며, multipart ETag를 전체 바이트
hash로 취급하지 않습니다. 새 백업 복원도 SST body GET 없이 HEAD/COPY를 사용합니다.
기존 SHA256 형식의 백업은 종전 복원 검증을 유지하며, archive 다운로드 시
SHA256 검증도 유지합니다.

[IDrive e2의 API 문서](https://www.idrive.com/s3-storage-e2/s3-compatible-api)는
HEAD와 COPY를 설명하지만 `ChecksumSHA256`의 PUT/COPY/HEAD 동작은 확인하지
못했습니다. 실제 공급자 검증 없이 checksum 지원을 가정하지 않습니다.
표본검사·주기적 scrub은 자동으로 실행하지 않습니다.

첫 증분 백업은 pool에 아직 없는 SST 전체를 서버 측 COPY합니다. 이후에는
bucket·live root/key·크기·원본 ETag가 같은 객체를 재사용하고, 신규 identity만
복사합니다. 이 identity 목록을 SHA256으로 주소화하지만 **SST 바이트를
SHA256으로 content-addressing한 것은 아닙니다**. SST hash를 얻기 위한
전체 GET은 추가하지 않습니다.

WAL·설정 tar는 여전히 세대별 full archive이며, DB offline 조건도 유지합니다.
compaction으로 SST가 교체되면 새 객체로 취급하므로 실제 절감 폭은 SST 교체율에
따라 달라집니다. 기존 full snapshot을 pool로 옮기거나 삭제하는 자동 마이그레이션은
없습니다. RAW 업로드는 원래 manifest content-addressed 경로를 사용하며 별도입니다.

## 운영 정책

- 백업 prefix에 만료 lifecycle을 설정하지 마세요. versioned bucket의 noncurrent
  versions/delete markers, 미완료 multipart는 별도 운영 정책이 필요하며, 이 도구는
  세대 객체와 미참조 shared pool 객체만 명시적으로 삭제합니다.
- lifecycle 설정과 충돌 시 lifecycle이 이 도구의 삭제를 앞당기거나 복구 세대를
  깨지 않도록 prefix를 분리하고, 운영 삭제 전 dry-run 목록/예상 bytes를 검토하세요.
- `_sst/`와 `_locks/`는 도구 전용 namespace입니다. 공유 SST를 세대 폴더와 함께
  일괄 삭제하거나 pool/claim에 임의 만료 규칙을 적용하지 마세요.

## 격리 실측

`tools/benchmark_backup_retention.py --baseline-root /path/to/pristine-checkout`
은 합성 File-store/SST fixture를 사용합니다. `--endpoint`, `--bucket`,
`--user`, `--password-env`를 지정하면 같은 경로를 실제 S3 호환 서버에서
실행합니다. 전용 bucket/prefix만 사용하세요.

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

`python3 -m tests.test_backup`는 부모가 실행합니다. 재개 경계만 별도 확인용:

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

## 이슈 #6 재검증

기존 구현을 변경하지 않고 `python3 -m tests.test_backup`과
`python3 tools/benchmark_backup_retention.py --baseline-root <현재 변경 전 checkout>`
을 실행했습니다. 격리된 counting-fake에서 동일 데이터 2회·소량 변경·대량
변경의 백업을 만들고, 최신·가장 오래된 보존·pin 세대를 빈 대상에 복원해
파일과 SST SHA-256이 모두 일치했습니다. dry-run의 삭제 예상 9,474 bytes는
실제 저장량 감소(366,524 → 357,050 bytes)와 같았습니다.
이는 기존 보존 구현의 재검증이며 새로운 복사 I/O 개선이나 실제 S3 공급자,
운영 bucket 삭제·복구 검증이 아닙니다.
