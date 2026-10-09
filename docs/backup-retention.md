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

삭제하지 않는 대상: live root, 백업 prefix 밖, pin/보존 세대, 방금 쓴 세대,
진행 중 backup/restore 참조(유예 내 미완료는 enforce 거부), 손상 세대,
활성 `refs/` 리더가 있는 세대, `RETIRING` 중인 세대(재계획이 아닌 재개 대상).

## 상호 배제(backup/restore/retention)

- reader(원격 다운로드 또는 로컬 tar+원격 COMPLETE 대조)는 two-phase handshake로
  `<prefix>/<id>/refs/<uuid>` 리더 ref를 등록합니다: 사전 확인(COMPLETE 존재 +
  `RETIRING` 부재) → 고유 ref PUT → 사후 재확인(마커 + COMPLETE) 후 사용.
  재확인이 실패하면 방금 쓴 ref를 해제하고 거부하므로, 경쟁에서 이긴 writer가
  진행할 수 있고 reader는 retiring/retired 세대를 소비하지 않습니다.
  검증된 로컬 tar 확보 뒤에만 해제하며, 모든 실패 경로에서도 해제합니다.
- 로컬 tar 복원도 SST snapshot 검증·사용이 끝날 때까지 ref를 유지합니다(조기
  해제 없음). SST 충돌을 포함한 모든 S3 복원 오류 경로에서 ref를 해제합니다.
- backup 업로드 검증 경로도 같은 ref를 사용합니다(삭제 중 세대 읽기 금지).
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

## 순서와 재시도

세대 정리는 `RETIRING` 마커 기록 → 마커 뒤 refs 사후 재확인 → `COMPLETE` 단독
삭제 → `COMPLETE` 부재 확인 뒤에만 payload 삭제 → `RETIRING` 제거 순입니다.
마커 뒤 늦은 reader가 보이면 `COMPLETE`를 건드리기 전에 중단하고
marker+COMPLETE를 남겨 재개가 안전하게 이어받습니다. 중단되면 `RETIRING`
prefix가 남아 restore가 선택하지 않고, 재실행이 이름 기준으로 나머지를
끝냅니다(멱등). 부분 삭제 뒤 재실행도 같은 순서로 완료됩니다. 새 세대 검증
전에 기존 복구 세대를 정리하지 않습니다. `backup` 뒤 후크 경고는 이미 검증된
백업을 실패로 바꾸지 않습니다.
## 검증 유지

복사·업로드 검증은 스트리밍 SHA-256이며 ETag를 비교하지 않습니다.
세대마다 SST 전체를 복사하는 독립 full snapshot이며, 공유 SST/content-addressed
최적화를 도입하지 않습니다.

## 운영 정책

- 백업 prefix에 만료 lifecycle을 설정하지 마세요. versioned bucket의 noncurrent
  versions/delete markers, 미완료 multipart는 별도 운영 정책이 필요하며, 이 도구는
  세대 객체만 명시적으로 삭제합니다.
- lifecycle 설정과 충돌 시 lifecycle이 이 도구의 삭제를 앞당기거나 복구 세대를
  깨지 않도록 prefix를 분리하고, 운영 삭제 전 dry-run 목록/예상 bytes를 검토하세요.
- `shared-object` 방식은 도입하지 않았으므로 해당 완료 기준은 not applicable입니다.

## 격리 실측

`tools/benchmark_backup_retention.py --baseline-root /path/to/pristine-checkout`
은 합성 File-store/SST fixture를 사용합니다. `--endpoint`, `--bucket`,
`--user`, `--password-env`를 지정하면 같은 경로를 실제 S3 호환 서버에서
실행합니다. 전용 bucket/prefix만 사용하세요.

로컬 MinIO에서 원본과 변경 구현 모두 세대별 full snapshot의 요청 수는
같았습니다. 중복 제거 또는 복사 비용 개선을 주장하지 않습니다.

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
