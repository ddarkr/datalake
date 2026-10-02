# 개발과 검증

[프로젝트 소개](../README.md) · [설치와 운영](operations.md)

아래 명령은 저장소 최상위 디렉터리에서 실행합니다.


| 경로 | 역할 |
| --- | --- |
| [`compose/`](../compose/) | 서비스·수집기·대시보드 원본 조각 |
| [`scripts/`](../scripts/) | 초기화, 집계, 차량 기록, 백업·복원 구현 |
| [`plugins/`](../plugins/) | 코딩 에이전트 연결과 설치기 |
| [`tools/render.py`](../tools/render.py) | 원본을 단일 배포 파일로 생성 |
| [`tools/check_env.py`](../tools/check_env.py) | Compose 환경변수와 예제의 일치 검사 |
| [`tests/`](../tests/) | 동작·보안·스토리지·렌더링 검증 |
| [`.github/workflows/compose.yml`](../.github/workflows/compose.yml) | push 시 플러그인·렌더러·생성물·프로필 검사 |

`compose.yaml`은 생성물입니다. 직접 편집하지 말고 `compose/*.yaml`과 `scripts/*`를 수정한 뒤 아래 **명시적 순서**로 재생성하세요. 기본 glob 순서는 CI의 생성 순서와 다릅니다.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install PyYAML==6.0.2

python tools/render.py \
  compose/core.yaml compose/database.yaml compose/ingest.yaml \
  compose/grafana.yaml compose/backup.yaml compose/vehicle-raw.yaml \
  compose/vehicle-vss.yaml compose/tesla-fleet.yaml compose/redecode.yaml \
  --out compose.yaml

python tools/check_env.py
python tests/test_render.py

for profile in server home mqtt vehicle fleet backup redecode '*'; do
  docker compose --env-file .env.example --profile server --profile "$profile" config --quiet
done
```

플러그인을 변경했다면 Node.js 22 이상에서 다음도 실행합니다.

```bash
npm ci --prefix plugins --ignore-scripts --no-audit --no-fund
node --test tests/test_plugin_*.mjs
```

`.env.example`을 사용한 프로필 검사는 실제 자격 증명 없이 수행하는 구조 검사입니다. 실제 S3 연결, 물리 CAN, 운영 서버 배포·데이터 수집을 검증하지 않습니다. 변경한 기능의 런타임 검증은 별도로 수행하세요.


Hermes 플러그인을 변경했다면 별도 Python 테스트도 실행합니다. native 호환성 검증에 필요한 외부 체크아웃과 제약은 [Hermes 문서](../plugins/hermes/README.md#tests)를 참고하세요.

```bash
python3 -m unittest discover -s plugins/hermes/tests -v
```
