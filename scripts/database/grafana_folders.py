"""파일로 공급한 Grafana 대시보드 폴더를 Datalake 아래에 정리합니다."""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


# 파일 provisioning과 같은 UID를 사용해 폴더 이동 뒤에도 대시보드 위치를 유지합니다.
FOLDERS = (
    ("datalake-ai", "AI", ""),
    ("datalake-home", "홈", ""),
    ("datalake-vehicle", "차량", ""),
    ("datalake-drives", "주행", "datalake-vehicle"),
    ("datalake-charging", "충전", "datalake-vehicle"),
    ("datalake-battery", "배터리", "datalake-vehicle"),
    ("datalake-can", "CAN 신호", "datalake-vehicle"),
    ("datalake-operations", "운영", ""),
)
# Grafana 12.4.0이 제공하는 폴더 리소스 버전입니다.
API = "/apis/folder.grafana.app/v1beta1/namespaces/default/folders"


class FolderError(RuntimeError):
    pass


def reconcile(request):
    # 기존 Datalake UID를 재사용해 기존 링크·권한·사용자 대시보드를 보존합니다.
    roots = []
    continuation = ""
    while True:
        page = request("GET", API + "?" + urllib.parse.urlencode(
            {"limit": 1000, "continue": continuation}))
        roots.extend(item for item in page["items"]
                     if item["spec"]["title"] == "Datalake"
                     and not item["metadata"].get("annotations", {}).get("grafana.app/folder"))
        continuation = page.get("metadata", {}).get("continue", "")
        if not continuation:
            break
    if len(roots) > 1:
        raise FolderError("Datalake 최상위 폴더가 여러 개입니다")
    if roots:
        root_uid = roots[0]["metadata"]["name"]
    else:
        root = request("POST", API, {
            "metadata": {"name": "datalake-root"}, "spec": {"title": "Datalake"}})
        root_uid = root["metadata"]["name"]
    for uid, title, parent in FOLDERS:
        parent = parent or root_uid
        path = API + "/" + uid
        folder = request("GET", path, missing_ok=True)
        if folder is None:
            request("POST", API, {
                "metadata": {"name": uid, "annotations": {"grafana.app/folder": parent}},
                "spec": {"title": title}})
            continue
        annotations = folder["metadata"].setdefault("annotations", {})
        if folder["spec"]["title"] != title or annotations.get("grafana.app/folder") != parent:
            # resourceVersion를 포함한 기존 리소스를 갱신합니다. 권한은 별도 API에 보존됩니다.
            folder["spec"]["title"] = title
            annotations["grafana.app/folder"] = parent
            request("PUT", path, folder)
    return root_uid


def main():
    url = os.environ.get("GRAFANA_URL", "http://grafana:3000").rstrip("/")
    user = os.environ.get("GF_ADMIN_USER", "admin")
    password = os.environ.get("GF_ADMIN_PASSWORD", "")
    if not password:
        raise FolderError("GF_ADMIN_PASSWORD가 필요합니다")
    auth = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()

    def request(method, path, body=None, missing_ok=False):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url + path, data=data, method=method,
                                     headers={"Authorization": auth, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if missing_ok and exc.code == 404:
                return None
            if path == "/api/health" and exc.code == 503:
                return {"database": "starting"}
            raise FolderError("Grafana 폴더 API 응답: HTTP " + str(exc.code)) from None

    deadline = time.monotonic() + 120
    while True:
        try:
            if request("GET", "/api/health").get("database") == "ok":
                break
        except urllib.error.URLError:
            pass
        if time.monotonic() >= deadline:
            raise FolderError("Grafana 준비 대기 시간이 초과됐습니다")
        time.sleep(1)
    reconcile(request)
    print("Datalake 대시보드 폴더 정리 완료")


if __name__ == "__main__":
    try:
        main()
    except (FolderError, urllib.error.URLError, TimeoutError) as exc:
        # 인증 정보나 서버 응답 본문을 출력하지 않습니다.
        message = str(exc) if isinstance(exc, FolderError) else "Grafana 연결 실패"
        print(message, file=sys.stderr)
        sys.exit(1)
