"""기존 폴더를 보존하는 중첩 폴더 전환 검사."""
import copy
import unittest

from scripts.database.grafana_folders import FolderError, reconcile


def resource(uid, title, parent=""):
    return {"metadata": {"name": uid, "resourceVersion": "7",
                         "annotations": {"grafana.app/folder": parent, "user-note": "보존"}},
            "spec": {"title": title}}


class FolderMigration(unittest.TestCase):
    def test_existing_root_and_unrelated_folders_survive_repeated_migration(self):
        stored = {"legacy-root": resource("legacy-root", "Datalake"),
                  "personal": resource("personal", "개인 기록"),
                  "datalake-ai": resource("datalake-ai", "이전 제목"),
                  "datalake-drives": resource("datalake-drives", "주행", "personal")}
        personal = copy.deepcopy(stored["personal"])
        writes = []

        def request(method, path, body=None, missing_ok=False):
            if method == "GET" and "?" in path:
                # 기존 root가 두 번째 페이지에 있어도 새 root를 만들면 안 됩니다.
                if "continue=next" in path:
                    return {"items": [copy.deepcopy(stored["legacy-root"])]}
                return {"items": [copy.deepcopy(stored["personal"])],
                        "metadata": {"continue": "next"}}
            uid = path.rsplit("/", 1)[1] if method != "POST" else body["metadata"]["name"]
            if method == "GET":
                return copy.deepcopy(stored.get(uid))
            writes.append((method, uid))
            if method == "PUT":
                self.assertEqual(body["metadata"]["resourceVersion"], "7")
            stored[uid] = copy.deepcopy(body)
            return copy.deepcopy(body)

        self.assertEqual(reconcile(request), "legacy-root")
        self.assertEqual(stored["personal"], personal)
        self.assertEqual(stored["datalake-ai"]["metadata"]["annotations"]["user-note"], "보존")
        self.assertEqual(stored["datalake-drives"]["metadata"]["annotations"]["grafana.app/folder"],
                         "datalake-vehicle")
        self.assertEqual(stored["datalake-ai"]["metadata"]["annotations"]["grafana.app/folder"],
                         "legacy-root")
        self.assertNotIn("datalake-root", stored)
        writes.clear()
        self.assertEqual(reconcile(request), "legacy-root")
        self.assertEqual(writes, [])

    def test_ambiguous_root_fails_without_changing_folders(self):
        def request(method, path, body=None, missing_ok=False):
            self.assertEqual(method, "GET")
            return {"items": [resource("one", "Datalake"), resource("two", "Datalake")]}

        with self.assertRaises(FolderError):
            reconcile(request)


if __name__ == "__main__":
    unittest.main()
