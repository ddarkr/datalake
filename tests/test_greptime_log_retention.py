#!/usr/bin/env python3
"""Exercise retention against real files, links and an open log."""
import os
from pathlib import Path
import tempfile
import time

from scripts.storage import greptime_log_retention as retention


def main():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "logs"
        root.mkdir()
        now = time.time()
        old = now - retention.RETENTION_SECONDS - 1
        files = {
            "expired": root / "greptimedb.2026-10-01-01",
            "open": root / "greptimedb-err.2026-10-01-02",
            "recent": root / "greptimedb.2026-10-29-01",
            "other": root / "other.2026-10-01-01",
            "malformed": root / "greptimedb.2026-10-01-01.backup",
        }
        for path in files.values():
            path.write_text(path.name)
            os.utime(path, (old, old))
        recent = now - retention.RETENTION_SECONDS + 1
        os.utime(files["recent"], (recent, recent))
        outside = Path(tmp) / "outside"
        outside.write_text("keep")
        link = root / "greptimedb.2026-10-01-03"
        link.symlink_to(outside)
        nested = root / "nested"
        nested.mkdir()
        (nested / "greptimedb.2026-10-01-04").write_text("keep")

        with files["open"].open() as active:
            info = os.fstat(active.fileno())
            if Path("/proc/self/fd").is_dir():
                assert (info.st_dev, info.st_ino) in retention.open_files(os.getpid())
            result = retention.prune_logs(root, {(info.st_dev, info.st_ino)}, now)
            assert result["removed"] == 1 and result["skipped_open"] == 1
            assert not files["expired"].exists() and files["open"].exists()
            assert all(files[name].exists() for name in ("recent", "other", "malformed"))
            assert link.is_symlink() and outside.read_text() == "keep"
            assert (nested / "greptimedb.2026-10-01-04").read_text() == "keep"
        assert retention.prune_logs(root, set(), now)["removed"] == 1
        assert not files["open"].exists()
        directory_link = Path(tmp) / "logs-link"
        directory_link.symlink_to(root, target_is_directory=True)
        try:
            retention.prune_logs(directory_link, set(), now)
        except OSError:
            pass
        else:
            raise AssertionError("symlink log directory accepted")
    print("Greptime log retention: age, open inode, names, symlinks and directories passed")


if __name__ == "__main__":
    main()
