#!/usr/bin/env python3
"""Static .env parity check: every ${VAR} in compose/*.yaml needs a KEY in .env.example.

Static only (regex over fragments, key list over .env.example). Ground truth
is the real compose model — needs docker, so the parent runs it:
  docker compose config 2>&1 | grep -i warn   # must be empty with .env loaded
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-?[^}]*)?\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def compose_vars():
    found = {}
    for path in sorted((ROOT / "compose").glob("*.yaml")):
        text = path.read_text(encoding="utf-8")
        for m in REF_RE.finditer(text):
            var = m.group(1) or m.group(2)
            # $$ is an escaped literal dollar, not a ref (script text).
            if m.start() > 0 and text[m.start() - 1] == "$":
                continue
            found.setdefault(var, []).append(path.name)
    return found


def env_keys():
    keys = set()
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*[:=]", line)
        if m:
            keys.add(m.group(1))
    return keys


def main():
    vars_ = compose_vars()
    keys = env_keys()
    missing = {v: files for v, files in vars_.items() if v not in keys}
    # $$ escape produces lone "$" hits (e.g. "$," in scripts); not real refs.
    missing.pop("$", None)
    if missing:
        for var, files in sorted(missing.items()):
            print(f"missing .env.example key: {var} (used in {', '.join(files)})")
        raise SystemExit(1)
    print(f"parity ok: {len(vars_)} compose var(s) all present in .env.example")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 — fail closed with the reason
        print(f"check_env: error: {e}", file=sys.stderr)
        raise SystemExit(2)
