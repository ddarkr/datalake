#!/usr/bin/env python3
"""Merge compose/*.yaml fragments into one self-contained compose.yaml.

Fragment contract: top-level keys may only be services/configs/volumes.
A configs entry may carry ``x-source: scripts/domain/module.py`` (repo-root
relative) instead of ``content:``; the renderer inlines the file bytes,
escapes every ``$`` as ``$$`` (so Compose never interpolates script
text), and drops ``x-source``. Inline ``configs.content`` is passed
through untouched — authors write ``$$`` there for a literal ``$``.
x-source must stay inside the repo root (escape fails the render).

Duplicate service/config/volume names across fragments fail the render.
The output must stay deployment self-contained: service ``build:``,
``env_file:``, ``secrets:``, configs ``file:``, host bind mounts,
anonymous volumes, and absolute host config/env/secret refs fail the
render. Content renders as multiline literal blocks for readability.
Dev-only dependency: PyYAML. Never needed on the deploy host.
"""

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TOP_KEYS = ("services", "configs", "volumes", "networks")


class RenderError(Exception):
    pass


def escape_script(text):
    return text.replace("$", "$$")


class RenderDumper(yaml.SafeDumper):
    pass


def _str_representer(dumper, data):
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


RenderDumper.add_representer(str, _str_representer)


def load_fragment(path):
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    unknown = set(doc) - set(TOP_KEYS)
    if unknown:
        raise RenderError(f"{path}: unexpected top-level keys: {sorted(unknown)}")
    for name, cfg in (doc.get("configs") or {}).items():
        if not isinstance(cfg, dict):
            continue
        src = cfg.pop("x-source", None)
        if src is None:
            continue
        if "content" in cfg:
            raise RenderError(f"{path}: config {name}: x-source with content")
        if "file" in cfg:
            raise RenderError(f"{path}: config {name}: file: refs are banned")
        target = (ROOT / src).resolve()
        if target != ROOT.resolve() and ROOT.resolve() not in target.parents:
            raise RenderError(f"{path}: config {name}: x-source escapes repo root: {src!r}")
        text = target.read_text(encoding="utf-8")
        cfg["content"] = escape_script(text)
    return doc


def merge_docs(docs):
    merged = {}
    for doc in docs:
        for key in TOP_KEYS:
            section = doc.get(key) or {}
            target = merged.setdefault(key, {})
            for name, value in section.items():
                if name in target:
                    raise RenderError(f"duplicate {key}.{name}")
                target[name] = value
    return merged


def _iter_strings(node):
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for v in node.values():
            yield from _iter_strings(v)
    elif isinstance(node, list):
        for v in node:
            yield from _iter_strings(v)


def check_final(doc):
    for name, svc in (doc.get("services") or {}).items():
        if not isinstance(svc, dict):
            raise RenderError(f"service {name}: not a mapping")
        if "image" not in svc:
            raise RenderError(f"service {name}: missing image (build: is banned)")
        for banned in ("build", "env_file", "secrets"):
            if banned in svc:
                raise RenderError(f"service {name}: {banned}: is banned")
        for text in _iter_strings(svc):
            if text.startswith(("/run/secrets/", "/var/run/secrets/")):
                raise RenderError(f"service {name}: absolute secret ref banned: {text!r}")
        for vol in svc.get("volumes") or []:
            if isinstance(vol, str):
                head = vol.split(":")[0] if ":" in vol else vol
                if ":" not in vol or head.startswith(("./", "../", "/")):
                    raise RenderError(f"service {name}: bind/anonymous mount banned: {vol!r}")
            elif isinstance(vol, dict):
                if vol.get("type", "volume") != "volume":
                    raise RenderError(f"service {name}: bind/anonymous mount banned: {vol!r}")
    for name, cfg in (doc.get("configs") or {}).items():
        if isinstance(cfg, dict):
            if "file" in cfg:
                raise RenderError(f"config {name}: file: is banned")
            if "x-source" in cfg:
                raise RenderError(f"config {name}: un-inlined x-source")


def render_to_text(doc):
    return yaml.dump(doc, Dumper=RenderDumper, sort_keys=False,
                     allow_unicode=True, width=4096)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("fragments", nargs="*")
    ap.add_argument("--out", default=str(ROOT / "compose.yaml"))
    args = ap.parse_args(argv)
    paths = [Path(p) for p in args.fragments] if args.fragments \
        else sorted((ROOT / "compose").glob("*.yaml"))
    if not paths:
        raise RenderError("no fragments found")
    docs = [load_fragment(p) for p in paths]
    merged = merge_docs(docs)
    check_final(merged)
    header = ("# GENERATED by tools/render.py — do not edit. "
              "Edit compose/*.yaml + scripts/**/*.py instead.\n")
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(header + render_to_text(merged))
    print(f"rendered {len(paths)} fragment(s) -> {args.out}")


if __name__ == "__main__":
    try:
        main()
    except RenderError as e:
        print(f"render: error: {e}", file=sys.stderr)
        raise SystemExit(1)
