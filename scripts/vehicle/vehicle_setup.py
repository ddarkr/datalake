#!/usr/bin/env python3
"""One-shot vehicle DBC/VSS setup (stdlib only).

Downloads pinned DBC + VSS-mapping artifacts, verifies SHA256, and publishes
them plus a full-provenance manifest into the shared vehicle volume.

Supplemental policy: there is no DBC patch/merge step. The supplemental DBC
is used verbatim as downloaded. When DBC_OVERRIDE_URL is configured, the
override artifact IS the supplemental (complete replacement, used verbatim).
Rationale: a regex BO_-block merge cannot carry VAL_/CM_/BA_ attribute
metadata or multiplex variants safely, so merging is refused by design --
publish a complete supplemental (or override) DBC instead.

Layout (atomic): each decode epoch is a generation directory
  <data>/generations/<epoch>/{dbc/primary.dbc,dbc/supplemental.dbc,
  mapping/vss_dbc.json,manifest.json}
plus a single symlink <data>/current -> generations/<epoch>. Publish stages
a temp dir, renames it into place, then swaps the symlink with one
os.replace -- a mid-run failure leaves the previous generation and the
previous current pointer untouched. Consumers read via /data/current/... .
Per-epoch manifests persist, so a reused epoch with different inputs
collides even after other epochs shipped in between (A->B->A fails).

VSS mapping shape: official upstream VSS JSON nests branches under
"children" (Vehicle.children.Speed, ...); iter_nodes/find_mappings walk
through "children" transparently so paths never contain a "children"
segment. Flat overlay-style trees (no "children" key) keep working.

Provenance: DBC artifacts require full upstream commits as well as content
hashes. Local mapping/overlay artifacts may use versioned content hashes
without claiming an upstream Git commit.

Required env: DBC_PRIMARY_URL, DBC_PRIMARY_SHA256, DBC_PRIMARY_COMMIT,
  DBC_SUPPLEMENTAL_URL, DBC_SUPPLEMENTAL_SHA256, DBC_SUPPLEMENTAL_COMMIT,
  VSS_MAPPING_URL, VSS_MAPPING_SHA256,
  VEHICLE_FIRMWARE, DECODE_EPOCH, VSS_VERSION.
Optional: VSS_MAPPING_COMMIT,
  DBC_OVERRIDE_URL (+ DBC_OVERRIDE_SHA256, DBC_OVERRIDE_VERSION,
  and optional DBC_OVERRIDE_COMMIT), DATA_DIR (default /data).
Missing required env or bad checksum exits non-zero; nothing is faked.
"""

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone

BO_RE = re.compile(r"^BO_\s+(\d+)\s+(\w+)", re.MULTILINE)
SG_RE = re.compile(r"^\s*SG_\s+(\w+)", re.MULTILINE)
EPOCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

GENERATIONS = "generations"
CURRENT_LINK = "current"


class ConfigError(ValueError):
    pass


class EpochCollision(ValueError):
    pass


def load_config(env=None):
    """Validate all inputs before any download or write. Pure (no I/O)."""
    env = os.environ if env is None else env

    def req(name):
        val = env.get(name, "").strip()
        if not val:
            raise ConfigError(f"required env {name} is missing")
        return val

    cfg = {
        "data_dir": env.get("DATA_DIR", "/data"),
        "primary_url": req("DBC_PRIMARY_URL"),
        "primary_sha256": req("DBC_PRIMARY_SHA256"),
        "primary_commit": req("DBC_PRIMARY_COMMIT"),
        "supplemental_url": req("DBC_SUPPLEMENTAL_URL"),
        "supplemental_sha256": req("DBC_SUPPLEMENTAL_SHA256"),
        "supplemental_commit": req("DBC_SUPPLEMENTAL_COMMIT"),
        "mapping_url": req("VSS_MAPPING_URL"),
        "mapping_sha256": req("VSS_MAPPING_SHA256"),
        "mapping_commit": env.get("VSS_MAPPING_COMMIT", "").strip(),
        "vehicle_firmware": req("VEHICLE_FIRMWARE"),
        "decode_epoch": req("DECODE_EPOCH"),
        "vss_version": req("VSS_VERSION"),
        "override_url": env.get("DBC_OVERRIDE_URL", "").strip(),
        "override_sha256": env.get("DBC_OVERRIDE_SHA256", "").strip(),
        "override_commit": env.get("DBC_OVERRIDE_COMMIT", "").strip(),
        "override_version": env.get("DBC_OVERRIDE_VERSION", "").strip(),
        "activate": env.get("SETUP_ACTIVATE_GENERATION", "1"),
    }
    if cfg["activate"] not in ("0", "1"):
        raise ConfigError("SETUP_ACTIVATE_GENERATION must be 0 or 1")
    cfg["activate"] = cfg["activate"] == "1"
    if cfg["override_url"] and not cfg["override_sha256"]:
        raise ConfigError("DBC_OVERRIDE_SHA256 is required when DBC_OVERRIDE_URL is set")
    if cfg["override_url"] and not cfg["override_version"]:
        raise ConfigError("DBC_OVERRIDE_VERSION is required when DBC_OVERRIDE_URL is set")
    for field in ("primary_commit", "supplemental_commit", "mapping_commit", "override_commit"):
        value = cfg[field]
        if value and not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", value):
            raise ConfigError(field + " must be a full Git commit hash")
        cfg[field] = value.lower()
    if not EPOCH_RE.fullmatch(cfg["decode_epoch"]):
        raise ConfigError(
            f"DECODE_EPOCH {cfg['decode_epoch']!r} must match [A-Za-z0-9][A-Za-z0-9._-]* "
            "(it becomes a directory name)")
    return cfg


def download(url, timeout=120):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read()
    except Exception as ex:
        print(f"vehicle_setup: download failed for {url}: {ex}", file=sys.stderr)
        sys.exit(2)


def check_sha(data, want, label):
    got = hashlib.sha256(data).hexdigest()
    if got.lower() != want.lower():
        print(f"vehicle_setup: SHA256 mismatch for {label}: want {want} got {got}",
              file=sys.stderr)
        sys.exit(2)
    return got


def parse_dbc(text):
    ids = {}
    for m in BO_RE.finditer(text):
        ids.setdefault(int(m.group(1)), []).append(m.group(2))
    signals = {m.group(1) for m in SG_RE.finditer(text)}
    return ids, signals


def split_messages(text):
    """Split DBC into header + [(can_id, name, block)]. Validation only
    (locating which message a signal belongs to); never used for patching."""
    matches = list(BO_RE.finditer(text))
    if not matches:
        raise ValueError("DBC defines no BO_ message blocks")
    header = text[:matches[0].start()]
    blocks = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        blocks.append((int(m.group(1)), m.group(2), text[m.start():end]))
    return header, blocks


def dbc_issues(text, label):
    """Same-file duplicate CAN IDs. Returns a list of findings (empty = OK)."""
    _, blocks = split_messages(text)
    seen = {}
    issues = []
    for cid, name, _ in blocks:
        if cid in seen:
            issues.append(
                f"{label}: duplicate CAN ID {cid} "
                f"({seen[cid]} vs {name})")
        else:
            seen[cid] = name
    return issues


def cross_file_dupes(p_ids, s_ids):
    """CAN IDs present in both files. Fatal -- never silently merged."""
    return sorted(set(p_ids) & set(s_ids))


def signal_conflicts(labeled):
    """Same signal name under different CAN IDs is ambiguous for the
    signal->message mapping. Returns {signal: sorted [(label, id)]}."""
    loc = {}
    for label, text in labeled:
        _, blocks = split_messages(text)
        for cid, _, blk in blocks:
            for m in SG_RE.finditer(blk):
                loc.setdefault(m.group(1), set()).add((label, cid))
    return {s: sorted(v) for s, v in loc.items()
            if len({cid for _, cid in v}) > 1}


def resolve_supplemental(supp_text, override_blob, override_commit,
                         override_version=""):
    """Complete-replacement policy: no merge. Returns (text, over_info)."""
    if override_blob is None:
        return supp_text, {"applied": False}
    try:
        text = override_blob.decode("utf-8", errors="strict")
    except UnicodeDecodeError as ex:
        raise ValueError(f"override is not valid UTF-8: {ex}")
    split_messages(text)  # fail closed on empty/invalid override DBC
    return text, {"applied": True, "sha256": "",
                  "commit": override_commit or None,
                  "version": override_version or None}


def iter_nodes(node, prefix=""):
    """Yield (vss_path, node) for every node in a VSS JSON tree.

    Official upstream VSS JSON nests branches under "children"; those are
    traversed transparently so "children" never appears in a path. Flat
    overlay-style trees (no "children" key) are traversed by key.
    """
    yield prefix, node
    if not isinstance(node, dict):
        return
    children = node.get("children")
    if isinstance(children, dict):
        for key, val in children.items():
            if isinstance(val, dict):
                path = f"{prefix}.{key}" if prefix else key
                yield from iter_nodes(val, path)
        return
    for key, val in node.items():
        if not isinstance(val, dict) or key in (
                "dbc2vss", "dbc", "vss2dbc", "transform", "mapping"):
            continue
        if prefix in ("", "Vehicle") or prefix.startswith("Vehicle.") \
                or key[:1].isupper():
            path = f"{prefix}.{key}" if prefix else key
            yield from iter_nodes(val, path)


def find_mappings(node, prefix=""):
    """Walk VSS JSON tree; return [(vss_path, dbc_signal, kind)]."""
    out = []
    for path, n in iter_nodes(node, prefix):
        if not isinstance(n, dict):
            continue
        for key in ("dbc2vss", "dbc"):
            m = n.get(key)
            if isinstance(m, dict) and "signal" in m:
                out.append((path, str(m["signal"]), "dbc2vss"))
                break
        m = n.get("vss2dbc")
        if isinstance(m, dict) and "signal" in m:
            out.append((path, str(m["signal"]), "vss2dbc"))
    return out


def validate_mapping(vehicle_node, dbc_signals):
    """Fail closed on empty/unknown/untyped mappings. Returns
    (dbc_refs, actuated_paths); vss2dbc entries are reported, never applied
    (the provider runs receive-only)."""
    refs = find_mappings(vehicle_node, "Vehicle")
    dbc_refs = [(p, s) for p, s, k in refs if k == "dbc2vss"]
    if not dbc_refs:
        raise ValueError("mapping defines no dbc2vss signals")
    unknown = sorted({s for _, s in dbc_refs if s not in dbc_signals})
    if unknown:
        raise ValueError(f"mapping references unknown DBC signals: {unknown}")
    nodes = dict(iter_nodes(vehicle_node, "Vehicle"))
    bare = sorted(p for p, _ in dbc_refs
                  if not isinstance(nodes.get(p), dict)
                  or not isinstance(nodes[p].get("datatype"), str)
                  or not isinstance(nodes[p].get("type"), str))
    if bare:
        raise ValueError(
            f"mapping paths are not typed VSS leaves "
            f"(need datatype+type): {bare}")
    actuated = sorted({p for p, _, k in refs if k == "vss2dbc"})
    return dbc_refs, actuated


def generation_dir(data_dir, epoch):
    return os.path.join(data_dir, GENERATIONS, epoch)


def list_generation_manifests(data_dir):
    """Load every persisted per-epoch manifest. Corrupt history fails closed."""
    root = os.path.join(data_dir, GENERATIONS)
    if not os.path.isdir(root):
        return []
    out = []
    for epoch in sorted(os.listdir(root)):
        if epoch.startswith("."):
            continue
        if not os.path.isdir(os.path.join(root, epoch)):
            continue
        mpath = os.path.join(root, epoch, "manifest.json")
        if not os.path.exists(mpath):
            raise ValueError(
                f"generation {epoch} has no manifest.json (corrupt history)")
        try:
            with open(mpath) as f:
                m = json.load(f)
        except (OSError, ValueError) as ex:
            raise ValueError(
                f"generation {epoch} manifest unreadable: {ex}")
        if not isinstance(m, dict) or "decode_epoch" not in m:
            raise ValueError(
                f"generation {epoch} manifest has no decode_epoch")
        out.append(m)
    return out


def check_epoch_history(existing, ident):
    """Any persisted generation with the same epoch but different inputs
    collides, however many epochs shipped since (A->B->A fails)."""
    for old in existing:
        if old.get("decode_epoch") != ident["decode_epoch"]:
            continue
        old_inputs = old.get("inputs", {})
        diffs = sorted(k for k in ident if old_inputs.get(k) != ident[k])
        if diffs:
            raise EpochCollision(
                f"decode_epoch {ident['decode_epoch']} already published with "
                f"different inputs ({', '.join(diffs)}); bump DECODE_EPOCH")


def current_target(data_dir):
    link = os.path.join(data_dir, CURRENT_LINK)
    if os.path.islink(link):
        return os.readlink(link)
    return None


def publish_generation(data_dir, epoch, files, activate=True):
    """Publish an immutable generation; optionally switch the live pointer.

    Offline preparation shares epoch history without switching the running
    provider's configuration. Failed staging leaves prior data untouched.
    """
    os.makedirs(os.path.join(data_dir, GENERATIONS), exist_ok=True)
    stage = tempfile.mkdtemp(prefix=".stage_", dir=data_dir)
    try:
        for rel, data in files.items():
            if not isinstance(data, (bytes, bytearray)):
                raise TypeError(f"publish file {rel} is not bytes")
            dest = os.path.join(stage, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
        target = generation_dir(data_dir, epoch)
        if os.path.exists(target):
            # Identical re-run: history already verified inputs match; just
            # re-point current below without touching the live generation.
            pass
        else:
            os.rename(stage, target)
            stage = None  # consumed by the rename; nothing to clean
        if activate:
            tmp_link = os.path.join(data_dir, ".current.tmp")
            if os.path.islink(tmp_link) or os.path.exists(tmp_link):
                os.unlink(tmp_link)
            os.symlink(os.path.join(GENERATIONS, epoch), tmp_link)
            os.replace(tmp_link, os.path.join(data_dir, CURRENT_LINK))
    finally:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)


def fail(msg):
    print(f"vehicle_setup: {msg}", file=sys.stderr)
    sys.exit(2)


def main():
    try:
        cfg = load_config()
    except ConfigError as ex:
        fail(str(ex))

    artifacts = [
        ("primary", cfg["primary_url"], cfg["primary_sha256"], cfg["primary_commit"]),
        ("supplemental", cfg["supplemental_url"], cfg["supplemental_sha256"],
         cfg["supplemental_commit"]),
        ("mapping", cfg["mapping_url"], cfg["mapping_sha256"], cfg["mapping_commit"]),
    ]
    if cfg["override_url"]:
        artifacts.append(("override", cfg["override_url"], cfg["override_sha256"],
                          cfg["override_commit"]))

    blobs, shas = {}, {}
    for role, url, sha, _ in artifacts:
        data = download(url)
        shas[role] = check_sha(data, sha, role)
        blobs[role] = data

    try:
        primary_text = blobs["primary"].decode("utf-8", errors="strict")
        supp_text = blobs["supplemental"].decode("utf-8", errors="strict")
    except UnicodeDecodeError as ex:
        fail(f"DBC artifact is not valid UTF-8: {ex}")

    try:
        supp_text, over_info = resolve_supplemental(
            supp_text, blobs.get("override"), cfg["override_commit"],
            cfg["override_version"])
    except ValueError as ex:
        fail(f"override rejected: {ex}")
    if over_info["applied"]:
        over_info["sha256"] = shas["override"]

    # Duplicate CAN IDs (within a file, or across primary/supplemental) and
    # one signal name under different CAN IDs are fatal -- never merged.
    try:
        issues = (dbc_issues(primary_text, "primary")
                  + dbc_issues(supp_text, "supplemental"))
        p_ids, p_signals = parse_dbc(primary_text)
        s_ids, s_signals = parse_dbc(supp_text)
        dupes = cross_file_dupes(p_ids, s_ids)
        conflicts = signal_conflicts([("primary", primary_text),
                                      ("supplemental", supp_text)])
    except ValueError as ex:
        fail(f"DBC rejected: {ex}")
    if issues:
        fail(f"duplicate CAN IDs: {'; '.join(issues)}")
    if dupes:
        detail = ", ".join(
            f"{i}(primary:{p_ids[i]}/supp:{s_ids[i]})" for i in dupes)
        fail(f"duplicate CAN IDs across DBC files: {detail}")
    if conflicts:
        detail = "; ".join(
            f"{s} in {['%s:%d' % loc for loc in v]}" for s, v in sorted(conflicts.items()))
        fail(f"signal name reused across CAN IDs (ambiguous mapping): {detail}")

    try:
        mapping = json.loads(blobs["mapping"].decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as ex:
        fail(f"mapping artifact is not valid JSON: {ex}")
    if not isinstance(mapping, dict) or not isinstance(mapping.get("Vehicle"), dict):
        fail("mapping JSON is not a VSS tree (no top-level Vehicle dict)")
    try:
        dbc_refs, actuated = validate_mapping(
            mapping.get("Vehicle"), p_signals | s_signals)
    except ValueError as ex:
        fail(str(ex))
    if actuated:
        print(f"vehicle_setup: WARNING {len(actuated)} vss2dbc (actuation) entries "
              f"in mapping; provider runs receive-only so they are ignored: "
              f"{actuated[:5]}")

    ident = {
        "decode_epoch": cfg["decode_epoch"],
        "vehicle_firmware": cfg["vehicle_firmware"],
        "vss_version": cfg["vss_version"],
        "primary_sha256": shas["primary"],
        "supplemental_sha256": shas["supplemental"],
        "mapping_sha256": shas["mapping"],
        "override_applied": over_info["applied"],
        "override_sha256": shas.get("override", ""),
        "override_version": cfg["override_version"],
    }
    try:
        existing = list_generation_manifests(cfg["data_dir"])
        check_epoch_history(existing, ident)
    except (ValueError, OSError) as ex:
        fail(str(ex))

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "vehicle_firmware": cfg["vehicle_firmware"],
        "decode_epoch": cfg["decode_epoch"],
        "vss_version": cfg["vss_version"],
        "inputs": ident,
        "dbc_primary_commit": cfg["primary_commit"],
        # Effective row metadata: an applied override IS the supplemental,
        # so the replaced supplemental commit is null here (never the
        # version string); original inputs stay under inputs/artifacts.
        "dbc_supplemental_commit": (None if over_info["applied"]
                                    else cfg["supplemental_commit"]),
        "mapping_revision": cfg["mapping_commit"],
        "dbc_override_version": (cfg["override_version"]
                                 if over_info["applied"] else None),
        "dbc_override_commit": ((cfg["override_commit"] or None)
                                if over_info["applied"] else None),
        "artifacts": [
            {"role": r, "url": u, "sha256": shas[r], "commit": c,
             "bytes": len(blobs[r])}
            for r, u, _, c in artifacts
        ],
        "dbc": {"can_ids": len(p_ids) + len(s_ids),
                "signals": len(p_signals | s_signals),
                "primary": {"messages": len(p_ids), "signals": len(p_signals)},
                "supplemental": {"messages": len(s_ids), "signals": len(s_signals)}},
        "override": over_info,
        "mapping": {"vss_paths_mapped": len(dbc_refs)},
    }
    outputs = {
        "dbc/primary.dbc": primary_text.encode("utf-8"),
        "dbc/supplemental.dbc": supp_text.encode("utf-8"),
        "mapping/vss_dbc.json": blobs["mapping"],
        "manifest.json": json.dumps(manifest, indent=2).encode("utf-8"),
    }
    try:
        publish_generation(cfg["data_dir"], cfg["decode_epoch"], outputs,
                           activate=cfg["activate"])
    except (OSError, TypeError, ValueError) as ex:
        fail(f"publish failed, previous generation preserved: {ex}")
    print(f"vehicle_setup: OK epoch={manifest['decode_epoch']} "
          f"can_ids={manifest['dbc']['can_ids']} "
          f"signals={manifest['dbc']['signals']} mapped={len(dbc_refs)} "
          f"override={over_info['applied']} activated={cfg['activate']}")


if __name__ == "__main__":
    main()
