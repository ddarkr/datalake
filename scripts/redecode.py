#!/usr/bin/env python3
"""Offline raw MF4 -> derived regeneration (one-shot, receive-only).

Reads sealed MF4 frames (local raw-spool volume, or a SHA256-verified S3
object URL / s3:// key), decodes with cantools DBC definitions, transforms
with the pinned upstream KUKSA Mapper (dbcfeederlib.dbc2vssmapper.Mapper,
fetched at a pinned commit by redecode-deps -- never reimplemented here),
preserves the original frame time as event_time, stores rows with the
shared vss_recorder helpers (make_row / deterministic_event_id /
store_update) in the SQLite outbox, then batch-uploads to Greptime.

No CAN socket is ever opened: input is files only (can.io.mf4.MF4Reader).
Reruns are idempotent (deterministic event_id + INSERT OR IGNORE +
seen_ids; Greptime PK re-merge absorbs post-ack replays); a new decode
epoch coexists (decode_epoch is part of the Greptime PK).

Env (all ${VAR:-} soft refs in compose; dotenv only):
  VEHICLE_ID / REDECODE_VEHICLE_ID, REDECODE_DECODE_EPOCH (or DECODE_EPOCH),
  REDECODE_INPUT_URL + REDECODE_INPUT_SHA256 (else spool scan) plus the
  sealed triple siblings REDECODE_INPUT_MANIFEST_URL /
  REDECODE_INPUT_SIDECAR_URL (default: <mf4>.manifest.json +
  <stem>.ingress.jsonl.gz beside the MF4 URL, the layout raw-upload
  stores; overrides exist for presigned URLs),
  REDECODE_SPOOL_DIR (/spool/raw), REDECODE_WORKDIR (/tmp/redecode),
  REDECODE_OUTBOX_PATH (/data/redecode-outbox.sqlite),
  REDECODE_BATCH_N (500, positive int via shared vr._positive_int),
  COLLECTOR_VERSION / REDECODE_COLLECTOR_VERSION,
  MANIFEST_PATH (/data/current/manifest.json),
  MAPPING_PATH (/data/current/mapping/vss_dbc.json),
  DBC_PRIMARY_PATH / DBC_SUPPLEMENTAL_PATH (/data/current/dbc/*.dbc),
  KUKSA_SRC_DIR (/opt/venv/kuksa_verified),
  GREPTIME_HTTP_URL/DB/USER/PASSWORD,
  RAW_S3_ENDPOINT_URL/RAW_S3_REGION/RAW_S3_ACCESS_KEY_ID/RAW_S3_SECRET_ACCESS_KEY for s3:// inputs.
Exit: 0 ok; 1 upload failed (outbox kept for retry); 2 config/input error.
"""

import glob
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import vss_recorder as vr
    import raw_recorder as rr
except ImportError as ex:
    sys.stderr.write(
        f"redecode: error: recorder modules must be mounted beside me: {ex}\n")
    raise SystemExit(2)


def e(name, default=""):
    return os.environ.get(name, default)


def fail(msg, code=2):
    sys.stderr.write("redecode: error: " + msg + "\n")
    raise SystemExit(code)


def fetch_bytes(url):
    """Actual fetch: s3:// via boto3 with explicit RAW_S3_* credentials
    (HTTPS endpoint gate, same policy as raw_upload.make_client), else plain HTTPS GET."""
    if url.startswith("s3://"):
        import boto3  # noqa: pinned dep, lazy so import errors stay local
        import urllib.parse as _up
        from botocore.config import Config
        endpoint = (e("RAW_S3_ENDPOINT_URL") or "").strip()
        parsed = _up.urlparse(endpoint)
        if parsed.scheme != "https" or not parsed.hostname:
            fail("RAW_S3_ENDPOINT_URL must be an https:// URL with a host")
        rest = url[len("s3://"):]
        bucket, _, key = rest.partition("/")
        if not bucket or not key:
            fail("bad s3 url: " + url)
        import io
        cli = boto3.client(
            "s3", endpoint_url=endpoint,
            region_name=e("RAW_S3_REGION") or None,
            aws_access_key_id=e("RAW_S3_ACCESS_KEY_ID") or None,
            aws_secret_access_key=e("RAW_S3_SECRET_ACCESS_KEY") or None,
            config=Config(connect_timeout=10, read_timeout=60))
        buf = io.BytesIO()
        cli.download_fileobj(bucket, key, buf)
        return buf.getvalue()
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sibling_urls(url):
    """Sibling triple URLs for one sealed MF4 (same layout raw-upload
    stores: <stem>.mf4 + <stem>.mf4.manifest.json +
    <stem>.ingress.jsonl.gz). Returns (mf4, manifest, sidecar, stem)."""
    cut = len(url)
    for sep in ("?", "#"):
        i = url.find(sep)
        if i != -1:
            cut = min(cut, i)
    head, tail = url[:cut], url[cut:]
    if not head.endswith(".mf4"):
        fail(f"input URL must name a sealed .mf4 object: {url}")
    base = head[:-len(".mf4")]
    stem = base.rsplit("/", 1)[-1]
    if not stem:
        fail(f"input URL must name a sealed .mf4 object: {url}")
    return (url, head + ".manifest.json" + tail,
            base + ".ingress.jsonl.gz" + tail, stem)


def verify_sealed_triple(mf4_path):
    """Fail closed unless the adjacent capture manifest (RawUploader v2)
    and raw-ingress sidecar agree with the sealed MF4 bytes.

    Returns (manifest, frames): validated ingress records carry exact
    nanoseconds and complete frame identities. MF4 group ordering cannot
    substitute another frame's timestamp."""
    manifest_path = mf4_path + ".manifest.json"
    stem = os.path.basename(mf4_path)
    if stem.endswith(".mf4"):
        stem = stem[:-len(".mf4")]
    sidecar_path = os.path.join(os.path.dirname(mf4_path),
                                stem + ".ingress.jsonl.gz")
    for need, what in ((manifest_path, "capture manifest"),
                       (sidecar_path, "raw-ingress sidecar")):
        if not os.path.isfile(need):
            fail(f"sealed triple incomplete, missing {what}: {need}")
    try:
        with open(manifest_path, encoding="utf-8") as fh:
            man = json.load(fh)
    except (OSError, ValueError) as ex:
        fail(f"capture manifest unreadable {manifest_path}: {ex}")
    if not isinstance(man, dict) or man.get("v") != 2:
        fail(f"capture manifest is not RawUploader v2: {manifest_path}")
    for field in ("mf4_sha256", "ingress_sha256",
                  "ingress_sidecar_sha256"):
        if not isinstance(man.get(field), str) or not man[field]:
            fail(f"capture manifest missing {field}: {manifest_path}")
    claimed = man.get("ingress_sidecar")
    if claimed and os.path.basename(claimed) != os.path.basename(
            sidecar_path):
        fail(f"capture manifest names another sidecar ({claimed}): "
             f"{manifest_path}")
    if sha256_file(mf4_path).lower() != man["mf4_sha256"].lower():
        fail(f"sealed MF4 drifted from manifest pin: {mf4_path}")
    if sha256_file(sidecar_path).lower() != \
            man["ingress_sidecar_sha256"].lower():
        fail(f"sealed sidecar drifted from manifest pin: {sidecar_path}")
    try:
        with open(sidecar_path, "rb") as fh:
            body = gzip.decompress(fh.read())
    except (OSError, EOFError) as ex:
        fail(f"sidecar gunzip failed {sidecar_path}: {ex}")
    if hashlib.sha256(body).hexdigest().lower() != \
            man["ingress_sha256"].lower():
        fail(f"sidecar content drifted from ingress pin: {sidecar_path}")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as ex:
        fail(f"sidecar ingress not UTF-8 {sidecar_path}: {ex}")
    frames = []
    for line in text.split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except ValueError as ex:
            fail(f"sidecar ingress corrupt {sidecar_path}: {ex}")
        ts = obj.get("twall_ns") if isinstance(obj, dict) else None
        if not isinstance(ts, int) or isinstance(ts, bool) or ts <= 0:
            fail(f"sidecar ingress bad twall_ns {sidecar_path}")
        try:
            frames.append(rr.decode_frame(obj))
        except (TypeError, ValueError, KeyError) as ex:
            fail(f"sidecar ingress invalid {sidecar_path}: {ex}")
    twall = sorted(frame["twall_ns"] for frame in frames)
    if man.get("frames") is not None and man["frames"] != len(twall):
        fail(f"sidecar frame count {len(twall)} != manifest frames "
             f"{man['frames']}: {sidecar_path}")
    # The capture manifest describes the chronological bounds.
    for field, got in (("twall_ns_first", twall[0] if twall else None),
                       ("twall_ns_last", twall[-1] if twall else None)):
        if man.get(field) is not None and man[field] != got:
            fail(f"sidecar {field} {got} != manifest {man[field]}: "
                 f"{sidecar_path}")
    try:
        rr.verify_sealed_mf4(mf4_path, frames)
    except (OSError, ValueError) as ex:
        fail(f"MF4/ingress mismatch {mf4_path}: {ex}")
    return man, frames


def fetch_all(urls):
    """Download each URL or fail closed naming the leg."""
    blobs = []
    for url in urls:
        try:
            blobs.append(fetch_bytes(url))
        except SystemExit:
            raise
        except Exception as ex:
            fail(f"input download failed ({url}): {ex}")
    return blobs


def resolve_inputs(workdir):
    """Return [(mf4_path, frames)]: every input hash-verified against
    its capture manifest + raw-ingress sidecar before decode.

    URL mode downloads the sealed triple (MF4 + <mf4>.manifest.json +
    <stem>.ingress.jsonl.gz, the layout raw-upload stores) instead of a
    lone checksum: REDECODE_INPUT_MANIFEST_URL / REDECODE_INPUT_SIDECAR_URL
    override the derived siblings (needed for presigned URLs); the MF4
    bytes must match both the manifest pin and REDECODE_INPUT_SHA256."""
    url = e("REDECODE_INPUT_URL", "").strip()
    if url:
        want = e("REDECODE_INPUT_SHA256", "").strip()
        if not want:
            fail("REDECODE_INPUT_URL needs REDECODE_INPUT_SHA256")
        _, derived_man, derived_car, stem = sibling_urls(url)
        man_url = e("REDECODE_INPUT_MANIFEST_URL", "").strip() \
            or derived_man
        car_url = e("REDECODE_INPUT_SIDECAR_URL", "").strip() \
            or derived_car
        mf4_blob, man_blob, car_blob = fetch_all([url, man_url, car_url])
        got = hashlib.sha256(mf4_blob).hexdigest()
        if got.lower() != want.lower():
            fail(f"input SHA256 mismatch: want {want} got {got}")
        os.makedirs(workdir, exist_ok=True)
        dest = os.path.join(workdir, stem + ".mf4")
        sidecar_dest = os.path.join(workdir, stem + ".ingress.jsonl.gz")
        try:
            with open(dest, "wb") as f:
                f.write(mf4_blob)
            with open(dest + ".manifest.json", "wb") as f:
                f.write(man_blob)
            with open(sidecar_dest, "wb") as f:
                f.write(car_blob)
        except OSError as ex:
            fail(f"cannot stage remote triple in {workdir}: {ex}")
        man, frames = verify_sealed_triple(dest)
        # Pin the manifest to the caller checksum too: a swapped manifest
        # claiming another MF4 must not pass on hash equality alone.
        if man["mf4_sha256"].lower() != want.lower():
            fail("capture manifest pin differs from REDECODE_INPUT_SHA256")
        return [(dest, frames)]
    spool = e("REDECODE_SPOOL_DIR", "/spool/raw")
    cands = sorted(glob.glob(os.path.join(spool, "sealed", "*.mf4")))
    if not cands:
        cands = sorted(glob.glob(os.path.join(spool, "*.mf4")))
    out = []
    for p in cands:
        if p.endswith(".tmp.mf4") or p.endswith(".stage.tmp"):
            continue
        stem = os.path.basename(p)
        if stem.endswith(".mf4"):
            stem = stem[:-len(".mf4")]
        if os.path.exists(os.path.join(os.path.dirname(p),
                                       stem + ".stage.tmp")):
            continue  # finalize still writing; invisible to the uploader too
        _, frames = verify_sealed_triple(p)
        out.append((p, frames))
    if not out:
        fail(f"no .mf4 inputs in {spool}/sealed and REDECODE_INPUT_URL unset")
    return out


def iter_frames(mf4_path, capture_frames):
    """Yield data frames with their exact, identity-matched capture time."""
    for message, frame in rr.matched_mf4_frames(mf4_path, capture_frames):
        if message.is_error_frame or message.is_remote_frame:
            continue
        yield message.arbitration_id, bytes(message.data), frame["twall_ns"]


def load_mapper(mapping_path, dbc_files):
    sys.path.insert(0, e("KUKSA_SRC_DIR", "/opt/venv/kuksa_verified"))
    try:
        from dbcfeederlib.dbc2vssmapper import Mapper
    except ImportError as ex:
        fail(f"pinned kuksa source missing in KUKSA_SRC_DIR "
             f"(redecode-deps not completed?): {ex}")
    return Mapper(mapping_path, dbc_files, use_strict_parsing=True,
                  fail_on_duplicate_signal_definitions=True)


def check_vehicle_artifacts(manifest_path, mapping_path, dbc_files):
    """Fail closed unless the DBC/mapping bytes still match the manifest
    artifact pins, so rows are never labeled with false provenance."""
    try:
        with open(manifest_path, encoding="utf-8") as fh:
            man = json.load(fh)
    except (OSError, ValueError) as ex:
        fail(f"cannot read manifest {manifest_path}: {ex}")
    arts = man.get("artifacts")
    if not isinstance(arts, list) or not arts:
        fail(f"generation manifest has no artifacts list: {manifest_path}")
    by_role = {}
    for art in arts:
        if isinstance(art, dict) and art.get("role"):
            by_role.setdefault(art["role"], art)
    want_files = {"mapping": mapping_path, "primary": dbc_files[0],
                  "supplemental": dbc_files[1]}
    roles = ["primary", "supplemental"]
    over = man.get("override") or {}
    if over.get("applied") and (by_role.get("override") or {}).get("sha256"):
        roles = ["primary", "override"]  # setup replaced supplemental bytes
        want_files["override"] = dbc_files[1]
    for role in ["mapping"] + roles:
        art = by_role.get(role)
        if not isinstance(art, dict) or not art.get("sha256"):
            fail(f"generation manifest missing {role} artifact hash: "
                 f"{manifest_path}")
        path = want_files[role]
        try:
            got = sha256_file(path)
        except OSError as ex:
            fail(f"cannot hash {role} artifact {path}: {ex}")
        if got.lower() != str(art["sha256"]).lower():
            fail(f"{role} artifact drifted from manifest pin "
                 f"(want {art['sha256']} got {got}): {path}")
    return man


def redecode_mf4(mf4_path, mapper, vehicle, epoch, meta, units, conn,
                 last, capture_frames):
    """Decode one MF4 into the outbox with the shared vr helpers.

    Returns (stored, skipped). The original frame time becomes
    event_time; event_id is vr.deterministic_event_id over
    (vehicle, path, time, epoch, value) so reruns IGNORE and a new
    epoch coexists.
    """
    stored, skipped = 0, 0
    for can_id, data, ns in iter_frames(mf4_path, capture_frames):
        try:
            msg_def = mapper.get_message_by_frame_id(can_id)
        except Exception:
            skipped += 1
            continue
        try:
            decoded = msg_def.decode(bytes(data), allow_truncated=True,
                                     decode_containers=True)
        except Exception:
            skipped += 1
            continue
        # ponytail: same container-frame split as upstream canreader
        frames = [(msg_def, decoded)] if isinstance(decoded, dict) else [
            (m, d) for m, d in decoded if not isinstance(d, bytes)]
        for mdef, signals in frames:
            for sig_name, raw in signals.items():
                try:
                    signal = mdef.get_signal_by_name(sig_name)
                except Exception:
                    continue
                # ponytail: same min/max range filter as upstream canreader
                if isinstance(raw, (int, float)):
                    if signal.minimum is not None and raw < signal.minimum:
                        continue
                    if signal.maximum is not None and raw > signal.maximum:
                        continue
                for mapping in mapper.get_dbc2vss_mappings(sig_name):
                    if not mapping.time_condition_fulfilled(ns / 1e9):
                        continue
                    try:
                        vss = mapping.transform_value(raw)
                    except Exception:
                        skipped += 1
                        continue
                    if vss is None:
                        continue
                    try:
                        if not mapping.change_condition_fulfilled(vss):
                            continue
                    except Exception:
                        skipped += 1
                        continue
                    try:
                        got = vr.classify(vss)
                    except vr.UnsupportedValue:
                        skipped += 1
                        continue
                    if got is None:
                        continue
                    num, text, boolean = got
                    row = vr.make_row(
                        vehicle, mapping.vss_name, ns,
                        vr.deterministic_event_id(
                            vehicle, mapping.vss_name, ns, epoch,
                            num, text, boolean),
                        meta, units, num, text, boolean, time.time_ns())
                    if vr.store_update(conn, last, mapping.vss_name, row):
                        stored += 1
    return stored, skipped


def main(argv=None):
    vehicle = e("REDECODE_VEHICLE_ID", "").strip() \
        or e("VEHICLE_ID", "").strip()
    if not vehicle:
        fail("REDECODE_VEHICLE_ID (or VEHICLE_ID) is required")
    epoch = e("REDECODE_DECODE_EPOCH", "").strip()
    if not epoch:
        fail("REDECODE_DECODE_EPOCH is required")
    base_url, db, user, password = (e("GREPTIME_HTTP_URL"), e("GREPTIME_DB"),
                                    e("GREPTIME_USER"), e("GREPTIME_PASSWORD"))
    if not all([base_url, db, user, password]):
        fail("GREPTIME_HTTP_URL/DB/USER/PASSWORD required")
    generation = os.path.join("/data/generations", epoch)
    manifest_path = e("MANIFEST_PATH", generation + "/manifest.json")
    mapping_path = e("MAPPING_PATH", generation + "/mapping/vss_dbc.json")
    dbc_files = [e("DBC_PRIMARY_PATH", generation + "/dbc/primary.dbc"),
                 e("DBC_SUPPLEMENTAL_PATH", generation + "/dbc/supplemental.dbc")]
    for p in dbc_files + [manifest_path, mapping_path]:
        if not os.path.isfile(p):
            fail(f"missing vehicle artifact (vehicle-setup first?): {p}")
    batch = vr._positive_int(
        "REDECODE_BATCH_N", e("REDECODE_BATCH_N", "500"), 500)
    inputs = resolve_inputs(e("REDECODE_WORKDIR", "/tmp/redecode"))
    manifest = check_vehicle_artifacts(manifest_path, mapping_path, dbc_files)
    if manifest.get("decode_epoch") != epoch:
        fail("selected generation differs from REDECODE_DECODE_EPOCH")
    # Decode only against verified provenance: read the meta pins now
    # that the bytes are known good (a stale reader must not label rows
    # the hashes just rejected).
    try:
        meta = vr.load_manifest(manifest_path)
    except Exception as ex:
        fail(f"cannot read manifest {manifest_path}: {ex}")
    if not isinstance(meta, dict):
        fail(f"cannot read manifest {manifest_path}: not a mapping")
    # The immutable generation supplies the epoch, never relabel its bytes.
    meta["collector_version"] = (
        e("REDECODE_COLLECTOR_VERSION", "").strip()
        or ((e("COLLECTOR_VERSION", "vss-recorder-1").strip()
             or "vss-recorder-1") + "-redecode"))
    try:
        _, units = vr.mapped_paths(mapping_path)
    except Exception as ex:
        fail(f"cannot read mapping {mapping_path}: {ex}")
    mapper = load_mapper(mapping_path, dbc_files)
    if not mapper.has_dbc2vss_mapping():
        fail(f"mapping defines no dbc2vss signals: {mapping_path}")
    outbox_path = e("REDECODE_OUTBOX_PATH", "/data/redecode-outbox.sqlite")
    conn = vr.open_outbox(outbox_path)
    last = {}
    total, skipped = 0, 0
    for path, capture_frames in inputs:
        n, s = redecode_mf4(path, mapper, vehicle, epoch, meta, units,
                             conn, last, capture_frames)
        total, skipped = total + n, skipped + s
        print(f"redecode: file={os.path.basename(path)} "
              f"stored={n} skipped={s} epoch={epoch}")
    uploaded = 0
    while True:
        try:
            n = vr.upload_once(conn, "vehicle_signal", base_url, db,
                               user, password, batch)
        except Exception as ex:
            print(f"redecode: upload failed, outbox kept at {outbox_path}: "
                  f"{ex}")
            return 1
        if not n:
            break
        uploaded += n
    print(f"redecode: done files={len(inputs)} stored={total} "
          f"uploaded={uploaded} skipped={skipped} epoch={epoch}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
