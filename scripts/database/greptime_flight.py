#!/usr/bin/env python3
"""GreptimeDB Arrow Flight fenced reads (AI incremental discovery).

Raw grpcio DoGet + official protobuf dynamic descriptors + pyarrow IPC
decoding. The stock pyarrow Flight client cannot read Greptime responses:
the terminal frame carries a nonempty IPC MessageHeader.NONE envelope,
which the stock reader rejects ("Header-type ... is not RecordBatch").
So frames are parsed as protobuf FlightData and only data frames are
reassembled into an IPC stream for pa.ipc.RecordBatchStreamReader
(which transparently handles dictionary batches).

Wire pins (GreptimeDB v1.2.1 commit 179ff8e, Arrow 25.0.1):
  Ticket.ticket = GreptimeRequest{header:1, query:3},
  Query.sql:1, RequestHeader{catalog:1, schema:2, authorization:3,
  dbname:4}, AuthHeader.basic:1, Basic{username:1, password:2}.
  DoGet metadata "x-greptime-flow-extensions" = JSON array of string
  pairs: flow.return_region_seq=true always; fenced reads add
  flow.incremental_mode=memtable_only and flow.incremental_after_seqs
  = JSON {region_id: seq}. The terminal frame carries app_metadata only
  (no data body): FlightMetadata.metrics:2 -> Metrics.metrics:1 -> JSON
  {region_watermarks: [{region_id, watermark}]}. HTTP /v1/sql never
  returns these watermarks.

Stdlib-only import time: grpc/pyarrow/protobuf import lazily inside
flight_query so aggregate stays importable without the deps installed.
"""

import json
import struct

GREPTIME_CATALOG = "greptime"
FLOW_EXTENSIONS_HEADER = "x-greptime-flow-extensions"
FLIGHT_DO_GET = "/arrow.flight.protocol.FlightService/DoGet"

STALE_MARKERS = ("STALE_CURSOR", "STALE_SNAPSHOT_FENCE",
                 "FALLBACK_FULL_RECOMPUTE")
UNPROVEN_MARKERS = ("UNPROVED",)


class FlightError(Exception):
    """Fenced read failed; caller fails the section (never silent skip)."""


class FlightUnavailable(FlightError):
    """No endpoint, missing dep, or unproved watermarks: caller rebuilds."""


class StaleFence(FlightError):
    """Stored lower bound is below the flushed frontier: caller rebuilds."""


class RowCapExceeded(FlightError):
    """Fenced result exceeds max_rows: caller fails closed, never partial."""


def available():
    """True only when all three wire deps import."""
    try:
        __import__("grpc")
        __import__("pyarrow")
        __import__("google.protobuf")
    except ImportError:
        return False
    return True


def _varint(value):
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _field(num, payload):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return _varint(num * 8 + 2) + _varint(len(payload)) + payload


def _ticket_bytes(sql, db, user, password):
    basic = _field(1, user) + _field(2, password)
    header = (_field(1, GREPTIME_CATALOG) + _field(2, db)
              + _field(3, _field(1, basic)) + _field(4, db))
    request = _field(1, header) + _field(3, _field(1, sql))
    return _field(1, request)


_descriptors = {}


def _messages():
    """Official protobuf dynamic classes for the Flight envelope."""
    if "data" in _descriptors:
        return (_descriptors["data"], _descriptors["meta"])
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    fd = descriptor_pb2.FileDescriptorProto(
        name="greptime_flight.proto", package="datalake", syntax="proto3")
    spec = [("FlightData", [(2, "data_header", 12, None),
                            (3, "app_metadata", 12, None),
                            (1000, "data_body", 12, None)]),
            ("Metrics", [(1, "metrics", 12, None)]),
            ("FlightMetadata", [(2, "metrics", 11, ".datalake.Metrics")])]
    for name, fields in spec:
        message = fd.message_type.add(name=name)
        for num, key, typ, ref in fields:
            f = message.field.add(number=num, name=key, type=typ, label=1)
            if ref:
                f.type_name = ref
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fd)
    data = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("datalake.FlightData"))
    meta = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("datalake.FlightMetadata"))
    _descriptors["data"] = data
    _descriptors["meta"] = meta
    return data, meta


def _parse_upper(app_metadata):
    """Terminal metadata -> {region_id: watermark}; None when unproved."""
    _, meta_cls = _messages()
    try:
        meta = meta_cls.FromString(app_metadata)
        decoded = json.loads(meta.metrics.metrics)
    except Exception:
        return None
    if not isinstance(decoded, dict):
        return None
    marks = decoded.get("region_watermarks")
    if not isinstance(marks, list) or not marks:
        return None
    upper = {}
    for entry in marks:
        if not isinstance(entry, dict):
            return None
        region = entry.get("region_id")
        mark = entry.get("watermark")
        if (isinstance(region, bool) or not isinstance(region, int)
                or isinstance(mark, bool) or not isinstance(mark, int)
                or not 0 <= region < 2**64 or not 0 <= mark < 2**64
                or region in upper and upper[region] != mark):
            return None
        upper[region] = mark
    return upper or None


def flight_query(sql, *, endpoint, db, user, password, lower=None,
                 timeout=60, max_rows=200000):
    """Run one fenced SQL query. Returns (cols, rows, upper).

    lower=None means an unfenced snapshot read (full data + upper
    watermarks). lower={region_id: seq} adds memtable_only +
    incremental_after_seqs so the server returns only rows committed
    after the bound plus the new upper. lower==flushed is an accepted
    no-op (zero rows, same upper); lower<flushed raises StaleFence.

    Timestamp columns are cast to int64 nanoseconds (never float, never
    truncated to microseconds); every other column uses to_pylist.
    """
    endpoint = (endpoint or "").strip()
    if not endpoint:
        raise FlightUnavailable("no grpc endpoint configured")
    try:
        import grpc
        import pyarrow as pa
    except ImportError as exc:
        raise FlightUnavailable("flight deps missing: " + str(exc)[:100])
    data_cls, _ = _messages()
    from urllib.parse import urlsplit
    host = endpoint
    secure = False
    if "://" in endpoint:
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in ("grpc", "http", "https", "grpcs")
                or not parsed.netloc or parsed.username is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise FlightError("invalid Flight endpoint")
        secure = parsed.scheme in ("https", "grpcs")
        host = parsed.netloc
    extensions = [["flow.return_region_seq", "true"]]
    if lower:
        clean = {}
        for region, seq in lower.items():
            try:
                clean[int(region)] = int(seq)
            except (TypeError, ValueError):
                raise FlightUnavailable("stored fence is not int-keyed")
        if not clean:
            raise FlightUnavailable("stored fence is empty")
        extensions.append(["flow.incremental_mode", "memtable_only"])
        extensions.append(["flow.incremental_after_seqs",
                           json.dumps({str(k): v for k, v in clean.items()},
                                      sort_keys=True)])
    ticket = _ticket_bytes(sql, db, user, password)
    channel = None
    stream_file = None
    try:
        options = (("grpc.max_receive_message_length", 64 * 1024 * 1024),)
        channel = (grpc.secure_channel(host, grpc.ssl_channel_credentials(), options)
                   if secure else grpc.insecure_channel(host, options))
        call = channel.unary_stream(
            FLIGHT_DO_GET, request_serializer=lambda b: b,
            response_deserializer=data_cls.FromString)
        try:
            frames = call(ticket, timeout=timeout,
                          metadata=[(FLOW_EXTENSIONS_HEADER,
                                     json.dumps(extensions))])
            total_rows = 0
            upper = {}
            terminal_seen = False
            try:
                import tempfile
                stream_file = tempfile.TemporaryFile(prefix="flight-ipc-")
                for frame in frames:
                    if terminal_seen:
                        raise FlightError("frame after terminal watermark proof")
                    header = bytes(frame.data_header or b"")
                    body = bytes(frame.data_body or b"")
                    meta = bytes(frame.app_metadata or b"")
                    if meta and not body:
                        terminal_seen = True
                        marks = _parse_upper(frame.app_metadata)
                        if marks is None:
                            raise FlightUnavailable(
                                "terminal watermarks missing or unproved")
                        upper.update(marks)
                        continue
                    if meta:
                        raise FlightError("data frame carries app_metadata")
                    if not header:
                        raise FlightError("data frame has empty IPC header")
                    stream_file.write(
                        struct.pack("<II", 0xFFFFFFFF, len(header))
                        + header + body)
                # EOF drain is the fence proof: an iterator that raises
                # (grpc error, truncated stream) never reaches here, so no
                # error after terminal metadata can advance a checkpoint.
            except FlightError:
                raise
            except grpc.RpcError as exc:
                detail = ""
                try:
                    detail = exc.details() or ""
                except Exception:
                    detail = ""
                # Native 1.2.1 surfaces staleness as INTERNAL with the
                # STALE_CURSOR body (never require one grpc status code).
                if any(m in detail for m in STALE_MARKERS):
                    raise StaleFence("fence below flushed frontier: "
                                     + detail[:200])
                raise FlightError("flight DoGet failed: " + detail[:200])
            if not terminal_seen:
                raise FlightUnavailable("no terminal watermark frame")
            if not upper:
                raise FlightUnavailable("server returned no region watermarks")
            stream_file.seek(0)
            try:
                reader = pa.ipc.open_stream(stream_file)
                schema = reader.schema
                batches = []
                for batch in reader:
                    total_rows += batch.num_rows
                    if total_rows > max_rows:
                        raise RowCapExceeded(
                            "row cap exceeded (> %d): refusing partial write"
                            % max_rows)
                    batches.append(batch)
                table = pa.Table.from_batches(batches, schema=schema)
            except FlightError:
                raise
            except Exception as exc:
                raise FlightError("ipc decode failed: " + str(exc)[:200])
        except FlightError:
            raise
        except grpc.RpcError as exc:
            detail = ""
            try:
                detail = exc.details() or ""
            except Exception:
                detail = ""
            if any(m in detail for m in STALE_MARKERS):
                raise StaleFence("fence below flushed frontier: "
                                 + detail[:200])
            raise FlightError("flight DoGet failed: " + detail[:200])
        cols = list(schema.names)
        columns = []
        for i, field in enumerate(schema):
            try:
                if pa.types.is_timestamp(field.type):
                    # Exact int64 ns: to_pylist on datetimes truncates to us.
                    columns.append(
                        table.column(i).cast(pa.timestamp("ns", tz=field.type.tz))
                        .cast(pa.int64()).to_pylist())
                else:
                    columns.append(table.column(i).to_pylist())
            except Exception as exc:
                raise FlightError("column decode failed (%s): %s"
                                  % (field.name, str(exc)[:150]))
        rows = [list(r) for r in zip(*columns)] if columns else []
        return cols, rows, upper
    finally:
        if stream_file is not None:
            stream_file.close()
        try:
            if channel is not None:
                channel.close()
        except Exception:
            pass
