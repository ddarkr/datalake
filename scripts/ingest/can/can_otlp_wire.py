"""Strict private CAN chunk envelopes carried by the standard OTLP protobufs."""
import re

from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest, ExportLogsServiceResponse,
)

MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_RECORDS = 10000
MAX_CHUNK_BYTES = 65536
MAX_INT = 2 ** 63 - 1
META_KEYS = {"schema_version", "vehicle", "collector_id", "session_id", "started_ns", "vehicle_firmware"}
CHUNK_KEYS = {"seq", "offset_ns", "phase", "data"}
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


class WireError(ValueError):
    """Invalid envelope or acknowledgement; messages never include raw content."""


class PartialSuccess(WireError):
    """OTLP partial acceptance/warning requires operator reconciliation, not retry."""


def _integer(value, minimum=0):
    return type(value) is int and minimum <= value <= MAX_INT


def validate_meta(meta):
    if type(meta) is not dict or set(meta) != META_KEYS:
        raise WireError("Invalid CAN metadata fields.")
    if type(meta["schema_version"]) is not int or meta["schema_version"] != 1:
        raise WireError("Unsupported CAN schema version.")
    for key in ("vehicle", "collector_id", "session_id"):
        if type(meta[key]) is not str or not IDENTITY.fullmatch(meta[key]):
            raise WireError("Invalid CAN identity.")
    if not _integer(meta["started_ns"], 1):
        raise WireError("Invalid capture start timestamp.")
    firmware = meta["vehicle_firmware"]
    if (type(firmware) is not str or len(firmware) > 160
            or any(not c.isprintable() for c in firmware)):
        raise WireError("Invalid vehicle firmware label.")


def _validate_chunks(meta, chunks):
    validate_meta(meta)
    if not isinstance(chunks, (list, tuple)) or not 1 <= len(chunks) <= MAX_RECORDS:
        raise WireError("Invalid CAN batch record count.")
    previous_seq = previous_offset = None
    for chunk in chunks:
        if type(chunk) is not dict or set(chunk) != CHUNK_KEYS:
            raise WireError("Invalid CAN chunk fields.")
        seq, offset = chunk["seq"], chunk["offset_ns"]
        if not _integer(seq) or not _integer(offset) or meta["started_ns"] + offset > MAX_INT:
            raise WireError("Invalid CAN chunk sequence/timestamp.")
        if previous_seq is not None and (seq != previous_seq + 1 or offset < previous_offset):
            raise WireError("Reordered CAN chunks.")
        if type(chunk["phase"]) is not str or chunk["phase"] not in ("capture", "close_drain"):
            raise WireError("Invalid CAN chunk phase.")
        if type(chunk["data"]) is not bytes or len(chunk["data"]) > MAX_CHUNK_BYTES:
            raise WireError("Invalid CAN chunk bytes.")
        previous_seq, previous_offset = seq, offset


def _put(attrs, key, value):
    attribute = attrs.add(key="can." + key)
    if type(value) is int:
        attribute.value.int_value = value
    else:
        attribute.value.string_value = value


def _varint_len(value):
    # Raw protobuf varint length; tag+value framing counted exactly below.
    length, value = 1, value >> 7
    while value:
        length, value = length + 1, value >> 7
    return length


def _body_len(data):
    # LogRecord field 5 (bytes): 1-byte tag + varint length + exact raw bytes.
    return 1 + _varint_len(len(data)) + len(data)


def _attr_len(key, value):
    # KeyValue entry: tag + varint inner length, where an int attribute costs its
    # varint and a UTF-8 string attribute costs its encoded byte length. Overlong
    # UTF-8 or lone surrogates cannot occur here: surrogates fail encoding exactly
    # like protobuf, and an explicit preflight type check precedes the codec.
    if type(value) is int:
        inner = 1 + _varint_len(len(key)) + len(key) + 2 + _varint_len(value)
    else:
        encoded = value.encode("utf-8")
        inner = (1 + _varint_len(len(key)) + len(key) + 2 + _varint_len(len(encoded))
                 + len(encoded))
    return 2 + _varint_len(inner) + inner


def encoded_size_estimate(meta, chunks):
    """Incremental envelope bound: envelope shares plus per-record worst cases.

    Varint/string framing is exact; only outer resource/scope container tags are
    fixed upper bounds, so the estimate never undercounts protobuf overhead.
    """
    _validate_chunks(meta, chunks)
    total = 4  # Outer repeated resource_logs entry: tag + varint bound.
    for key in sorted(META_KEYS):
        total += _attr_len("can." + key, meta[key])
    total += len("tesla-can") + len("1") + 14  # Scope + fixed container tags.
    for chunk in chunks:
        record = 1 + _varint_len(meta["started_ns"] + chunk["offset_ns"])  # Field 1.
        record += _body_len(chunk["data"])
        for key in ("seq", "offset_ns", "phase"):
            record += _attr_len("can." + key, chunk[key])
        total += 2 + _varint_len(record) + record
    return total


def encode_batch(meta, chunks):
    """Encode exact chunk bytes and integer nanosecond timestamps deterministically."""
    if encoded_size_estimate(meta, chunks) > MAX_REQUEST_BYTES:
        raise WireError("CAN request exceeds byte limit.")
    request = ExportLogsServiceRequest()
    resource = request.resource_logs.add()
    for key in sorted(META_KEYS):
        _put(resource.resource.attributes, key, meta[key])
    scope = resource.scope_logs.add()
    scope.scope.name, scope.scope.version = "tesla-can", "1"
    for chunk in chunks:
        record = scope.log_records.add(time_unix_nano=meta["started_ns"] + chunk["offset_ns"])
        record.body.bytes_value = chunk["data"]
        for key in ("seq", "offset_ns", "phase"):
            _put(record.attributes, key, chunk[key])
    payload = request.SerializeToString(deterministic=True)
    if len(payload) > MAX_REQUEST_BYTES:
        raise WireError("CAN request exceeds byte limit.")
    return payload


def _attrs(attributes, expected):
    result = {}
    for attribute in attributes:
        key = attribute.key
        if not key.startswith("can.") or key[4:] not in expected or key[4:] in result:
            raise WireError("Invalid or duplicate CAN attributes.")
        key = key[4:]
        kind = "int_value" if key in ("schema_version", "started_ns", "seq", "offset_ns") else "string_value"
        if attribute.value.WhichOneof("value") != kind:
            raise WireError("Invalid CAN attribute type.")
        result[key] = getattr(attribute.value, kind)
    if set(result) != expected:
        raise WireError("Missing CAN attributes.")
    return result


def decode_batch(payload):
    """Validate a fixed-schema OTLP envelope; never decode or filter USB bytes."""
    if type(payload) is not bytes or not payload or len(payload) > MAX_REQUEST_BYTES:
        raise WireError("Invalid CAN request size.")
    request = ExportLogsServiceRequest()
    try:
        request.ParseFromString(payload)
    except DecodeError:
        raise WireError("Malformed OTLP request.") from None
    known = ExportLogsServiceRequest()
    known.CopyFrom(request)
    known.DiscardUnknownFields()
    if known.SerializeToString(deterministic=True) != request.SerializeToString(deterministic=True):
        raise WireError("Unknown CAN envelope fields.")
    if len(request.resource_logs) != 1:
        raise WireError("Expected one CAN resource.")
    resource = request.resource_logs[0]
    if (len(resource.scope_logs) != 1
            or any(field.name not in {"resource", "scope_logs"} for field, _value in resource.ListFields())
            or any(field.name != "attributes" for field, _value in resource.resource.ListFields())):
        raise WireError("Invalid CAN resource envelope.")
    meta = _attrs(resource.resource.attributes, META_KEYS)
    scope = resource.scope_logs[0]
    if (scope.scope.name != "tesla-can" or scope.scope.version != "1"
            or any(field.name not in {"scope", "log_records"} for field, _value in scope.ListFields())
            or any(field.name not in {"name", "version"} for field, _value in scope.scope.ListFields())
            or not 1 <= len(scope.log_records) <= MAX_RECORDS):
        raise WireError("Invalid CAN instrumentation scope.")
    chunks = []
    for record in scope.log_records:
        if (record.body.WhichOneof("value") != "bytes_value"
                or any(field.name not in {"time_unix_nano", "body", "attributes"}
                       for field, _value in record.ListFields())):
            raise WireError("Invalid CAN log record envelope.")
        chunk = _attrs(record.attributes, CHUNK_KEYS - {"data"})
        chunk["data"] = record.body.bytes_value
        if (not _integer(meta.get("started_ns"), 1) or not _integer(chunk["offset_ns"])
                or record.time_unix_nano != meta["started_ns"] + chunk["offset_ns"]):
            raise WireError("Inconsistent CAN event timestamp.")
        chunks.append(chunk)
    _validate_chunks(meta, chunks)
    return meta, chunks


def success_response():
    return ExportLogsServiceResponse().SerializeToString(deterministic=True)


def check_response(payload):
    """Only complete acceptance authorizes removing a queued duplicate."""
    if type(payload) is not bytes or len(payload) > 65536:
        raise WireError("Invalid OTLP response size.")
    response = ExportLogsServiceResponse()
    try:
        response.ParseFromString(payload)
    except DecodeError:
        raise WireError("Malformed OTLP response.") from None
    known = ExportLogsServiceResponse()
    known.CopyFrom(response)
    known.DiscardUnknownFields()
    if known.SerializeToString(deterministic=True) != response.SerializeToString(deterministic=True):
        raise WireError("Unknown OTLP response fields.")
    if response.partial_success.rejected_log_records or response.partial_success.error_message:
        raise PartialSuccess("OTLP partial acceptance or warning; batch retained and blocked.")
