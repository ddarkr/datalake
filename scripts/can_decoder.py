"""Server-only CAN decoding; the receiver owns the unmodified raw archive."""

import collections
import hashlib
import json
import math
from pathlib import Path
import re
import time

import cantools


SHORT = re.compile(rb'([tr])([0-9A-Fa-f]{3})([0-8])([0-9A-Fa-f]*)')
EXTENDED = re.compile(rb'([TR])([0-9A-Fa-f]{8})([0-8])([0-9A-Fa-f]*)')
CONTROL = re.compile(rb'(?:[zZCO]|S6|V[0-9A-Fa-f]{4}|[EF][0-9A-Fa-f]{2}|N[0-9A-Fa-f]{4}|\x07)')
BUFFER_LIMIT = 4096
DECODER_VERSION = 'custom-can-v1'
STRUCTURAL_FIELDS = (
    'source', 'id', 'signal', 'source_signal', 'kind', 'start_bit',
    'bit_length', 'byte_order', 'signed', 'scale', 'offset',
    'is_multiplexer', 'multiplexer_signal', 'multiplexer_ids',
    'actual_dbc_length', 'unit', 'source_unit', 'choices',
)
COUNT_KEYS = (
    'bytes', 'chunks', 'records', 'frames', 'decoded_frames', 'rows',
    'unknown_frames', 'remote_frames', 'malformed_records', 'dlc_mismatch',
    'unsupported_mux', 'decode_errors', 'control_records', 'oversize_records',
    'discarded_bytes', 'tail_bytes', 'unknown_enum_signals',
)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _choice_quality(label):
    tokens = re.split(r'[^A-Z0-9]+', label.upper())
    if 'INVALID' in tokens:
        return 'invalid'
    if 'SNA' in tokens or 'UNAVAILABLE' in tokens or 'NOT_AVAILABLE' in label.upper():
        return 'unavailable'
    return 'reported_unverified'


class Decoder:
    def __init__(self, dbc_path, definitions_path):
        dbc_bytes = Path(dbc_path).read_bytes()
        database = cantools.database.load_string(dbc_bytes.decode('utf-8'),
                                                 database_format='dbc', strict=True)
        definitions = json.loads(Path(definitions_path).read_text(encoding='utf-8'))
        self.messages = {(message.frame_id, message.is_extended_frame): message
                         for message in database.messages}
        self.definitions = {}
        structural = []
        for definition in definitions['signals']:
            # Only structural fields enter the mapping hash: never observed values or paths.
            item = {key: definition[key] for key in STRUCTURAL_FIELDS}
            identifier = int(item['id'], 16)
            extended = definition.get('is_extended_frame', False)
            item['is_extended_frame'] = extended
            message = self.messages.get((identifier, extended))
            if message is None:
                raise ValueError('Definition refers to an absent DBC message.')
            signal = message.get_signal_by_name(item['signal'])
            geometry = (signal.start, signal.length, signal.byte_order, signal.is_signed,
                        signal.scale, signal.offset, signal.is_multiplexer,
                        signal.multiplexer_signal, signal.multiplexer_ids, message.length)
            expected = tuple(item[key] for key in (
                'start_bit', 'bit_length', 'byte_order', 'signed', 'scale', 'offset',
                'is_multiplexer', 'multiplexer_signal', 'multiplexer_ids', 'actual_dbc_length'))
            if geometry != expected:
                raise ValueError('Definition geometry differs from strict DBC.')
            choices = {str(key): str(value) for key, value in (signal.choices or {}).items()}
            if choices != item['choices'] or signal.unit != item['unit']:
                raise ValueError('Definition choices or unit differ from strict DBC.')
            # Evidence supplies actual upstream pins; never substitute content hashes for commits.
            pin = re.search(r'\b' + re.escape(item['source']) + r'@([0-9a-f]{7,40})\b',
                            definition.get('evidence', ''))
            item['source_commit'] = pin.group(1) if pin else None
            key = (identifier, extended, item['signal'])
            if key in self.definitions:
                raise ValueError('Duplicate signal definition.')
            self.definitions[key] = item
            structural.append(item)
        all_signals = {(message.frame_id, message.is_extended_frame, signal.name)
                       for message in database.messages for signal in message.signals}
        if set(self.definitions) != all_signals:
            raise ValueError('Every DBC signal needs an explicit kind definition.')
        self.override_version = definitions['revision']
        structural.sort(key=lambda item: (int(item['id'], 16), item['is_extended_frame'], item['signal']))
        self.mapping_revision = DECODER_VERSION + ':' + _hash(structural)
        self.epoch = _hash({'dbc_sha256': hashlib.sha256(dbc_bytes).hexdigest(),
                            'mapping_revision': self.mapping_revision,
                            'override_version': self.override_version,
                            'decoder_version': DECODER_VERSION})

    def decode(self, meta, chunk, state):
        """Decode one sequential raw chunk; return rows, JSON state, and count deltas.

        event_time is arrival of the chunk completing a frame, not a bus timestamp.
        Oversize records are discarded until their CR, never resynchronized mid-record.
        All discarded/unknown bytes remain in the receiver's independent raw archive.
        """
        identity = {key: meta[key] for key in ('vehicle', 'collector_id', 'session_id', 'started_ns')}
        if state is None:
            state = dict(identity, version=1, epoch=self.epoch, next_seq=0,
                         last_offset_ns=-1, next_frame=0, tail_hex='', discarding=False)
        else:
            state = dict(state)
        if (state.get('version') != 1 or state.get('epoch') != self.epoch
                or any(state.get(key) != value for key, value in identity.items())):
            raise ValueError('Decoder state belongs to a different session or epoch.')
        if (type(chunk['seq']) is not int or chunk['seq'] != state['next_seq']
                or type(chunk['offset_ns']) is not int or chunk['offset_ns'] < 0
                or chunk['offset_ns'] < state['last_offset_ns']):
            raise ValueError('Decoder chunk sequence or host time is not monotonic.')
        if chunk['phase'] not in ('capture', 'close_drain', 'control'):
            raise ValueError('Unsupported serial chunk phase.')
        data = chunk['data']
        if not isinstance(data, bytes) or len(data) > 65536:
            raise ValueError('Decoder requires a bounded raw byte chunk.')
        tail = bytes.fromhex(state['tail_hex'])
        if len(tail) > BUFFER_LIMIT:
            raise ValueError('Persisted serial tail exceeds its bound.')
        counts = collections.Counter({key: 0 for key in COUNT_KEYS})
        counts.update(bytes=len(data), chunks=1)
        rows = []
        event_time = meta['started_ns'] + chunk['offset_ns']
        ingest_time = time.time_ns()
        envelope_id = _hash([meta['vehicle'], meta['collector_id'], meta['session_id'], chunk['seq']])
        parts = data.split(b'\r')
        for index, part in enumerate(parts):
            complete = index < len(parts) - 1
            if state['discarding']:
                counts['discarded_bytes'] += len(part)
                if complete:
                    counts['records'] += 1
                    state['discarding'] = False
                continue
            record = tail + part
            tail = b''
            if len(record) > BUFFER_LIMIT:
                counts['oversize_records'] += 1
                counts['malformed_records'] += 1
                counts['discarded_bytes'] += len(record)
                if complete:
                    counts['records'] += 1
                else:
                    state['discarding'] = True
                continue
            if not complete:
                tail = record
                continue
            record = record.strip(b'\n')
            if not record:
                counts['control_records'] += 1
                continue
            counts['records'] += 1
            if CONTROL.fullmatch(record):
                counts['control_records'] += 1
                continue
            match = (EXTENDED if record[:1] in (b'T', b'R') else SHORT).fullmatch(record)
            if match is None:
                counts['malformed_records'] += 1
                continue
            kind, address, dlc_text, payload_hex = match.groups()
            identifier, dlc = int(address, 16), int(dlc_text)
            extended = kind in (b'T', b'R')
            remote = kind in (b'r', b'R')
            if (identifier > (0x1fffffff if extended else 0x7ff)
                    or len(payload_hex) != (0 if remote else dlc * 2)):
                counts['malformed_records'] += 1
                continue
            ordinal = state['next_frame']
            state['next_frame'] += 1
            counts['frames'] += 1
            if remote:
                counts['remote_frames'] += 1
                continue
            message = self.messages.get((identifier, extended))
            if message is None:
                counts['unknown_frames'] += 1
                continue
            if dlc != message.length:
                counts['dlc_mismatch'] += 1
                continue
            payload = bytes.fromhex(payload_hex.decode('ascii'))
            try:
                decoded = message.decode(payload, decode_choices=False, scaling=False,
                                         allow_truncated=False, allow_excess=False)
            except cantools.database.errors.DecodeError:
                counts['unsupported_mux' if message.is_multiplexed() else 'decode_errors'] += 1
                continue
            counts['decoded_frames'] += 1
            for name, raw in decoded.items():
                definition = self.definitions[(identifier, extended, name)]
                if definition['kind'] != 'data':
                    continue
                value_num = value_text = None
                choices = definition['choices']
                label = choices.get(str(raw))
                quality = 'reported_unverified'
                if label is not None:
                    value_text, quality = label, _choice_quality(label)
                elif any(_choice_quality(choice) == 'reported_unverified' for choice in choices.values()):
                    value_text, quality = f'UNKNOWN({raw})', 'unknown_enum'
                    counts['unknown_enum_signals'] += 1
                else:
                    value_num = raw * definition['scale'] + definition['offset']
                    if not math.isfinite(value_num):
                        counts['decode_errors'] += 1
                        continue
                path = f'Vehicle.CAN.x{identifier:03X}.{name}'
                unit = {'kph': 'km/h', 'C': '°C', 'DegC': '°C', 'rpm': 'RPM',
                        'KWh': 'kWh'}.get(definition['source_unit'], definition['source_unit'])
                rows.append({
                    'event_time': event_time, 'vehicle': meta['vehicle'], 'path': path,
                    'source': 'can', 'event_id': _hash([
                        meta['vehicle'], meta['collector_id'], meta['session_id'],
                        ordinal, path, self.epoch, event_time]),
                    'decode_epoch': self.epoch, 'value_num': value_num,
                    'value_text': value_text, 'value_bool': None, 'unit': unit,
                    'vss_version': None, 'vehicle_firmware': meta['vehicle_firmware'] or None,
                    'dbc_primary_commit': definition['source_commit'],
                    'dbc_supplemental_commit': None, 'dbc_override_version': self.override_version,
                    'dbc_override_commit': None, 'mapping_revision': self.mapping_revision,
                    'collector_version': 'tesla-can/1', 'ingest_time': ingest_time,
                    'source_system': 'can', 'source_field': definition['source_signal'],
                    'collector_id': meta['collector_id'], 'source_is_resend': False,
                    'quality': quality, 'envelope_id': envelope_id,
                    'config_version': self.epoch, 'connectivity': None,
                })
        state.update(next_seq=chunk['seq'] + 1, last_offset_ns=chunk['offset_ns'], tail_hex=tail.hex())
        counts['tail_bytes'] = len(tail)
        counts['rows'] = len(rows)
        return rows, state, dict(counts)
