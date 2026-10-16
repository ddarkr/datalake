#!/usr/bin/env python3
"""All-family paired native proof, called by benchmark_greptime_egress.

prove_summary_cases(c, restart=callable, grpc_url='grpc://...') uses the
existing owned S3/meter context. restart must restart that same instance and
volumes and return only when SQL is ready. No services are created here.
Every output cell is compared; SQL/Flight failures are fatal, not skipped.
"""
import base64
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import urllib.parse
import urllib.request
from unittest.mock import patch

from scripts.analytics import aggregate as agg
from scripts.analytics.battery import battery_runtime as battery
from scripts.analytics.battery import battery_rul
from scripts.database import db_init, greptime_flight
from tools import benchmark_ai_incremental as ai
from tools import benchmark_greptime_storage as bench

ROOT = Path(__file__).resolve().parents[1]
TABLES = ('ai_session_summary', 'ai_activity_daily', 'ai_daily_summary',
          'ai_tool_daily', 'ai_log_daily', 'home_agg', 'vehicle_agg',
          'trip_summary', 'charge_session', 'vehicle_analysis')
ORIGIN = dt.datetime(2026, 9, 20)
NOW = ORIGIN + dt.timedelta(days=2, hours=6)
HOUR = 3600000000000


def fixture_module(name):
    spec = importlib.util.spec_from_file_location('p4_fixture_' + name,
                                                  ROOT / 'tests' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ns(value):
    return agg.dt_to_ns(value)


def literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return ai._q(value)


def prove_summary_cases(c, *, restart, grpc_url):
    """Return per-phase meter evidence and full paired result digests.

    Context keys: base, auth=(user,password), snap_port, query_costs, tmp.
    Native OTLP metrics protobuf support and a real Flight endpoint required.
    Caller owns cleanup of this already isolated harness instance.
    """
    from tools.benchmark_greptime_egress import meter_delta, proven_zero_payload
    auth = base64.b64encode(('%s:%s' % c['auth']).encode()).decode()
    prefix = 'p4_summary_' + hashlib.sha256(c['project'].encode()).hexdigest()[:10]
    dbs = {'baseline': prefix + '_ordinary', 'strict': prefix + '_strict'}
    report = c.setdefault('summary_report', {})
    report.update(schema='greptime-summary-local-sst/1', phases={},
                  comparisons={}, databases=dbs, tables=TABLES,
                  clock=NOW.isoformat(), flight={}, checkpoints={})
    report['price_fixture'] = 'tests.test_ai_incremental.PRICE_P1; no external registry'

    def sql(db, statement):
        return ai.sql(c['base'], auth, db, statement)[0]

    def records(db, statement):
        payload = sql(db, statement)
        rec = payload['output'][0]['records']
        return [dict(zip([x['name'] for x in rec['schema']['column_schemas']], row))
                for row in rec['rows']]

    def measure(label, action, kind):
        before = bench.snap_get(c['snap_port'])
        entry = report['phases'][label] = {'kind': kind, 'status': 'running'}
        try:
            result = action()
            entry['status'] = 'success'
            return result
        except Exception as error:
            entry.update(status='failure', error=bench._redact(str(error)))
            raise
        finally:
            after = bench.snap_get(c['snap_port'])
            cost = meter_delta(before, after)
            entry.update(cost)
            bench.validate_meter_errors(after)
            if kind == 'verification_user_query':
                c['query_costs'].append(cost)
            elif kind == 'baseline_analytics_verification':
                c['comparison_costs'].append(cost)
            if kind == 'strict_analytics' and not proven_zero_payload(cost):
                entry['status'] = 'failure'
                raise AssertionError('strict analytics downloaded payload/unknown: ' + label)

    def insert(db, table, cols, rows):
        for start in range(0, len(rows), 150):
            chunk = rows[start:start + 150]
            sql(db, 'INSERT INTO "' + table + '" (' + ','.join('"' + x + '"' for x in cols)
                + ') VALUES ' + ','.join('(' + ','.join(literal(v) for v in row) + ')'
                                         for row in chunk))

    def signals(db, rows):
        cols = ['event_time', 'vehicle', 'path', 'source', 'event_id', 'decode_epoch',
                'source_field', 'value_num', 'value_text', 'value_bool', 'unit', 'quality']
        values = []
        for row in rows:
            stamp = row['event_time_ns']
            values.append([agg.ns_to_sql_lit(stamp).strip("'"), row['vehicle'], row['path'],
                           row['source'], row.get('event_id', row['source_field'] + ':' + str(stamp)),
                           row['decode_epoch'], row['source_field'], row.get('value_num'),
                           row.get('value_text'), row.get('value_bool'), row.get('unit'),
                           row.get('quality')])
        insert(db, 'vehicle_signal', cols, values)

    ai_fixture = fixture_module('test_ai_incremental')

    def ai_rows(db, late=False):
        traces, logs = ai_fixture.late_rows() if late else ai_fixture.base_history()
        for table, rows in (('opentelemetry_traces', traces), ('opentelemetry_logs', logs)):
            for row in rows:
                insert(db, table, list(row), [list(row.values())])

    energy = fixture_module('test_battery_energy')
    electrical = fixture_module('test_battery_electrical')
    conditions = fixture_module('test_battery_conditions')
    alerts = fixture_module('test_battery_alerts')
    rul = fixture_module('test_battery_rul')
    lab_records, metadata = rul.lab_envelope()
    model = battery_rul.train_model(lab_records, metadata, test_battery_ids=['B3'])
    # Each fixture builder returns its own analyzer namespace. Keep scope
    # bindings independent; never flatten calibration keys across analyzers.
    analysis = {
        'energy': energy.cal()['energy'],
        'electrical': electrical.circuit_cfg()['electrical'],
        'alerts': alerts.cfg()['alerts'],
        'conditions': conditions.cfg(
            max_gap_ns=2 * HOUR, domain='synthetic',
            brick_voltage_calibration=conditions.cal('V', 1),
            module_temp_calibration=conditions.mod_cal(),
            pack_current_calibration=conditions.pack_cal())['conditions'],
    }
    # Full-discharge SOC/counters must not share the trip/charge or brick
    # fixture scope: brick_pair starts at 50% while full_discharge starts at
    # 100%, and the trip fixture supplies its own charging counter.
    for calibration in analysis['energy']['field_calibration'].values():
        calibration['vehicle'] = 'battery-energy'
    analysis['rul'] = {'model': model, 'domain': 'nasa-pcoe-lab',
                       'history': rul.history_of(lab_records, 'B1', 8),
                       'history_scope': {'vehicle': 'labcell-B1', 'source': 'lab',
                                         'decode_epoch': 'synthetic-v1'}}
    for cycle, row in enumerate(analysis['rul']['history']):
        row['observed_ns'] = ns(ORIGIN - dt.timedelta(days=10 - cycle))
    shift = ns(ORIGIN) - electrical.ANCHOR
    for key in ('initial_soc_time_ns',):
        analysis['electrical'][key] += shift
    analysis['electrical']['ekf']['init_time_ns'] += shift
    config_file = Path(c['tmp']) / 'p4-summary-battery.json'
    config_file.write_text(json.dumps(analysis, sort_keys=True))

    settings = {'AGG_LOOKBACK_HOURS': '72', 'BATTERY_LOOKBACK_HOURS': '72',
                'HOME_RAW_TTL': '', 'OTEL_TTL': '', 'VEHICLE_ID': '',
                'HOME_AGG_SOURCES': json.dumps([{'table': 'p4_home', 'time_col': 'ts',
                                               'entity_col': 'entity', 'value_col': 'value'}]),
                'BATTERY_ANALYSIS_CONFIG': str(config_file),
                'GREPTIME_GRPC_URL': grpc_url, 'GREPTIME_USER': c['auth'][0],
                'GREPTIME_PASSWORD': c['auth'][1], 'AGG_AI_FULL_REBUILD': '0',
                'VEHICLE_SPEED_PATH': 'Speed', 'VEHICLE_SOC_PATH': 'Soc',
                'VEHICLE_DRIVE_ENERGY_PATH': 'LifetimeEnergyUsed',
                'VEHICLE_CHARGE_ENERGY_PATH': 'DCChargingEnergyIn',
                'VEHICLE_CHARGING_POWER_PATH': 'Power',
                'VEHICLE_CHARGING_PATH': 'Charging'}

    def dirty(db, generation):
        rows = [[(ORIGIN + dt.timedelta(hours=h)).isoformat(' '), 'v', 'can', 'e1',
                 generation, NOW.isoformat(' ')] for h in range(5)]
        insert(db, 'vehicle_signal_dirty', ['window_start', 'vehicle', 'source',
               'decode_epoch', 'generation', 'received_at'], rows)
    def metrics(db, late=False, instrument='coding_agent.lines_of_code.count'):
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        request = ExportMetricsServiceRequest()
        group = request.resource_metrics.add().scope_metrics.add()
        group.scope.name = 'p4-native-fixture'
        for tempo in (1, 2):
            metric = group.metrics.add(name=instrument)
            metric.sum.aggregation_temporality = tempo
            metric.sum.is_monotonic = True
            for minute, value in ((1, 3), (2, 8)):
                point = metric.sum.data_points.add(
                    time_unix_nano=ns(ORIGIN + dt.timedelta(hours=10, minutes=minute + int(late) * 5)),
                    start_time_unix_nano=ns(ORIGIN + dt.timedelta(hours=10)), as_int=value)
                for key, val in {'coding_agent.client': 'codex',
                                 'coding_agent.session.id': 'hist-0', 'type': 'added',
                                 'decision': 'accept', 'stream': str(tempo)}.items():
                    point.attributes.add(key=key).value.string_value = val
                point.attributes.add(key='datalake_temporality').value.int_value = tempo
                point.attributes.add(key='datalake_start_time_unix_nano').value.int_value = ns(ORIGIN + dt.timedelta(hours=10))
        req = urllib.request.Request(c['base'].rstrip('/') + '/v1/otlp/v1/metrics?db='
                                     + urllib.parse.quote(db), data=request.SerializeToString(),
                                     headers={'Content-Type': 'application/x-protobuf',
                                              'Authorization': 'Basic ' + auth}, method='POST')
        with urllib.request.urlopen(req, timeout=120) as response:
            response.read()

    def missing_capability():
        real_open = urllib.request.urlopen
        attempts = []
        def absent_endpoint(req, *args, **kwargs):
            attempts.append(req.full_url)
            if '/v1/sql' in req.full_url:
                raise AssertionError('strict capability absence retried ordinary SQL')
            req.full_url = req.full_url.replace('/v1/local-sql', '/v1/p4-missing-local-sql')
            return real_open(req, *args, **kwargs)
        with patch.dict(os.environ, dict(settings, AGG_LOCAL_SST_ONLY='1')), \
             patch.object(urllib.request, 'urlopen', absent_endpoint), \
             patch.object(agg, 'utcnow', return_value=NOW):
            cfg = agg.load_cfg()
            for section in (agg.ai_section, agg.vehicle_history_section):
                previous = len(attempts)
                try:
                    section((c['base'], auth, dbs['strict']), cfg)
                except agg.SqlError:
                    pass
                else:
                    raise AssertionError('absent strict endpoint did not fail ' + section.__name__)
                if len(attempts) != previous + 1:
                    raise AssertionError(('strict capability retry', attempts))
        report['missing_capability'] = {'failed_closed': True, 'attempts': len(attempts)}


    def fixture(db):
        sql('public', 'CREATE DATABASE "' + db + '"')
        for _, ddl in db_init.ddl_statements(''):
            sql(db, ddl)
        sql(db, 'CREATE TABLE p4_home(ts TIMESTAMP(9) TIME INDEX, entity STRING, '
                '"value" DOUBLE, alternate DOUBLE, PRIMARY KEY(entity))')
        ai_rows(db)
        tool = {'event.name': 'codex.tool_result', 'session.id': 'sA',
                'gen_ai.tool.call.id': 'p4-tool', 'tool_name': 'shell'}
        insert(db, 'opentelemetry_logs', ['timestamp', 'severity_text', 'severity_number',
               'scope_name', 'log_attributes', 'resource_attributes'],
               [[ORIGIN.isoformat(' '), 'ERROR', 17, 'p4-tool', json.dumps(tool), '{}'],
                [(ORIGIN + dt.timedelta(days=1)).isoformat(' '), 'INFO', 9, 'p4-log', '{}', '{}']])
        insert(db, 'p4_home', ['ts', 'entity', 'value', 'alternate'],
               [[(ORIGIN + dt.timedelta(minutes=m)).isoformat(' '), entity, value, 2]
                for m, value in ((1, 2), (2, 4), (61, None), (1441, 8))
                for entity in ('a', 'b')])
        rows = [dict(row, vehicle='battery-energy')
                for row in energy.full_discharge(start=ns(ORIGIN))]
        rows += electrical.circuit_rows()
        for row in rows:
            if row['vehicle'] == 'battery-circuit':
                row['event_time_ns'] += shift
        for source in ('fleet', 'can'):
            selected = [dict(r, source=source, path=r['source_field']) for r in rows]
            selected += [dict(r, source=source, path=r['source_field'])
                         for r in conditions.brick_pair(ns(ORIGIN), 4.1, 4.0)]
            for minute in range(61):
                stamp = ns(ORIGIN + dt.timedelta(minutes=minute))
                for field, val, unit in [('Speed', 36 if 1 <= minute <= 10 else 0, 'km/h'),
                                         ('Power', 7 if 20 <= minute <= 40 else 0, 'kW'),
                                         ('Charging', 1 if 20 <= minute <= 40 else 0, None),
                                         ('DCChargingEnergyIn', 100 + max(0, min(20, minute - 20)) / 10, 'kWh')]:
                    selected.append(dict(event_time_ns=stamp, vehicle='v', path=field,
                                         source=source, decode_epoch='e1', source_field=field,
                                         value_num=None if field == 'Charging' else val,
                                         value_bool=bool(val) if field == 'Charging' else None,
                                         unit=unit, quality='valid'))
            signals(db, selected)
            insert(db, 'vehicle_event', ['event_time', 'vehicle', 'event_type', 'name',
                   'source', 'event_id', 'decode_epoch', 'started_at', 'ended_at', 'is_active'],
                   [[ORIGIN.isoformat(' '), 'v', 'alerts', 'bms_a035', source, 'alert-1', 'e1',
                     ORIGIN.isoformat(' '), (ORIGIN + dt.timedelta(minutes=30)).isoformat(' '), False]])
        signals(db, [dict(event_time_ns=ns(ORIGIN), vehicle='labcell-B1', source='lab',
                          decode_epoch='synthetic-v1', path='Soc', source_field='Soc',
                          value_num=50, unit='%')])
        dirty(db, 'generation-1')
        metrics(db)
        for row in records(db, "SELECT table_name FROM information_schema.tables WHERE table_schema=" + literal(db)):
            sql(db, "ADMIN flush_table('" + row['table_name'] + "')")

    for side, db in dbs.items():
        measure(side + '_fixture_flush', lambda db=db: fixture(db), 'fixture_nonquery')

    def snapshot(db):
        return {table: sorted(records(db, 'SELECT * FROM "' + table + '"'),
                              key=lambda r: json.dumps(r, sort_keys=True)) for table in TABLES}

    def compare(label):
        sides = {side: measure(label + '_' + side + '_snapshot',
                              lambda db=db: snapshot(db), 'verification_user_query')
                 for side, db in dbs.items()}
        for table in TABLES:
            if sides['baseline'][table] != sides['strict'][table]:
                raise AssertionError(json.dumps({'phase': label, 'table': table,
                                                'baseline': sides['baseline'][table],
                                                'strict': sides['strict'][table]}, sort_keys=True))
        report['comparisons'][label] = {table: {'rows': len(rows),
            'sha256': hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}
            for table, rows in sides['strict'].items()}
        return sides['strict']

    def run(side, label):
        db = dbs[side]
        strict = side == 'strict'
        with patch.dict(os.environ, dict(settings, AGG_LOCAL_SST_ONLY=str(int(strict)))):
            cfg = agg.load_cfg()
            real_battery = battery.run_battery
            real_flight = greptime_flight.flight_query
            calls = []
            def flight(*args, **kwargs):
                calls.append({'lower': bool(kwargs.get('lower')),
                              'strict': kwargs.get('local_sst_only', os.environ.get('AGG_LOCAL_SST_ONLY') == '1')})
                return real_flight(*args, **kwargs)
            def battery_clock(ctx, conf, **kwargs):
                return real_battery(ctx, conf, now_ns=ns(NOW))
            real_urlopen = urllib.request.urlopen
            def guarded_urlopen(req, *args, **kwargs):
                url = req.full_url if isinstance(req, urllib.request.Request) else str(req)
                if strict and '/v1/sql' in url:
                    raise AssertionError('ordinary SQL retry during strict analytics')
                return real_urlopen(req, *args, **kwargs)
            def action():
                with patch.object(agg, 'utcnow', return_value=NOW), \
                     patch.object(agg, 'load_price_table', return_value=ai_fixture.PRICE_P1), \
                     patch.object(battery, 'run_battery', battery_clock), \
                     patch.object(greptime_flight, 'flight_query', flight), \
                     patch.object(urllib.request, 'urlopen', guarded_urlopen):
                    counts, failed, _ = agg.run_all((c['base'], auth, db), cfg)
                    if failed:
                        raise AssertionError('failed native sections: ' + repr(failed))
                    # Scheduled CAN battery is intentionally disabled. Exercise its
                    # actual analyzers explicitly rather than changing scheduling.
                    explicit = dict(cfg, battery_exclude_sources=(), battery_exclude_scopes=())
                    real_battery((c['base'], auth, db), explicit, now_ns=ns(NOW))
                    return counts
            counts = measure(label + '_' + side, action,
                             'strict_analytics' if strict else 'baseline_analytics_verification')
            if not calls or (strict and not all(x['strict'] for x in calls)):
                raise AssertionError('native typed Flight path not exercised: ' + repr(calls))
            report['flight'][label + '_' + side] = calls
            return counts

    def both(label):
        counts = {side: run(side, label) for side in dbs}
        snap = compare(label)
        report['checkpoints'][label] = counts
        for side, db in dbs.items():
            state = measure(label + '_' + side + '_fence', lambda db=db:
                            records(db, 'SELECT fence FROM ai_aggregate_state'),
                            'verification_user_query')
            if not state or not all(row.get('fence') and json.loads(row['fence']) for row in state):
                raise AssertionError('native AI fence absent; full-scan fallback is not proof')
        return snap

    measure('flushed_fixture_same_volume_reopen', restart, 'restart_nonquery')
    first = both('sst_initial')
    if both('cached_sst_repeat') != first:
        raise AssertionError('cached repeat changed full user output cells')
    def state_snapshot():
        return {table: sorted(records(dbs['strict'], 'SELECT * FROM ' + table),
                              key=lambda r: json.dumps(r, sort_keys=True))
                for table in ('ai_aggregate_state', 'vehicle_aggregate_checkpoint')}
    before_failure = measure('failure_before_state', state_snapshot, 'verification_user_query')
    measure('missing_capability_no_retry', missing_capability, 'capability_probe')
    after_failure = measure('failure_after_state', state_snapshot, 'verification_user_query')
    if before_failure != after_failure:
        raise AssertionError('strict capability failure advanced AI/CAN state')
    report['missing_capability']['state_unchanged'] = True
    if sum(r.get('lines_added') or 0 for r in first['ai_activity_daily']) != 19:
        raise AssertionError('native delta(3+8) + cumulative(8) activity oracle failed')
    if not any(r['tool_name'] == 'shell' for r in first['ai_tool_daily']):
        raise AssertionError('native tool log output missing')
    if not any(r['session_id'] == 'sA' for r in first['ai_session_summary']):
        raise AssertionError('cross-day native AI session fixture missing')
    if not any(r['severity_text'] == 'ERROR' for r in first['ai_log_daily']):
        raise AssertionError('native ERROR log daily output missing')
    resolutions = {r['resolution'] for r in first['home_agg']}
    if resolutions != {'5m', '1h', '1d'}:
        raise AssertionError(('Home resolution coverage', resolutions))
    for table in ('trip_summary', 'charge_session'):
        if {r['source'] for r in first[table]} != {'can', 'fleet'}:
            raise AssertionError((table, 'missing CAN/nonCAN segment coverage'))
    modules = {r['analysis_id'].split(':')[0] for r in first['vehicle_analysis']}
    for suffix in battery.CONFIG_SUFFIXES:
        if not any(suffix in name for name in modules):
            raise AssertionError(('missing battery analyzer', suffix, modules))
    def expected_battery(metric, vehicle, source, epoch, status, value=None):
        candidates = [row for row in first['vehicle_analysis']
                      if row['metric'] == metric and row['vehicle'] == vehicle
                      and row['source'] == source and row['decode_epoch'] == epoch]
        rows = [row for row in candidates
                if row['status'] == status and row['value'] is not None]
        if value is not None:
            rows = [row for row in rows if abs(float(row['value']) - value) < 1e-8]
        if not rows:
            raise AssertionError(('battery known-answer fixture failed', metric,
                                  vehicle, source, epoch, status, value,
                                  'candidates', candidates[:8]))
    expected_battery('battery.conditions.brick_spread_v', 'v', 'fleet', 'e1',
                     'derived', .1)
    expected_battery('battery.energy.full_usable_capacity_kwh', 'battery-energy', 'fleet', 'e1',
                     'derived', 40)
    expected_battery('battery.electrical.soc_coulomb_pct', 'battery-circuit', 'fleet',
                     'circuit-v1', 'estimated', 49.91666666666661)
    expected_battery('battery.alerts.episode', 'v', 'fleet', 'e1', 'reported', 1800)
    expected_battery('battery.rul.baseline_cycles_remaining', 'labcell-B1', 'lab',
                     'synthetic-v1', 'estimated', model['baseline_rul_cycles'])
    rul_rows = [r for r in first['vehicle_analysis']
                if 'rul' in r['analysis_id'] and r['vehicle'] == 'labcell-B1'
                and r['source'] == 'lab' and r['decode_epoch'] == 'synthetic-v1'
                and r['metric'] == 'battery.rul.cycles_remaining'
                and r['status'] == 'estimated' and r['value'] is not None
                and r['value'] > 0 and r['model_version'] == model['model_version']
                and r['evidence_count'] == 8
                and r['sample_count'] == model['train']['n_samples']
                and 'unvalidated' in (r['reason'] or '') and r['uncertainty'] is None]
    if not rul_rows:
        raise AssertionError('real scoped shared RUL model did not produce a lab estimate')

    def late(db):
        ai_rows(db, late=True)
        metrics(db, late=True)
        for source in ('can', 'fleet'):
            rows = []
            for minute, field, value in ((0, 'Speed', 36), (19, 'Charging', 1),
                                         (40, 'Power', None)):
                stamp = ns(ORIGIN + dt.timedelta(minutes=minute))
                rows.append(dict(event_time_ns=stamp, vehicle='v', source=source,
                                 decode_epoch='e1', path=field, source_field=field,
                                 value_num=None if field == 'Charging' else value,
                                 value_bool=bool(value) if field == 'Charging' else None,
                                 unit={'Speed': 'km/h', 'Power': 'kW'}.get(field),
                                 quality='valid'))
            signals(db, rows)
        dirty(db, 'generation-2')
    for side, db in dbs.items():
        measure(side + '_late_memtable_latest_null', lambda db=db: late(db), 'fixture_nonquery')
    corrected = both('late_memtable_boundary_latest_null')
    if corrected['trip_summary'] == first['trip_summary']:
        raise AssertionError('late start boundary failed to change trips')
    if corrected['charge_session'] == first['charge_session']:
        raise AssertionError('late charging boundary failed to change charge sessions')
    for db in dbs.values():
        rows = measure(db + '_null_oracle', lambda db=db: records(db,
            "SELECT value_num FROM vehicle_signal WHERE path='Power' AND event_time="
            + literal((ORIGIN + dt.timedelta(minutes=40)).isoformat(' '))), 'verification_user_query')
        if len(rows) != 2 or any(row['value_num'] is not None for row in rows):
            raise AssertionError(('latest NULL resurrected', rows))
    replay = both('unchanged_generation')
    if replay != corrected:
        raise AssertionError('unchanged input/config changed user output cells')
    if any(v.get('vehicle_history') != 0 for v in report['checkpoints']['unchanged_generation'].values()):
        raise AssertionError('unchanged CAN generation was processed again')

    settings['HOME_AGG_SOURCES'] = settings['HOME_AGG_SOURCES'].replace('"value"', '"alternate"')
    analysis['alerts']['dictionary_version'] = 'p4-config-2'
    config_file.write_text(json.dumps(analysis, sort_keys=True))
    both('config_without_can_notification')
    if any(v.get('vehicle_history') != 0 for v in report['checkpoints']['config_without_can_notification'].values()):
        raise AssertionError('config alone incorrectly replayed CAN generation')
    for side, db in dbs.items():
        measure(side + '_new_config_generation', lambda db=db: (dirty(db, 'generation-3'),
                metrics(db, instrument='coding_agent.commit.count')), 'fixture_nonquery')
    before_restart = both('config_new_can_generation_metric_registry')
    measure('same_volume_restart', restart, 'restart_nonquery')
    if both('same_volume_restart') != before_restart:
        raise AssertionError('same-volume restart changed full user output cells')
    for side, db in dbs.items():
        measure(side + '_repair_generation', lambda db=db: (
            dirty(db, 'generation-4'), sql(db, "ADMIN flush_table('vehicle_signal')")),
            'fixture_nonquery')
    def raw_manifest():
        return sorted(records(dbs['strict'],
            "SELECT file_id,file_size,index_file_path,index_file_size FROM "
            "information_schema.ssts_manifest WHERE visible=true AND table_id IN "
            "(SELECT table_id FROM information_schema.tables WHERE table_schema="
            + literal(dbs['strict']) + " AND table_name='vehicle_signal')"),
            key=lambda row: row['file_id'])
    sources = measure('missing_input_manifest', raw_manifest, 'metadata_nonquery')
    if not sources:
        raise AssertionError('missing-input summary proof requires flushed native SSTs')
    before_failure = measure('missing_input_before_state', state_snapshot, 'verification_user_query')
    ids = [row['file_id'] for row in sources]
    measure('summary_input_offline_hold', lambda: c['mirror_fault'](ids, False), 'fault_nonquery')
    try:
        def reject_missing_input():
            with patch.dict(os.environ, dict(settings, AGG_LOCAL_SST_ONLY='1')), \
                 patch.object(agg, 'utcnow', return_value=NOW):
                try:
                    agg.vehicle_history_section((c['base'], auth, dbs['strict']), agg.load_cfg())
                except agg.SqlError:
                    return
                raise AssertionError('missing local SST did not reject pending CAN work')
        measure('missing_input_strict_analytics', reject_missing_input, 'strict_analytics')
        if measure('missing_input_after_state', state_snapshot, 'verification_user_query') != before_failure:
            raise AssertionError('missing local SST advanced durable summary checkpoints')
        if measure('missing_input_after_manifest', raw_manifest, 'metadata_nonquery') != sources:
            raise AssertionError('missing local SST changed canonical input manifest')
    finally:
        measure('summary_input_offline_restore', lambda: c['mirror_fault'](ids, True), 'fault_nonquery')
    if both('missing_input_repaired') != before_restart:
        raise AssertionError('repaired local input changed summary output cells')
    if any(v.get('vehicle_history', 0) <= 0 for v in report['checkpoints']['missing_input_repaired'].values()):
        raise AssertionError('pending CAN work did not resume after local input repair')
    report['missing_input'] = {'failed_closed': True, 'state_unchanged': True,
                               'manifest_unchanged': True, 'repair_completed': True}
    return report
