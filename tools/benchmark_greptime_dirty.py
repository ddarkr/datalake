#!/usr/bin/env python3
"""Stock S3-primary cold dirty-hour attribution; synthetic data, bounded cache.

python tools/benchmark_greptime_dirty.py --json-out /tmp/dirty-egress.json
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import sys
import time
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools import benchmark_greptime_egress as e
from scripts.analytics import aggregate as a
from scripts.database import db_init


def scenario(c, refill=True):
    sql, records, phase = c['sql'], c['records'], c['phase']
    config = os.path.join(c['tmp'], 'greptimedb.toml')
    with open(config) as source:
        toml = source.read()
    before = 'enable_refill_cache_on_read = true'
    assert toml.count(before) == 1
    with open(config, 'w') as output:
        output.write(toml.replace(before, 'enable_refill_cache_on_read = ' + str(refill).lower()))
    c['command']('restart', 'greptimedb')
    e.b.gwait(c['base'], auth=c['auth'])
    c['result']['settings']['enable_refill_cache_on_read'] = refill
    ctx = (c['base'], base64.b64encode(('%s:%s' % c['auth']).encode()).decode(), 'public')
    cfg = dict(vehicle='proof-car', lookback_h=3, max_rows=200000,
               paths=dict(speed='Vehicle.Speed', soc='', drive_energy='', charge_energy='', power='', charging=''),
               trip_gap_min=10, charge_gap_min=30, trip_min_speed_kph=1.,
               signal_max_age_s=300, battery_config='', interval_s=300)
    keep = {'vehicle_signal', 'vehicle_agg', 'trip_summary', 'charge_session',
            'vehicle_signal_dirty', 'vehicle_aggregate_checkpoint'}
    for name, ddl in db_init.ddl_statements(''):
        if name in keep:
            sql(ddl)
    origin = dt.datetime(2026, 1, 2)
    hours = 16
    per_hour = c['rows'] // hours
    def ingest():
        before = e.b.compaction_metrics(c['base'], c['auth'])
        for hour in range(hours):
            for start in range(0, per_hour, 500):
                values = []
                for i in range(start, min(start + 500, per_hour)):
                    at = origin + dt.timedelta(hours=hour, seconds=i * 3600 / per_hour)
                    entropy = hashlib.shake_256(('%s:%s' % (hour, i)).encode()).hexdigest(128)
                    # A short speed episode plus high-entropy unrelated numeric paths.
                    path = 'Vehicle.Speed' if i < 60 else 'Entropy.' + str(i % 8)
                    speed = 36 if 1 <= i <= 10 else 0
                    value = speed if i < 60 else i % 101
                    values.append("('%s','proof-car','%s','can','%s','proof-epoch',%s,'km/h','Speed','valid')" % (at, path, entropy, value))
                sql('INSERT INTO vehicle_signal(event_time,vehicle,path,source,event_id,decode_epoch,value_num,unit,source_field,quality) VALUES ' + ','.join(values))
            at = origin + dt.timedelta(hours=hour)
            sql("INSERT INTO vehicle_signal_dirty VALUES ('%s','proof-car','can','proof-epoch','generation-1','%s')" % (at, at))
            sql("ADMIN flush_table('vehicle_signal')")
        time.sleep(5)
        files = e.b.s3_list_all(c['target'], c['access'], c['secret'])
        retained = sum(n for k, n in files if k.endswith('.parquet'))
        assert retained > e.size_bytes(c['cache']), (retained, c['cache'])
        after = e.b.compaction_metrics(c['base'], c['auth'])
        assert after['completed'] > before['completed'], (before, after)
        assert after['failures'] == before['failures'], (before, after)
        return dict(raw_rows=hours * per_hour, dirty_hours=hours, retained_parquet_bytes=retained,
                    cache_bytes=e.size_bytes(c['cache']), compaction_before=before, compaction_after=after)
    phase('ingest_background_compaction', 'nonquery', ingest)
    phase('idle', 'nonquery', lambda: time.sleep(15))
    def restart():
        c['command']('restart', 'greptimedb')
        e.b.gwait(c['base'], auth=c['auth'])
    phase('same_volume_restart', 'nonquery', restart)
    def cold():
        e.b.reset_owned_cache(c['tmp'], c['project'], c['env'], c['evidence'])
        e.b.gwait(c['base'], auth=c['auth'])
    phase('cache_eviction_restart', 'nonquery', cold)
    now = origin + dt.timedelta(hours=hours + 1)
    # Wrap the real section boundaries, retaining the actual coordinator/checkpoint.
    rollup, trip = a._rollup_hour_windows, a._trip_charge_scope_windows
    counter = {'rollup': 0, 'trip': 0}
    def wrapped(kind, fn):
        def run(*args, **kwargs):
            counter[kind] += 1
            value = []
            def action():
                value.append(fn(*args, **kwargs))
                return repr(value[0])
            phase('%s_%02d' % (kind, counter[kind]), 'analytics_nonquery', action)
            return value[0]
        return run
    with patch.object(a, '_rollup_hour_windows', wrapped('rollup', rollup)), patch.object(a, '_trip_charge_scope_windows', wrapped('trip', trip)):
        completed = phase('dirty_coordinator', 'analytics_nonquery', lambda: a.vehicle_history_section(ctx, cfg, now=now))
    assert completed == hours, completed
    repeated = phase('unchanged_generation', 'analytics_nonquery', lambda: a.vehicle_history_section(ctx, cfg, now=now))
    assert repeated == 0, repeated
    def oracle():
        raw = records("SELECT date_bin('1h'::INTERVAL,event_time) AS window_start,vehicle,path,source,decode_epoch,AVG(value_num) AS avg_value,MIN(value_num) AS min_value,MAX(value_num) AS max_value,COUNT(*) AS sample_count,MAX(unit) AS unit FROM vehicle_signal WHERE value_num IS NOT NULL GROUP BY date_bin('1h'::INTERVAL,event_time),vehicle,path,source,decode_epoch ORDER BY window_start,path")
        derived = records("SELECT window_start,vehicle,path,source,decode_epoch,avg_value,min_value,max_value,sample_count,unit FROM vehicle_agg WHERE resolution='1h' ORDER BY window_start,path")
        assert raw == derived, 'raw/hourly output mismatch'
        return dict(exact_hour_groups=len(raw), trip_rows=len(records('SELECT * FROM trip_summary')),
                    checkpoint_rows=len(records('SELECT * FROM vehicle_aggregate_checkpoint')))
    phase('ui_output_parity', 'user_query', oracle)
    def correction():
        sql("INSERT INTO vehicle_signal(event_time,vehicle,path,source,event_id,decode_epoch,value_num,unit,source_field,quality) VALUES ('2026-01-02 00:45:00','proof-car','Corrected','can','late','proof-epoch',17,'V','Late','valid')")
        sql("INSERT INTO vehicle_signal_dirty VALUES ('2026-01-02 00:00:00','proof-car','can','proof-epoch','generation-2','2026-01-02 18:00:00')")
        sql("ADMIN flush_table('vehicle_signal')")
        return a.vehicle_history_section(ctx, cfg, now=now)
    assert phase('corrected_generation', 'analytics_nonquery', correction) == 1
    phase('corrected_output_parity', 'user_query', oracle)
    assert phase('corrected_unchanged_generation', 'analytics_nonquery', lambda: a.vehicle_history_section(ctx, cfg, now=now)) == 0
    c['result']['pending_movement'] = dict(before=hours, processed=completed, after=0, unchanged_processed=repeated)
    c['result']['attribution_note'] = 'Nested rollup/trip phases are subsets of coordinator; never sum overlapping intervals.'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--rows', type=int, default=160000)
    ap.add_argument('--cache', default='4MiB')
    ap.add_argument('--json-out', required=True)
    ap.add_argument('--no-refill', action='store_true')
    args = ap.parse_args()
    result = {}
    e.run_variant('cold-dirty-stock', args.rows, args.cache, 0, 2**40, result,
                  scenario=lambda c: scenario(c, not args.no_refill))
    with open(args.json_out, 'w') as out:
        json.dump(result, out, indent=2, default=str)
    print(json.dumps({k: result.get(k) for k in ('status', 'error', 'pending_movement')}))
    return int(result.get('status') != 'success')


if __name__ == '__main__':
    sys.exit(main())
