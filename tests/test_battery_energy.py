#!/usr/bin/env python3
"""Synthetic physical answers and refusal boundaries; no vehicle/network calls."""
import copy
import math
import os
import sys

from scripts.analytics.battery import battery_energy as en

HOUR = 3600000000000
T0 = 1790474400000000000
STEP = 60000000000


def sig(t, field, value, unit=None, **kwargs):
    return dict(event_time_ns=t, ingest_time_ns=t, vehicle='v', source='fleet',
                decode_epoch='e1', source_field=field, path=field,
                value_num=value, unit=unit, quality=kwargs.pop('quality', 'valid'), **kwargs)


def cal(**overrides):
    energy = {'current_sign': 'positive_charge', 'max_gap_ns': 2*STEP,
              'max_skew_ns': STEP, 'domain': 'synthetic', 'conditions': '25C',
              'reference': {'version': 'new-pack-1', 'domain': 'synthetic',
                            'conditions': '25C', 'energy_kwh': 50},
              'field_calibration': {field: {
                  'vehicle': 'v', 'source': 'fleet', 'decode_epoch': 'e1',
                  'declared_domain': 'synthetic', 'version': 'units-1',
                  'unit': unit, 'unit_scale': 1, 'unit_offset': 0}
                  for field, unit in [('PackCurrent', 'A'), ('PackVoltage', 'V')]}}
    energy.update(overrides)
    return {'energy': energy}


def pair(t, current):
    return [sig(t, 'PackCurrent', current), sig(t, 'PackVoltage', 400)]


def one(rows, suffix):
    matches = [r for r in rows if r['metric'] == 'battery.energy.'+suffix]
    assert len(matches) == 1, (suffix, matches)
    return matches[0]


def full_discharge(start=T0, span=100, energy=40):
    """Observed idle -> 10-minute battery discharge -> observed idle.

    Independent counter energy is synthetic, not derived from reported SOC.
    Native Fleet V/A remain unit-NULL until the explicit scoped calibration.
    """
    rows = []
    for minute in range(12):
        fraction = max(0, min(1, (minute-1)/10))
        values = [('PackCurrent', -600 if 1 <= minute <= 10 else 0, None),
                  ('PackVoltage', 400, None), ('Soc', 100-fraction*span, '%'),
                  ('LifetimeEnergyUsed', 200+fraction*energy, 'kWh'),
                  ('DCChargingEnergyIn', 100, 'kWh'), ('ACChargingEnergyIn', 300, 'kWh')]
        rows.extend(sig(start+minute*STEP, field, value, unit)
                    for field, value, unit in values)
    return rows


def test_zero_crossing_preserves_gross_directions():
    rows = pair(T0, 10) + pair(T0+HOUR, -10)
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    for suffix in ('vi_charge_energy_kwh', 'vi_discharge_energy_kwh'):
        assert abs(one(out, suffix)['value']-1) < 1e-10
    for suffix in ('charge_throughput_ah', 'discharge_throughput_ah'):
        assert abs(one(out, suffix)['value']-2.5) < 1e-10
    swapped = en.analyze(rows, [], cal(max_gap_ns=HOUR, current_sign='positive_discharge'))
    assert one(swapped, 'charge_throughput_ah')['value'] == 2.5


def test_barriers_never_bridge_and_ah_does_not_require_voltage():
    base = pair(T0, 10)+pair(T0+STEP, 10)+pair(T0+2*STEP, 10)
    for field in ('PackCurrent', 'PackVoltage'):
        for barrier in (sig(T0+STEP//2, field, None, quality='invalid'),
                        sig(T0+STEP//2, field, 10, 'unknown'),
                        sig(T0+STEP, field, 99)):
            out = en.analyze(base+[barrier], [], cal())
            assert one(out, 'vi_charge_energy_kwh')['value'] is None
            ah = one(out, 'charge_throughput_ah')['value']
            assert ah is None if field == 'PackCurrent' else abs(ah-1/3) < 1e-10
    missing_voltage = [r for r in base if r['source_field'] == 'PackCurrent']
    out = en.analyze(missing_voltage, [], cal())
    assert abs(one(out, 'charge_throughput_ah')['value']-1/3) < 1e-10
    assert one(out, 'vi_charge_energy_kwh')['value'] is None
    assert one(en.analyze(pair(T0, 10)+pair(T0+HOUR, 10), [], cal()),
               'charge_throughput_ah')['value'] is None


def test_counter_domains_reset_conflict_and_invalid():
    rows = [sig(T0, field, value, 'kWh') for field, value in
            [('DCChargingEnergyIn', 100), ('ACChargingEnergyIn', 200), ('LifetimeEnergyUsed', 300)]]
    rows += [sig(T0+STEP, field, value, 'kWh') for field, value in
             [('DCChargingEnergyIn', 108), ('ACChargingEnergyIn', 208.8), ('LifetimeEnergyUsed', 308.2)]]
    out = en.analyze(rows, [], cal())
    for suffix, value in [('dc_charging_energy_in_kwh', 8), ('ac_charging_energy_in_kwh', 8.8),
                          ('discharge_energy_kwh', 8.2)]:
        assert abs(one(out, suffix)['value']-value) < 1e-10
    assert abs(one(out, 'efc_oneway_cycles')['value']-8.2/50) < 1e-10
    assert abs(one(out, 'efc_bidirectional_cycles')['value']-16.2/100) < 1e-10
    for barrier in [sig(T0+STEP//2, 'DCChargingEnergyIn', None, 'kWh', quality='invalid'),
                    sig(T0+STEP//2, 'DCChargingEnergyIn', 99, 'kWh'),
                    sig(T0, 'DCChargingEnergyIn', 101, 'kWh')]:
        out = en.analyze(rows+[barrier], [], cal())
        assert one(out, 'dc_charging_energy_in_kwh')['value'] is None
        assert one(out, 'efc_bidirectional_cycles')['value'] is None


def test_complete_full_discharge_soh_uncertainty_and_reference():
    cfg = cal(soc_uncertainty_pct=.5, energy_uncertainty_kwh=.1)
    out = en.analyze(full_discharge(), [], cfg)
    assert one(out, 'discharge_session_energy_kwh')['value'] == 40
    assert one(out, 'full_usable_capacity_kwh')['value'] == 40
    assert one(out, 'soh_pct')['value'] == 80
    assert one(out, 'capacity_trend_kwh')['value'] == -10
    sigma = 40*math.sqrt((math.sqrt(2)*.1/40)**2+(math.sqrt(2)*.005)**2)
    assert abs(one(out, 'soh_pct')['uncertainty']-sigma/50*100) < 1e-10
    for change in ({'domain': 'other'}, {'conditions': 'cold'}, {'reference': None}):
        assert one(en.analyze(full_discharge(), [], cal(**change)), 'soh_pct')['value'] is None


def test_partial_capacity_is_not_absolute_soh():
    out = en.analyze(full_discharge(span=20, energy=5), [], cal())
    assert one(out, 'interval_capacity_kwh')['value'] == 25
    assert one(out, 'full_usable_capacity_kwh')['value'] is None
    assert one(out, 'soh_pct')['value'] is None
    for span in (0, 5):
        assert one(en.analyze(full_discharge(span=span), [], cal()), 'interval_capacity_kwh')['value'] is None


def test_estimated_soh_uses_partial_session_and_bms_nominal_reference():
    # 40 %p discharge of 20 kWh -> 50 kWh interval capacity.
    rows = full_discharge(span=40, energy=20)
    nominal = [sig(T0, 'NominalFullPackEnergyKwh', 62.5, 'kWh')]
    no_ref = cal(reference=None)
    got = one(en.analyze(rows + nominal, [], no_ref), 'soh_estimated_pct')
    assert abs(got['value'] - 80.0) < 1e-9 and 'ref=bms_nominal_full_pack:62.5' in got['reason']
    # Default estimate noise always yields a bounded interval, never a bare point.
    assert got['uncertainty'] > 0
    assert got['uncertainty_lower'] < got['value'] < got['uncertainty_upper']
    # Absolute soh_pct still refuses a partial window.
    assert one(en.analyze(rows + nominal, [], no_ref), 'soh_pct')['value'] is None
    # Configured reference wins over the BMS nominal.
    configured = one(en.analyze(rows + nominal, [], cal()), 'soh_estimated_pct')
    assert abs(configured['value'] - 100.0) < 1e-9
    # Span below soh_min_soc_span_pct, or no reference at all: unavailable.
    short = full_discharge(span=20, energy=10)
    assert one(en.analyze(short + nominal, [], no_ref), 'soh_estimated_pct')['value'] is None
    assert one(en.analyze(rows, [], no_ref), 'soh_estimated_pct')['value'] is None
    # bms_first: a vehicle reading beats the configured community fallback,
    # which still applies while the vehicle never reports one.
    first = cal(reference_source='bms_first')
    assert abs(one(en.analyze(rows + nominal, [], first), 'soh_estimated_pct')['value'] - 80.0) < 1e-9
    fallback = one(en.analyze(rows, [], first), 'soh_estimated_pct')
    assert abs(fallback['value'] - 100.0) < 1e-9 and 'ref=new-pack-1' in fallback['reason']


def test_cross_hour_session_credited_only_at_closure():
    rows = full_discharge(T0+55*STEP)
    early = en.analyze(rows, [], dict(cal(), window_start_ns=T0, window_end_ns=T0+HOUR-1))
    late = en.analyze(rows, [], dict(cal(), window_start_ns=T0+HOUR, window_end_ns=T0+2*HOUR-1))
    after = en.analyze(rows, [], dict(cal(), window_start_ns=T0+2*HOUR, window_end_ns=T0+3*HOUR-1))
    assert one(early, 'discharge_session_energy_kwh')['value'] is None
    assert one(late, 'discharge_session_energy_kwh')['value'] == 40
    assert one(after, 'discharge_session_energy_kwh')['value'] is None
    assert one(after, 'soh_pct')['value'] == 80  # retained measurement, as-of in reason
    # Counter deltas partition the meter across hours: 12 + 28 = the full 40.
    assert one(early, 'discharge_energy_kwh')['value'] == 12
    assert one(late, 'discharge_energy_kwh')['value'] == 28


def test_counter_delta_anchors_prior_window_reading_only_when_bounded():
    def meter(t, value, **kwargs):
        return sig(t, 'LifetimeEnergyUsed', value, 'kWh', **kwargs)
    win = dict(window_start_ns=T0+HOUR, window_end_ns=T0+2*HOUR-1)
    rows = [meter(T0+HOUR-STEP, 100), meter(T0+HOUR+STEP, 101), meter(T0+HOUR+2*STEP, 103)]
    out = one(en.analyze(rows, [], dict(cal(), **win)), 'discharge_energy_kwh')
    assert out['value'] == 3 and 'anchor=prior_window' in out['reason']
    # Prior reading beyond max_gap_ns (2 steps here) is not a baseline.
    far = [meter(T0+HOUR-2*STEP, 100)] + rows[1:]
    assert one(en.analyze(far, [], dict(cal(), **win)), 'discharge_energy_kwh')['value'] == 2
    # An invalid latest prior reading is a barrier, not skipped over.
    barrier = rows + [meter(T0+HOUR-STEP//2, None, quality='invalid')]
    assert one(en.analyze(barrier, [], dict(cal(), **win)), 'discharge_energy_kwh')['value'] == 2
    # A meter decrease across the boundary is a reset, never negative energy.
    reset = [meter(T0+HOUR-STEP, 200)] + rows[1:]
    assert one(en.analyze(reset, [], dict(cal(), **win)), 'discharge_energy_kwh')['value'] is None


def test_parked_discharge_credits_offline_gap_once_in_resume_window():
    def meter(t, value, **kwargs):
        return sig(t, 'LifetimeEnergyUsed', value, 'kWh', **kwargs)
    # Production shape: drive ends 19:31, next reading 07:24 after the night.
    rows = [meter(T0, 100), meter(T0+STEP, 100.5),
            meter(T0+12*HOUR, 100.516), meter(T0+12*HOUR+STEP, 101.0)]
    resume = dict(window_start_ns=T0+12*HOUR, window_end_ns=T0+13*HOUR-1)
    before = dict(window_start_ns=T0, window_end_ns=T0+HOUR-1)
    got = one(en.analyze(rows, [], dict(cal(), **resume)), 'parked_discharge_kwh')
    assert abs(got['value'] - 0.016) < 1e-9 and 'offline_gap_s=43140' in got['reason']
    # Resume-window counter excludes the gap leg; together they partition 1.0.
    counter = one(en.analyze(rows, [], dict(cal(), **resume)), 'discharge_energy_kwh')
    first = one(en.analyze(rows, [], dict(cal(), **before)), 'discharge_energy_kwh')
    assert abs(first['value'] + got['value'] + counter['value'] - 1.0) < 1e-9
    assert one(en.analyze(rows, [], dict(cal(), **before)), 'parked_discharge_kwh')['value'] is None
    # Beyond max_offline_gap_ns, invalid endpoint, or meter decrease: unavailable.
    short = cal(max_offline_gap_ns=HOUR)
    assert one(en.analyze(rows, [], dict(short, **resume)), 'parked_discharge_kwh')['value'] is None
    invalid = rows + [meter(T0+2*STEP, None, quality='invalid')]
    assert one(en.analyze(invalid, [], dict(cal(), **resume)), 'parked_discharge_kwh')['value'] is None
    reset = rows[:2] + [meter(T0+12*HOUR, 50.0)] + rows[3:]
    assert one(en.analyze(reset, [], dict(cal(), **resume)), 'parked_discharge_kwh')['value'] is None


def test_incomplete_or_invalid_full_cycle_is_never_healthy():
    rows = full_discharge()
    variants = [[r for r in rows if r['event_time_ns'] > T0],
                [r for r in rows if r['event_time_ns'] < T0+11*STEP],
                [r for r in rows if r['event_time_ns'] not in (T0+4*STEP, T0+5*STEP)]]
    for field, unit in [('Soc', '%'), ('LifetimeEnergyUsed', 'kWh'), ('PackCurrent', None)]:
        variants.append(rows+[sig(T0+5*STEP, field, None, unit, quality='invalid')])
    variants.append(rows+[sig(T0+5*STEP, 'Soc', 99, '%')])
    for data in variants:
        assert one(en.analyze(data, [], cal()), 'soh_pct')['value'] is None


def test_charge_and_discharge_are_distinct_sessions():
    rows = full_discharge()
    charge = []
    for row in rows:
        item = dict(row, event_time_ns=row['event_time_ns']+20*STEP)
        if item['source_field'] == 'PackCurrent': item['value_num'] *= -1
        if item['source_field'] == 'DCChargingEnergyIn':
            item['value_num'] = 100+(row['event_time_ns']-T0)/STEP
        charge.append(item)
    out = en.analyze(rows+charge, [], cal())
    discharge = one(out, 'discharge_session_energy_kwh')
    charged = one(out, 'charge_session_energy_kwh')
    assert discharge['value'] == 40 and charged['value'] == 11
    assert discharge['analysis_id'] != charged['analysis_id']
    assert discharge['episode_id'] != charged['episode_id']


def test_online_availability_order_and_duplicates():
    rows = pair(T0, 10)+pair(T0+STEP, 10)
    cfg = dict(cal(), decision_time_ns=T0+2*STEP)
    baseline = one(en.analyze(rows, [], cfg), 'charge_throughput_ah')
    assert abs(baseline['value']-1/6) < 1e-10
    replayed = one(en.analyze(list(reversed(rows+rows)), [], cfg), 'charge_throughput_ah')
    assert baseline['value'] == replayed['value'] and baseline['revision'] == replayed['revision']
    for ingest in (None, T0+3*STEP):
        unknown = [dict(r, ingest_time_ns=ingest) for r in rows]
        assert one(en.analyze(unknown, [], cfg), 'charge_throughput_ah')['value'] is None


def test_scope_units_configuration_and_nullable_epochs():
    rows = pair(T0, 10)+pair(T0+STEP, 10)
    wrong = cal()
    wrong['energy']['field_calibration']['PackCurrent']['vehicle'] = 'other'
    assert one(en.analyze(rows, [], wrong), 'charge_throughput_ah')['value'] is None
    malformed = cal()
    malformed['energy']['field_calibration']['PackCurrent'] = {'unit': 'A'}
    assert one(en.analyze(rows, [], malformed), 'charge_throughput_ah')['status'] == 'error'
    duplicate = cal()
    entry = duplicate['energy']['field_calibration']['PackCurrent']
    duplicate['energy']['field_calibration']['PackCurrent'] = [entry, dict(entry)]
    assert one(en.analyze(rows, [], duplicate), 'charge_throughput_ah')['status'] == 'error'
    native = [dict(r, unit='A', decode_epoch='native') for r in rows if r['source_field'] == 'PackCurrent']
    out = en.analyze(rows+native+[dict(r, decode_epoch=None) for r in native], [], cal())
    assert {r['decode_epoch'] for r in out} == {'e1', 'native'}
    assert all(abs(r['value']-1/6) < 1e-10 for r in out if r['metric'].endswith('.charge_throughput_ah'))
    substitute = [dict(r, source_field='BatteryCurrent', unit='A') for r in native]
    assert one(en.analyze(substitute, [], cal()), 'charge_throughput_ah')['value'] is None
    for cfg in ({'window_start_ns':'bad'}, {'window_start_ns':2,'window_end_ns':1}):
        assert all(r['status'] == 'error' for r in en.analyze(rows, [], cfg))


def test_bms_circular_never_promoted_to_soh():
    rows = [sig(T0, 'EnergyRemaining', 20, 'kWh'), sig(T0+STEP, 'EnergyRemaining', 22, 'kWh'),
            sig(T0, 'Soc', 40, '%'), sig(T0+STEP, 'Soc', 60, '%')]
    out = en.analyze(rows, [], cal())
    assert one(out, 'bms_circular_capacity_kwh')['value'] == 10
    assert one(out, 'soh_pct')['value'] is None
    rows.append(sig(T0+STEP//2, 'EnergyRemaining', None, 'kWh', quality='invalid'))
    assert one(en.analyze(rows, [], cal()), 'bms_circular_capacity_kwh')['value'] is None


def test_latest_physical_sample_requires_scoped_units_scale_offset_and_sign():
    rows = [sig(T0, 'PackVoltage', 200), sig(T0, 'PackCurrent', 5)]
    cfg = cal()
    cfg['energy']['field_calibration']['PackVoltage'].update(
        unit_scale=2, unit_offset=10)
    cfg['energy']['field_calibration']['PackCurrent'].update(
        unit_scale=10, unit_offset=-5)
    for sign, expected in (('positive_charge', 18.45),
                           ('positive_discharge', -18.45)):
        cfg['energy']['current_sign'] = sign
        out = en.analyze(rows, [], cfg)
        voltage = one(out, 'latest_pack_voltage_v')
        power = one(out, 'latest_power_kw')
        assert voltage['value'] == 410 and voltage['unit'] == 'V'
        assert abs(power['value'] - expected) < 1e-10 and power['unit'] == 'kW'
        assert power['value_text'] == voltage['value_text']
        assert power['value_text'].endswith('.000000000')
        assert 'asof_ns=%d' % T0 in power['reason']
    for missing in ({}, {'energy': {'current_sign': 'positive_charge'}},
                    {'energy': {'field_calibration': cfg['energy']['field_calibration']}}):
        assert one(en.analyze(rows, [], missing), 'latest_power_kw')['value'] is None
    wrong_scope = copy.deepcopy(cfg)
    wrong_scope['energy']['field_calibration']['PackVoltage']['vehicle'] = 'other'
    assert one(en.analyze(rows, [], wrong_scope), 'latest_power_kw')['value'] is None
    assert one(en.analyze(rows, [], wrong_scope), 'latest_pack_voltage_v')['value'] is None
    # Native physical unit metadata needs no guessed scope calibration.
    native = [sig(T0, 'PackVoltage', 410, 'V'), sig(T0, 'PackCurrent', 45, 'A')]
    assert one(en.analyze(native, [], {'energy': {'current_sign': -1}}),
               'latest_power_kw')['value'] == -18.45


def test_latest_physical_invalid_unmatched_conflicting_and_stale_never_fallback():
    good = pair(T0, -100)
    assert one(en.analyze(good, [], cal()), 'latest_power_kw')['value'] == -40
    for bad in (pair(T0+STEP, None),
                [sig(T0+STEP, 'PackVoltage', None, quality='invalid')],
                [sig(T0+STEP, 'PackCurrent', 20)],
                pair(T0+STEP, 10) + [sig(T0+STEP, 'PackVoltage', 401)]):
        out = en.analyze(good+bad, [], cal())
        assert one(out, 'latest_power_kw')['value'] is None
    out = en.analyze(good+[sig(T0+STEP, 'PackVoltage', 401)], [], cal())
    assert one(out, 'latest_pack_voltage_v')['value'] == 401
    assert one(out, 'latest_power_kw')['value'] is None
    cfg = dict(cal(), window_start_ns=T0, window_end_ns=T0+2*STEP)
    assert one(en.analyze(good, [], cfg), 'latest_power_kw')['value'] == -40
    cfg['window_end_ns'] += 1
    for suffix in ('latest_power_kw', 'latest_pack_voltage_v'):
        result = one(en.analyze(good, [], cfg), suffix)
        assert result['value'] is None and result['reason'].startswith('stale:')
    assert one(en.analyze(pair(T0, 0), [], cal()), 'latest_power_kw')['value'] == 0


if __name__ == '__main__':
    names = sorted(n for n in globals() if n.startswith('test_'))
    for name in names:
        globals()[name]()
    print('test_battery_energy: ok (%d tests)' % len(names))
