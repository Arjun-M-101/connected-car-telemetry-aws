import sys, pathlib, random
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import simulator as s

FIELDS = {"vehicle_id","event_time","tyre_fl_psi","tyre_fr_psi","tyre_rl_psi","tyre_rr_psi",
          "accelerator_pct","clutch_pct","speed_kmph","fuel_level_pct","fuel_efficiency_kmpl"}

def test_message_has_all_sensor_fields():
    random.seed(1)
    msg = s.step(s.make_state("CAR-001"), leak=False)
    assert set(msg) == FIELDS

def test_values_stay_in_physical_range():
    random.seed(2)
    st = s.make_state("CAR-002")
    for _ in range(500):
        m = s.step(st, leak=False)
        assert 0 <= m["accelerator_pct"] <= 100 and m["clutch_pct"] in (0.0, 100.0)
        assert 0 <= m["speed_kmph"] <= 140 and 0 <= m["fuel_level_pct"] <= 100
        assert 4 <= m["fuel_efficiency_kmpl"] <= 25

def test_leak_only_hits_car_001_front_left():
    random.seed(3)
    leaky, other = s.make_state("CAR-001"), s.make_state("CAR-002")
    for _ in range(60):
        a, b = s.step(leaky, True), s.step(other, True)
    assert a["tyre_fl_psi"] < 28 and b["tyre_fl_psi"] > 28
