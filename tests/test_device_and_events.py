import sys, pathlib, json
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import device_mqtt as d
import send_test_events as e

def test_outbox_flushes_in_order_and_empties():
    sent, ob = [], d.Outbox()
    for i in range(3): ob.add(i)
    assert ob.flush(sent.append) is True and sent == [0, 1, 2] and len(ob) == 0

def test_outbox_keeps_messages_when_offline_then_recovers():
    ob, sent, online = d.Outbox(), [], {"up": False}
    def send(m):
        if not online["up"]: raise ConnectionError("down")
        sent.append(m)
    for i in range(3): ob.add(i)
    assert ob.flush(send) is False and len(ob) == 3 and sent == []
    online["up"] = True
    assert ob.flush(send) is True and sent == [0, 1, 2]

def test_outbox_is_bounded():
    ob = d.Outbox(maxlen=2)
    for i in range(5): ob.add(i)
    assert list(ob.q) == [3, 4]

def test_failure_midway_does_not_lose_or_duplicate():
    ob, sent = d.Outbox(), []
    for i in range(3): ob.add(i)
    def flaky(m):
        if m == 1 and not sent.count("retry"):
            sent.append("retry"); raise ConnectionError()
        sent.append(m)
    assert ob.flush(flaky) is False
    assert ob.flush(flaky) is True
    assert [x for x in sent if x != "retry"] == [0, 1, 2]

def test_device_message_has_extra_fields_and_sensor_fields():
    import simulator as s, random
    random.seed(5)
    m = d.build_message(s.make_state("CAR-101"), 7)
    assert m["seq"] == 7 and m["schema_version"] == "1" and "fw_version" in m and "battery_v" in m
    assert "tyre_fl_psi" in m and json.dumps(m)

def test_test_events_cover_cases_and_alert_is_opt_in():
    names = [n for n, _ in e.build_events()]
    assert len(names) == 7 and not any("LOW TYRE" in n for n in names)
    assert any("LOW TYRE" in n for n, _ in e.build_events(True))
    assert all(p.get("vehicle_id", "CAR-900") in ("CAR-900", "CAR-777") for _, p in e.build_events())
