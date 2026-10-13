import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pytest
import chatbot as c

def test_every_query_is_read_only_select():
    for name in c.QUERIES:
        sql = c.build_sql(name, "CAR-001").strip().upper()
        assert sql.startswith("SELECT"), name
        for bad in ("DROP ", "DELETE ", "INSERT ", "UPDATE ", "CREATE ", "ALTER ", ";"):
            assert bad not in sql, (name, bad)

def test_every_query_has_partition_filter():
    for name in c.QUERIES:
        assert "dt >=" in c.build_sql(name, "CAR-001"), name

def test_sql_parses_as_trino():
    sqlglot = pytest.importorskip("sqlglot")
    for name in c.QUERIES:
        sqlglot.parse_one(c.build_sql(name, "CAR-001"), read="trino")

@pytest.mark.parametrize("text,exp", [("car 1", "CAR-001"), ("how is CAR-002 doing", "CAR-002"),
                                      ("car_003?", "CAR-003"), ("any car", None), ("", None)])
def test_find_vehicle(text, exp):
    assert c.find_vehicle(text) == exp

@pytest.mark.parametrize("q,intent", [
    ("Which car has the lowest tyre pressure?", "lowest_tyre"),
    ("is there a tire leak", "lowest_tyre"),
    ("best mileage?", "fuel_efficiency"),
    ("who accelerates aggressively", "harsh_acceleration"),
    ("how often is clutch used", "clutch_usage"),
    ("current status", "latest_status"),
    ("tell me about CAR-002", "vehicle_summary"),
    ("hello", "latest_status")])
def test_keyword_route(q, intent):
    assert c.keyword_route(q)[0] == intent

def test_parse_route_ok_and_noisy():
    assert c.parse_route('{"intent":"lowest_tyre","vehicle_id":null}') == ("lowest_tyre", None)
    assert c.parse_route('Sure! ```json\n{"intent":"vehicle_summary","vehicle_id":"CAR-002"}\n```') == ("vehicle_summary", "CAR-002")

def test_parse_route_rejects_bad_input():
    assert c.parse_route("no json here") is None
    assert c.parse_route('{"intent":"drop_table","vehicle_id":null}') is None
    assert c.parse_route('{"intent":"vehicle_summary","vehicle_id":null}') is None
    assert c.parse_route("{broken") is None

def test_vehicle_id_injection_is_blocked():
    assert c.parse_route('{"intent":"lowest_tyre","vehicle_id":"x\' OR 1=1 --"}') == ("lowest_tyre", None)
    with pytest.raises(ValueError):
        c.build_sql("vehicle_summary", "CAR-001' OR '1'='1")
    with pytest.raises(ValueError):
        c.build_sql("vehicle_summary", None)

class FakeAthena:
    def __init__(self, header, rows, state="SUCCEEDED"):
        self.h, self.r, self.state, self.sql = header, rows, state, None
    def start_query_execution(self, **kw):
        self.sql = kw["QueryString"]; assert kw["WorkGroup"] == "vehicle-telemetry"
        return {"QueryExecutionId": "q1"}
    def get_query_execution(self, **kw):
        return {"QueryExecution": {"Status": {"State": self.state, "StateChangeReason": "boom"}}}
    def get_query_results(self, **kw):
        rows = [{"Data": [{"VarCharValue": x} for x in self.h]}]
        rows += [{"Data": [{"VarCharValue": x} if x is not None else {} for x in r]} for r in self.r]
        return {"ResultSet": {"Rows": rows}}

def test_ask_without_llm_returns_table():
    a = FakeAthena(["vehicle_id", "min_psi"], [["CAR-001", "23.9"]])
    out = c.ask("lowest tyre pressure", a)
    assert "CAR-001 | 23.9" in out and "min_psi" in out

def test_ask_empty_result():
    out = c.ask("fuel", FakeAthena(["vehicle_id"], []))
    assert "no data" in out.lower()

def test_athena_failure_raises():
    with pytest.raises(RuntimeError):
        c.run_athena(FakeAthena(["x"], [], state="FAILED"), "SELECT 1")

class FakeBedrock:
    def __init__(self, replies, fail=False):
        self.replies, self.fail, self.calls = list(replies), fail, 0
    def converse(self, **kw):
        self.calls += 1
        if self.fail:
            raise RuntimeError("AccessDenied")
        return {"output": {"message": {"content": [{"text": self.replies.pop(0)}]}}}

def test_ask_with_llm_routes_and_answers():
    b = FakeBedrock(['{"intent":"fuel_efficiency","vehicle_id":null}', "CAR-001 is most efficient."])
    a = FakeAthena(["vehicle_id", "avg_kmpl"], [["CAR-001", "18.3"]])
    assert c.ask("who saves fuel?", a, b, "model-x") == "CAR-001 is most efficient."
    assert "fuel_efficiency_kmpl" in a.sql and b.calls == 2

def test_ask_falls_back_when_bedrock_fails():
    a = FakeAthena(["vehicle_id", "min_psi"], [["CAR-001", "23.9"]])
    out = c.ask("tyre pressure", a, FakeBedrock([], fail=True), "model-x")
    assert "CAR-001 | 23.9" in out

# ---- OpenAI client (network mocked) ----
import io, json as _json, urllib.error

class _Resp(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): return False

def test_openai_client_maps_request_and_response(monkeypatch):
    seen = {}
    def fake_urlopen(req, timeout=0):
        seen["body"] = _json.loads(req.data); seen["auth"] = req.get_header("Authorization")
        return _Resp(_json.dumps({"choices": [{"message": {"content": "hello"}}]}).encode())
    monkeypatch.setattr(c.urllib.request, "urlopen", fake_urlopen)
    out = c.converse(c.OpenAIClient("k123"), "some-model", "SYS", "USER Q")
    assert out == "hello" and seen["auth"] == "Bearer k123"
    assert seen["body"]["model"] == "some-model"
    assert seen["body"]["messages"] == [{"role": "system", "content": "SYS"}, {"role": "user", "content": "USER Q"}]

def test_openai_http_error_becomes_runtime_error_and_bot_falls_back(monkeypatch):
    def boom(req, timeout=0):
        raise urllib.error.HTTPError("u", 401, "x", {}, io.BytesIO(b'{"error":"bad key"}'))
    monkeypatch.setattr(c.urllib.request, "urlopen", boom)
    a = FakeAthena(["vehicle_id", "min_psi"], [["CAR-001", "23.9"]])
    out = c.ask("tyre pressure", a, c.OpenAIClient("bad"), "m")
    assert "CAR-001 | 23.9" in out

def test_pick_llm_priority(monkeypatch):
    for k in ("LLM_API_KEY", "LLM_MODEL", "LLM_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL", "BEDROCK_MODEL_ID"):
        monkeypatch.delenv(k, raising=False)
    assert c.pick_llm()[2] == "keyword mode"
    monkeypatch.setenv("OPENAI_API_KEY", "k"); monkeypatch.setenv("OPENAI_MODEL", "m")
    client, model, label = c.pick_llm()
    assert label == "OpenAI" and model == "m" and isinstance(client, c.OpenAIClient)


# ---- OpenAI-compatible client (Gemini free key etc.), network mocked ----
def _ok(text="hi"):
    return _Resp(_json.dumps({"choices": [{"message": {"content": text}}]}).encode())

def _http(code, headers=None):
    return urllib.error.HTTPError("u", code, "x", headers or {}, io.BytesIO(b'{"error":"nope"}'))

def test_compat_client_uses_base_url_and_bearer(monkeypatch):
    seen = {}
    def fake(req, timeout=0):
        seen["url"] = req.full_url; seen["auth"] = req.get_header("Authorization"); seen["body"] = _json.loads(req.data)
        return _ok("ok")
    monkeypatch.setattr(c.urllib.request, "urlopen", fake)
    cl = c.OpenAICompatClient("KEY", c.GEMINI_BASE + "/")
    assert c.converse(cl, "gemini-x", "SYS", "Q") == "ok"
    assert seen["url"] == "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    assert seen["auth"] == "Bearer KEY" and seen["body"]["max_tokens"] == 1000

def test_compat_client_retries_with_other_token_param_on_400(monkeypatch):
    bodies = []
    def fake(req, timeout=0):
        b = _json.loads(req.data); bodies.append(b)
        if "max_tokens" in b:
            raise _http(400)
        return _ok("second try")
    monkeypatch.setattr(c.urllib.request, "urlopen", fake)
    assert c.converse(c.OpenAICompatClient("k"), "m", "S", "Q") == "second try"
    assert "max_completion_tokens" in bodies[1] and "max_tokens" not in bodies[1]

def test_compat_client_waits_once_on_429(monkeypatch):
    calls, slept = [], []
    def fake(req, timeout=0):
        calls.append(1)
        if len(calls) == 1:
            raise _http(429, {"Retry-After": "3"})
        return _ok("after wait")
    monkeypatch.setattr(c.urllib.request, "urlopen", fake)
    monkeypatch.setattr(c.time, "sleep", lambda s: slept.append(s))
    assert c.converse(c.OpenAICompatClient("k"), "m", "S", "Q") == "after wait"
    assert slept == [3.0]

def test_compat_client_gives_up_on_repeated_429_and_bot_falls_back(monkeypatch):
    monkeypatch.setattr(c.urllib.request, "urlopen", lambda req, timeout=0: (_ for _ in ()).throw(_http(429)))
    monkeypatch.setattr(c.time, "sleep", lambda s: None)
    a = FakeAthena(["vehicle_id", "min_psi"], [["CAR-001", "23.9"]])
    out = c.ask("tyre pressure", a, c.OpenAICompatClient("k"), "m")
    assert "CAR-001 | 23.9" in out

def test_compat_client_error_never_contains_the_key(monkeypatch):
    monkeypatch.setattr(c.urllib.request, "urlopen", lambda req, timeout=0: (_ for _ in ()).throw(_http(401)))
    with pytest.raises(RuntimeError) as ei:
        c.converse(c.OpenAICompatClient("SECRET-KEY-123"), "m", "S", "Q")
    assert "SECRET-KEY-123" not in str(ei.value) and "401" in str(ei.value)

def test_compat_client_handles_list_content(monkeypatch):
    body = _json.dumps({"choices": [{"message": {"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}}]}).encode()
    monkeypatch.setattr(c.urllib.request, "urlopen", lambda req, timeout=0: _Resp(body))
    assert c.converse(c.OpenAICompatClient("k"), "m", "S", "Q") == "ab"

def test_list_models_strips_prefix(monkeypatch):
    body = _json.dumps({"data": [{"id": "models/gemini-b"}, {"id": "models/gemini-a"}]}).encode()
    monkeypatch.setattr(c.urllib.request, "urlopen", lambda req, timeout=0: _Resp(body))
    assert c.OpenAICompatClient("k", c.GEMINI_BASE).list_models() == ["gemini-a", "gemini-b"]

def test_pick_llm_prefers_generic_key_and_defaults_to_gemini(monkeypatch):
    for k in ("LLM_API_KEY", "LLM_MODEL", "LLM_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL", "BEDROCK_MODEL_ID"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "o"); monkeypatch.setenv("OPENAI_MODEL", "om")
    monkeypatch.setenv("LLM_API_KEY", "g"); monkeypatch.setenv("LLM_MODEL", "gemini-x")
    client, model, label = c.pick_llm()
    assert isinstance(client, c.OpenAICompatClient) and model == "gemini-x"
    assert client.URL.startswith("https://generativelanguage.googleapis.com/") and "generativelanguage" in label
    monkeypatch.setenv("LLM_BASE_URL", "https://api.groq.com/openai/v1")
    assert c.pick_llm()[0].URL == "https://api.groq.com/openai/v1/chat/completions"

def test_ask_with_compat_llm_end_to_end(monkeypatch):
    replies = ['{"intent":"lowest_tyre","vehicle_id":null}', "CAR-001 is below 28 psi, so yes it is a problem."]
    monkeypatch.setattr(c.urllib.request, "urlopen", lambda req, timeout=0: _ok(replies.pop(0)))
    a = FakeAthena(["vehicle_id", "min_psi"], [["CAR-001", "23.7"]])
    out = c.ask("Which car has the lowest tyre pressure and is it a problem?", a, c.OpenAICompatClient("k"), "m")
    assert out.startswith("CAR-001 is below 28 psi") and "tyre" in a.sql.lower()
