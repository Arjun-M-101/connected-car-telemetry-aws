"""Vehicle telemetry chatbot: an LLM (any provider) + Amazon Athena (data).

Safe design: the LLM never writes SQL. It only picks one of a fixed set of
pre-written, read-only queries (and an optional vehicle id), then phrases the
result in plain English. If no LLM works, the bot falls back to keyword
routing, so the demo still works.

LLM providers (first one configured wins):
  1. Any OpenAI-compatible API (Google Gemini free key, Groq, OpenRouter, ...):
       export LLM_API_KEY=...  LLM_MODEL=...  [LLM_BASE_URL=...]
     (LLM_BASE_URL defaults to Google Gemini's OpenAI-compatible endpoint.)
  2. OpenAI:          OPENAI_API_KEY + OPENAI_MODEL
  3. Amazon Bedrock:  BEDROCK_MODEL_ID
  4. Nothing set -> keyword mode.

Usage:
  python3 chatbot.py --list-llm-models             # models your LLM_API_KEY can use
  python3 chatbot.py --list-models                 # Bedrock model ids
  python3 chatbot.py --ask "Which car has the lowest tyre pressure?"
  python3 chatbot.py                               # interactive chat
  python3 chatbot.py --no-llm --ask "fuel efficiency"   # keyword mode
"""
import argparse, json, os, re, sys, time
import urllib.error, urllib.parse, urllib.request
import boto3

REGION = os.environ.get("AWS_REGION", "ap-south-1")
WORKGROUP = "vehicle-telemetry"
DATABASE = "vehicle_telemetry"
# Only look at the last 2 days of partitions (cheap, fast).
RECENT = "dt >= date_format(current_date - interval '2' day, '%Y-%m-%d')"
VEHICLE_RE = re.compile(r"^CAR-\d{3}$")

QUERIES = {
    "latest_status": ("Latest reading for every car",
        f"""SELECT vehicle_id, event_time, tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi,
                   speed_kmph, fuel_level_pct, fuel_efficiency_kmpl
            FROM (SELECT *, row_number() OVER (PARTITION BY vehicle_id ORDER BY ingest_ts DESC) rn
                  FROM telemetry WHERE {RECENT}) WHERE rn = 1 ORDER BY vehicle_id"""),
    "lowest_tyre": ("Lowest tyre pressure seen per car",
        f"""SELECT vehicle_id, min(least(tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi)) AS min_psi,
                   count_if(least(tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi) < 28) AS readings_below_28_psi
            FROM telemetry WHERE {RECENT} GROUP BY vehicle_id ORDER BY min_psi"""),
    "fuel_efficiency": ("Average fuel efficiency and fuel left per car",
        f"""SELECT vehicle_id, round(avg(fuel_efficiency_kmpl), 2) AS avg_kmpl,
                   round(min(fuel_level_pct), 1) AS fuel_left_pct
            FROM telemetry WHERE {RECENT} GROUP BY vehicle_id ORDER BY avg_kmpl DESC"""),
    "harsh_acceleration": ("Hard acceleration (accelerator above 85%) per car",
        f"""SELECT vehicle_id, count_if(accelerator_pct > 85) AS harsh_events, count(*) AS readings
            FROM telemetry WHERE {RECENT} GROUP BY vehicle_id ORDER BY harsh_events DESC"""),
    "clutch_usage": ("Share of readings with the clutch pressed, per car",
        f"""SELECT vehicle_id, round(100.0 * count_if(clutch_pct > 0) / count(*), 1) AS clutch_pressed_pct
            FROM telemetry WHERE {RECENT} GROUP BY vehicle_id ORDER BY vehicle_id"""),
    "vehicle_summary": ("Summary of one car",
        """SELECT vehicle_id, count(*) AS readings,
                  round(avg(fuel_efficiency_kmpl), 2) AS avg_kmpl,
                  round(min(least(tyre_fl_psi, tyre_fr_psi, tyre_rl_psi, tyre_rr_psi)), 1) AS min_psi,
                  round(max(speed_kmph), 1) AS max_speed_kmph,
                  count_if(accelerator_pct > 85) AS harsh_events
           FROM telemetry WHERE vehicle_id = '{vid}' AND """ + RECENT + " GROUP BY vehicle_id"),
}
KEYWORDS = [  # fallback routing, first match wins
    ("lowest_tyre", ("tyre", "tire", "pressure", "psi", "puncture", "leak")),
    ("fuel_efficiency", ("fuel", "mileage", "efficiency", "kmpl")),
    ("harsh_acceleration", ("harsh", "aggressive", "accelerat", "pedal")),
    ("clutch_usage", ("clutch",)),
    ("vehicle_summary", ("summary", "overview", "report")),
    ("latest_status", ("latest", "current", "status", "now", "last")),
]

def find_vehicle(text):
    m = re.search(r"car[\s_-]*0*(\d{1,3})", text or "", re.I)
    return f"CAR-{int(m.group(1)):03d}" if m else None

def keyword_route(question):
    q = question.lower()
    vid = find_vehicle(question)
    for intent, words in KEYWORDS:
        if any(w in q for w in words):
            return intent, vid
    return ("vehicle_summary" if vid else "latest_status"), vid

def parse_route(text):
    """Parse the LLM's JSON answer; return (intent, vehicle_id) or None if unusable."""
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except ValueError:
        return None
    intent, vid = d.get("intent"), d.get("vehicle_id")
    if intent not in QUERIES:
        return None
    if vid is not None and not VEHICLE_RE.match(str(vid)):
        vid = None
    if intent == "vehicle_summary" and not vid:
        return None
    return intent, vid

def build_sql(intent, vid=None):
    sql = QUERIES[intent][1]
    if "{vid}" in sql:
        if not (vid and VEHICLE_RE.match(vid)):
            raise ValueError("vehicle_summary needs a vehicle id like CAR-001")
        sql = sql.format(vid=vid)
    return sql

GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/openai"

class OpenAICompatClient:
    """Tiny client for any OpenAI-compatible Chat Completions API, with the same
    .converse() shape as Bedrock, so the rest of the bot does not care which LLM is
    behind it. Standard library only. The API key is read from the environment and is
    never written to a file or printed."""

    def __init__(self, api_key, base_url="https://api.openai.com/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.URL = self.base_url + "/chat/completions"

    def _post(self, body):
        req = urllib.request.Request(self.URL, data=json.dumps(body).encode(), headers={
            "Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)

    def converse(self, modelId, system, messages, inferenceConfig=None):
        msgs = [{"role": "system", "content": system[0]["text"]}]
        msgs += [{"role": m["role"], "content": m["content"][0]["text"]} for m in messages]
        # Providers disagree on the name of the length limit, so try them in turn.
        # Limit is generous: some models spend tokens on hidden "thinking" first.
        attempts = [{"max_tokens": 1000}, {"max_completion_tokens": 1000}, {}]
        waited = False
        i = 0
        while i < len(attempts):
            body = {"model": modelId, "messages": msgs, **attempts[i]}
            try:
                data = self._post(body)
                break
            except urllib.error.HTTPError as e:
                detail = e.read().decode(errors="replace")[:200]
                if e.code == 429 and not waited:        # free tiers: short wait, one retry
                    waited = True
                    try:
                        pause = float(e.headers.get("Retry-After", 5))
                    except (TypeError, ValueError):
                        pause = 5.0
                    time.sleep(min(pause, 20))
                    continue
                if e.code in (400, 422) and i + 1 < len(attempts):
                    i += 1                               # maybe the token-limit name was the problem
                    continue
                raise RuntimeError(f"LLM HTTP {e.code}: {detail}") from None
            except urllib.error.URLError as e:
                raise RuntimeError(f"LLM network error: {e.reason}") from None
        content = (data["choices"][0]["message"].get("content")) or ""
        if isinstance(content, list):                    # some providers return parts
            content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
        return {"output": {"message": {"content": [{"text": content}]}}}

    def list_models(self):
        req = urllib.request.Request(self.base_url + "/models",
                                     headers={"Authorization": f"Bearer {self.api_key}"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
        return sorted(re.sub(r"^models/", "", m["id"]) for m in data.get("data", []))

class OpenAIClient(OpenAICompatClient):
    """Plain OpenAI (kept for backward compatibility)."""
    def __init__(self, api_key):
        super().__init__(api_key, "https://api.openai.com/v1")

def pick_llm():
    """Return (client, model_id, label).
    1) LLM_API_KEY + LLM_MODEL (+ LLM_BASE_URL, default Gemini)  2) OPENAI_API_KEY + OPENAI_MODEL
    3) BEDROCK_MODEL_ID  4) keyword mode."""
    if os.environ.get("LLM_API_KEY") and os.environ.get("LLM_MODEL"):
        base = os.environ.get("LLM_BASE_URL") or GEMINI_BASE
        host = urllib.parse.urlparse(base).hostname or "custom"
        return OpenAICompatClient(os.environ["LLM_API_KEY"], base), os.environ["LLM_MODEL"], f"OpenAI-compatible API ({host})"
    if os.environ.get("OPENAI_API_KEY") and os.environ.get("OPENAI_MODEL"):
        return OpenAIClient(os.environ["OPENAI_API_KEY"]), os.environ["OPENAI_MODEL"], "OpenAI"
    if os.environ.get("BEDROCK_MODEL_ID"):
        return (boto3.client("bedrock-runtime", region_name=REGION),
                os.environ["BEDROCK_MODEL_ID"], "Amazon Bedrock")
    return None, None, "keyword mode"

def converse(bedrock, model_id, system, user, max_tokens=400):
    r = bedrock.converse(modelId=model_id, system=[{"text": system}],
                         messages=[{"role": "user", "content": [{"text": user}]}],
                         inferenceConfig={"maxTokens": max_tokens, "temperature": 0})
    return r["output"]["message"]["content"][0]["text"]

def route_with_llm(bedrock, model_id, question):
    system = ("You route questions about a connected-car fleet to one pre-written query. "
              "Reply with ONLY JSON: {\"intent\": <one of " + ", ".join(QUERIES) + ">, "
              "\"vehicle_id\": <like CAR-001 or null>}. Use vehicle_summary only when a specific car is named.")
    return parse_route(converse(bedrock, model_id, system, question, 100))

def run_athena(athena, sql, timeout=60):
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP,
            QueryExecutionContext={"Database": DATABASE})["QueryExecutionId"]
    t0 = time.time()
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - t0 > timeout:
            raise TimeoutError("Athena query timed out")
        time.sleep(0.7)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena {st['State']}: {st.get('StateChangeReason', '')}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    cells = [[c.get("VarCharValue") for c in r["Data"]] for r in rows]
    return cells[0], cells[1:]

def table_text(header, rows):
    return "\n".join([" | ".join(header)] + [" | ".join("" if v is None else v for v in r) for r in rows])

def plain_answer(title, header, rows):
    return f"{title}:\n{table_text(header, rows)}" if rows else f"{title}: no data found yet."

def answer_with_llm(bedrock, model_id, question, title, header, rows):
    system = ("You help a car driver or support team. Answer using ONLY the data table provided. "
              "Be short (max 4 sentences). Tyre pressure is in psi; below 28 psi is low. "
              "If the table is empty say there is no data yet. Never invent numbers.")
    user = f"Question: {question}\nData ({title}):\n{table_text(header, rows)}"
    return converse(bedrock, model_id, system, user)

def ask(question, athena, bedrock=None, model_id=None, verbose=True):
    route = None
    if bedrock and model_id:
        try:
            route = route_with_llm(bedrock, model_id, question)
        except Exception as e:  # model not enabled, throttling, etc.
            print(f"[LLM routing failed: {type(e).__name__}: {str(e)[:250]}; using keywords]", file=sys.stderr)
    intent, vid = route or keyword_route(question)
    if intent == "vehicle_summary" and not vid:
        intent = "latest_status"
    sql = build_sql(intent, vid)
    title = QUERIES[intent][0]
    if verbose:
        print(f"[intent={intent} vehicle={vid}]")
    header, rows = run_athena(athena, sql)
    if bedrock and model_id:
        try:
            return answer_with_llm(bedrock, model_id, question, title, header, rows)
        except Exception as e:
            print(f"[LLM answer failed: {type(e).__name__}: {str(e)[:250]}; showing table]", file=sys.stderr)
    return plain_answer(title, header, rows)

def list_models():
    b = boto3.client("bedrock", region_name=REGION)
    print("Inference profiles (use these ids first):")
    for p in b.list_inference_profiles().get("inferenceProfileSummaries", []):
        print("  ", p["inferenceProfileId"])
    print("Foundation models (text, on-demand):")
    for m in b.list_foundation_models(byOutputModality="TEXT").get("modelSummaries", []):
        if "ON_DEMAND" in m.get("inferenceTypesSupported", []):
            print("  ", m["modelId"])

def list_llm_models():
    key = os.environ.get("LLM_API_KEY")
    if not key:
        sys.exit("Set LLM_API_KEY first (see the runbook), then run this again.")
    client = OpenAICompatClient(key, os.environ.get("LLM_BASE_URL") or GEMINI_BASE)
    try:
        names = client.list_models()
    except urllib.error.HTTPError as e:
        sys.exit(f"Could not list models: HTTP {e.code} {e.read().decode(errors='replace')[:200]}")
    except urllib.error.URLError as e:
        sys.exit(f"Could not reach the LLM service: {e.reason}")
    print("Model names you can put in LLM_MODEL (look for 'flash-lite' or 'flash'):")
    for n in names:
        print("  ", n)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ask")
    ap.add_argument("--no-llm", action="store_true")
    ap.add_argument("--list-models", action="store_true", help="list Bedrock models")
    ap.add_argument("--list-llm-models", action="store_true", help="list models for LLM_API_KEY")
    a = ap.parse_args()
    if a.list_models:
        return list_models()
    if a.list_llm_models:
        return list_llm_models()
    athena = boto3.client("athena", region_name=REGION)
    bedrock, model_id, label = (None, None, "keyword mode") if a.no_llm else pick_llm()
    print(f"[mode: {label}" + (f", model {model_id}]" if model_id else "; set LLM_API_KEY + LLM_MODEL to enable an LLM]"))
    if a.ask:
        return print(ask(a.ask, athena, bedrock, model_id))
    print("Ask about the fleet (e.g. 'which car has the lowest tyre pressure?'). Type 'exit' to quit.")
    while True:
        try:
            q = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if q.lower() in ("exit", "quit"):
            break
        if q:
            try:
                print("bot>", ask(q, athena, bedrock, model_id), "\n")
            except Exception as e:
                print("bot> sorry, that failed:", e, "\n")

if __name__ == "__main__":
    main()
