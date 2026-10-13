# Runbook: from where you are now to the finish line

One guide, start to end. Do the steps in order. Every step has a **Check** line: if you do not see it, stop and send the exact output.

## Where you are now

| Done and proven | Left to do |
|---|---|
| Sensors -> IoT Core -> Firehose -> S3 -> Athena | Switch on the AI answers (Steps 1-4) |
| Alert email for low tyre pressure | Put the code on GitHub (Step 6) |
| DynamoDB real-time table (3 rules active) | Fill the write-up (Step 7) |
| Chatbot in keyword mode (3 answers) | Send Balaji the message (Step 8) |

## Why Bedrock failed, and what we do instead

```
Your question
     |
     v
chatbot.py (in CloudShell)
     |-- 1. asks an LLM: "which of my 6 safe queries fits this question?"
     |        (only the question text is sent)
     |-- 2. runs that ONE fixed, read-only query on Athena (stays in AWS)
     |-- 3. asks the LLM: "say this small result table as a sentence"
     |        (the question + a few rows of synthetic data are sent)
     v
A sentence like "CAR-001 is below 28 psi, so yes, it is a problem."
```

* "Error 002: Access to Bedrock models is not allowed for this account" is blocked for the whole account, so no model id can fix it. I cannot see why from here.
* The LLM in the picture above can be any service that speaks the "OpenAI chat" format. You do not need OpenAI or any payment: **Google Gemini has a free API key (no card)**. We use that. The bot sends the request with plain Python (`urllib`), so there is nothing new to install.
* Balaji said "u can use github, openai also". GitHub Models (GitHub's free model service) was shut down on 30 July 2026, so it is not an option. Use the wording in Step 8 to ask him.

## Step 1: Get a free Gemini API key (5 min)

1. Use your **personal** Google account (not a Kyndryl one).
2. Open https://aistudio.google.com in a browser and sign in.
3. Find **Get API key** (left menu or top right), then **Create API key**. If it asks for a project, choose **Create project** / the default one.
4. Click the copy icon. The key starts with `AIza`. Keep it in a private note for now. **Never** paste it in a file, a screenshot, a chat, or GitHub.

**Check:** you have a long text starting with `AIza`.
If you cannot get a key (no access, country block, or the page asks for payment): tell me exactly what the page says. Then keep keyword mode (Step 5a), which is a valid honest result.

Free-tier facts to remember: it has per-minute and per-day request limits, and Google may use free-tier prompts to improve its products. That is fine here because we send only your question and a few rows of **synthetic** data.

## Step 2: Upload the new zip and run the tests (5 min)

In AWS CloudShell (region **ap-south-1**): **Actions -> Upload file** -> `vehicle-telemetry-v3.zip`.
```bash
cd ~
unzip -o vehicle-telemetry-v3.zip
cd vehicle-telemetry
pip install -q -r requirements.txt
python3 -m pytest -q
```
**Check:** `39 passed`.
If `cfn-lint` makes pip slow or fail, run `pip install -q pytest sqlglot boto3` instead. cfn-lint is only for GitHub's checks.

## Step 3: Give the bot your key and pick a model (5 min)

Type this command, press Enter, then paste the key (the screen shows nothing while you paste; that is normal) and press Enter:
```bash
read -rs -p "Paste Gemini key, then Enter: " LLM_API_KEY; echo; export LLM_API_KEY
python3 chatbot.py --list-llm-models
```
**Check:** a list of names such as `gemini-...-flash-lite`. Pick one that contains `flash-lite` (fast, highest free limits). If there is none, pick one with `flash`. Do not pick names with `pro`, `image`, `tts`, `live`, `embedding`.
```bash
export LLM_MODEL="<name from the list>"
python3 chatbot.py --ask "Which car has the lowest tyre pressure and is it a problem?"
```
**Check:** the first line is `[mode: OpenAI-compatible API (generativelanguage.googleapis.com), model ...]`, then `[intent=lowest_tyre vehicle=None]`, then **a sentence** that names CAR-001 (not a table). Screenshot it.

If you close CloudShell or it times out, the key and model are forgotten (this is good for safety). Run the two `export` lines again next time.

### If something fails (look at the line starting with `[LLM ...failed:`)

| You see | Meaning | Fix |
|---|---|---|
| `HTTP 400` and `API key not valid` | Key typed or pasted wrong | Redo Step 3 and paste again |
| `HTTP 403` | Key blocked or API not enabled for the project | In AI Studio create a new key in a new project, redo Step 3 |
| `HTTP 404` or `model not found` | Wrong model name | Re-run `--list-llm-models`, copy the name exactly |
| `HTTP 429` | Free-tier limit hit | Wait one minute, ask again. Try a `flash-lite` model |
| `network error` | CloudShell cannot reach the service | Tell me the full line |
| A table instead of a sentence, no error line | `LLM_MODEL` is not set in this session | Run both `export` lines again |

The bot never crashes on LLM problems. It falls back to the table, so your demo is safe.

## Step 4: Capture the chatbot evidence (10 min)

```bash
python3 chatbot.py --ask "Which car has the lowest tyre pressure and is it a problem?"
python3 chatbot.py --ask "Which car is the most fuel efficient?"
python3 chatbot.py --ask "How is CAR-001 doing?"
python3 chatbot.py --ask "Does anyone accelerate aggressively?"
python3 chatbot.py
```
In the last command type a question, press Enter, then type `exit`. If you hit the per-minute limit, wait 60 seconds.
Keep to this sentence in the write-up: "The LLM only chooses one of six fixed queries and phrases the result; it never writes SQL."
Take one screenshot of each answer.

## Step 5: Fallback if there is no key (only if Step 1 failed)

### 5a. Keep keyword mode
```bash
python3 chatbot.py --no-llm --ask "Which car has the lowest tyre pressure?"
```
Write honestly: "Bedrock is blocked on my personal account (Error 002) and I had no free LLM key, so the chatbot runs in keyword mode; the LLM part is implemented and tested with mocks but not run."

### 5b. Optional, in parallel: ask AWS why Bedrock is blocked
AWS console -> Support -> Create case -> Account and billing -> ask: "Bedrock returns Error 002 'Access to Bedrock models is not allowed for this account'. Why, and how can it be enabled?" Do not wait for the reply.

## Step 6: Put the project on GitHub (20 min)

1. In CloudShell: `cd ~ && zip -qr vehicle-telemetry-out.zip vehicle-telemetry -x "*/__pycache__/*" "*/.pytest_cache/*"`.
2. **Actions -> Download file** -> path `/home/cloudshell-user/vehicle-telemetry-out.zip`.
3. On your laptop unzip it. On github.com create a **private** repo named `connected-car-telemetry-aws` (no README, no .gitignore).
4. In a terminal inside the unzipped folder:
```bash
git init
git add .
git commit -m "Connected-car telemetry on AWS"
git branch -M main
git remote add origin <your repo URL>
git push -u origin main
```
5. Safety check before pushing: `git grep -n "AIza"` must print **nothing**.

**Check:** the repo's **Actions** tab shows a green `ci` run (about 1-2 minutes).
Keep it private unless Kyndryl policy allows public. Ask Balaji if unsure.

## Step 7: Finish the write-up (about 2 hours)

Open `docs/IMPLEMENTATION_GUIDE.md`. Everything is already drafted. You only:
1. Replace each `[SCREENSHOT]` with a picture you took yourself.
2. Fill every `[bracket]` using only what you really ran. Section 13 already lists the real issues you hit during this project.
3. In Appendix B, edit the "What I did" column in your own words.
4. Set the dates and the version history (Appendix C). Add Balaji's comments after he replies.
5. Tell me when it is done and I will turn it into a Word document in the same style as your lakehouse guide.

Screenshots to collect: IoT rules (3), MQTT test client, S3 files, Athena results, alert email, DynamoDB scan (CloudShell and console), chatbot sentence answers (Step 3-4), green GitHub `ci` run.

## Step 8: Message Balaji

Use `docs/MESSAGE_TO_BALAJI.md`: message A now (it asks your open question), message B on Friday with the guide and screenshots.

## Teardown (only after Balaji has reviewed)
```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name vehicle-telemetry --region ap-south-1 --query "Stacks[0].Outputs[?OutputKey=='Bucket'].OutputValue" --output text)
aws s3 rm s3://$BUCKET --recursive
aws cloudformation delete-stack --stack-name vehicle-telemetry --region ap-south-1
aws cloudformation wait stack-delete-complete --stack-name vehicle-telemetry --region ap-south-1
```
Also delete the Gemini key in AI Studio when you no longer need it.
