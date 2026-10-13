# Connected-Car Telemetry on AWS

Sensor data from a (simulated) connected car -> AWS -> queryable tables, with alerts,
a real-time "latest status" view and a small AI chatbot.

![architecture](docs/architecture.png)
*Diagram generated from `docs/architecture.dot` (Graphviz); the script was written with AI assistance (Claude).*

| Need (from the brief) | How it is covered |
|---|---|
| Sensor -> AWS service -> table | simulator -> IoT Core -> Firehose -> S3 -> Glue table -> Athena |
| Tyre pressure | 4 tyres, psi; alert email below 28 psi |
| Accelerator, clutch | pedal percentages |
| Fuel efficiency | km/l + fuel level; per-car averages |
| Report to driver / support team | SNS email, DynamoDB latest state, SQL reports, chatbot |
| AI chatbot | `chatbot.py`: Athena (read-only, fixed queries) + an LLM through any OpenAI-compatible API (Google Gemini free key by default), Amazon Bedrock or OpenAI, with keyword fallback |

## Quick start (AWS CloudShell, region ap-south-1)
1. `aws cloudformation deploy --template-file template.yaml --stack-name vehicle-telemetry --capabilities CAPABILITY_IAM --parameter-overrides AlertEmail=you@example.com --region ap-south-1`
2. Confirm the SNS email.
3. `python3 simulator.py --vehicles 3 --count 60` (add `--leak` for the alert demo, `--count 0` to run forever)
4. After ~2 minutes run `sql/queries.sql` in Athena (workgroup `vehicle-telemetry`, database `vehicle_telemetry`).
5. Real-time view: `aws dynamodb scan --table-name vehicle-latest-state --region ap-south-1`
6. Chatbot (keyword mode): `python3 chatbot.py --no-llm --ask "which car has the lowest tyre pressure?"`
7. Chatbot with an LLM: `export LLM_API_KEY=...` (free Google AI Studio key), `python3 chatbot.py --list-llm-models`, `export LLM_MODEL=<name>`, then `python3 chatbot.py --ask "..."`. Never commit the key.

Full step-by-step: `docs/RUNBOOK.md`. Write-up: `docs/IMPLEMENTATION_GUIDE.md`.

## Tests
`pip install -r requirements.txt && pytest -q && cfn-lint template.yaml` (39 tests; no AWS account needed)

## Tear down
`aws s3 rm s3://<bucket> --recursive` then `aws cloudformation delete-stack --stack-name vehicle-telemetry --region ap-south-1`
