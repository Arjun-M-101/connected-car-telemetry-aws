# Messages for Balaji (edit the [brackets] to match reality)

## A. Send NOW (short check-in; he asked for it, and it shows you ask questions early)
Hi Balaji, quick update on the car-telemetry task. I have the sensor -> AWS -> table flow working
end to end (simulated tyre pressure, accelerator/clutch and fuel efficiency -> IoT Core -> Firehose -> S3 -> Athena,
with an email alert for low tyre pressure, and a DynamoDB table with each car's latest reading).
I also built the chatbot: it works in keyword mode, and I'm connecting an LLM. Amazon Bedrock is blocked on my personal
AWS account ("Access to Bedrock models is not allowed for this account"), so I'm using a free Google Gemini API key for the LLM step.
One doubt: when you said "u can use github, openai also", did you mean (1) host the code on GitHub and/or use OpenAI models
for the chatbot, or (2) something else? I can't pay for an OpenAI key, so is a free Gemini key acceptable, and is a private GitHub repo OK?
Also, whitepaper status: [still Under Review in ConsultKX; I'm preparing the appendix, references, acronyms,
AI-usage note and version history locally as you suggested].

## B. Friday message (with attachments)
Hi Balaji, the connected-car telemetry demo is ready.
Flow: simulated car sensors (tyre pressure x4, accelerator + clutch, fuel level + efficiency) -> AWS IoT Core ->
(1) Firehose -> S3 -> Glue/Athena for history, (2) SNS email when any tyre is below 28 psi, (3) DynamoDB for the latest status
per car in near-instant time -> a chatbot that uses Athena plus an LLM. Everything is deployed as code (CloudFormation) and has
39 automated tests. Attached: implementation guide, screenshots, GitHub repo link [link].
Honest notes: the sensors are simulated (no physical device); the history path is near-real-time (about 1-2 min because of
Firehose buffering) while the DynamoDB view is real time; Bedrock was blocked on my account, so the chatbot calls [Google Gemini model X
through a free API key / runs in keyword mode because ...]. The LLM only picks one of six fixed read-only queries and never writes SQL.
AI usage: Claude helped generate the starter code, the guide structure and the architecture diagram script; I deployed, tested and documented it myself.
It is listed in Appendix B. Could you review and tell me what to improve? I can also walk an architect through it if a viva is needed.
