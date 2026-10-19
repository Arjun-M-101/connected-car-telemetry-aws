#!/usr/bin/env bash
# Tear down the connected-car telemetry project COMPLETELY and prove that nothing is left.
#
#   bash scripts/teardown.sh            shows what it found, asks you to type DELETE, then deletes
#   bash scripts/teardown.sh --yes      same, without the question
#   bash scripts/teardown.sh --verify   only checks what is left; changes nothing
#
# Why this script exists (what went wrong with the old four-command tear-down):
#   1. Devices were removed using the local certs/ folder. If a thing had an OLDER certificate too
#      (provisioned in an earlier session), that certificate stayed attached, delete-thing failed
#      ("still attached to one or more principals") and the stack could not delete vehicle-device-policy.
#      -> This script asks AWS which certificates are attached; it never relies on local files.
#   2. $BUCKET was empty in a new CloudShell session, so "aws s3 rm" did nothing and the stack could
#      not delete the non-empty bucket.  -> This script finds the bucket itself.
#   3. Firehose can still write its last 60-second buffer AFTER the bucket was emptied.
#      -> This script switches the IoT rules off and waits for the buffer before emptying.
#   4. "wait stack-delete-complete" sat silently and the check command did not look for
#      DELETE_IN_PROGRESS, so a stuck delete looked finished.
#      -> This script prints the stack status every 15 s, explains a DELETE_FAILED, fixes the cause
#         and retries, then checks every resource type one by one.
#
# Safe to run again at any time (for example after CloudShell timed out): every step first asks AWS
# what still exists. It needs no local state: no certs/ folder, no $BUCKET, no saved variables.
set -uo pipefail

STACK="${STACK_NAME:-vehicle-telemetry}"
REGION="${STACK_REGION:-ap-south-1}"
POLICY="${DEVICE_POLICY:-vehicle-device-policy}"
FLUSH_WAIT="${FLUSH_WAIT:-75}"          # seconds; Firehose buffers up to 60 s
POLL="${POLL:-15}"                      # seconds between stack status checks
MAX_WAIT="${MAX_WAIT:-1800}"            # give up waiting for one delete attempt after 30 min
POLICY_RETRY="${POLICY_RETRY:-20}"      # policy delete retries (x POLL s): detach can take up to 5 min
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""

STACK_LC="$(printf '%s' "$STACK" | tr '[:upper:]' '[:lower:]')"
BUCKET_PREFIX="${STACK_LC}-databucket-"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." 2>/dev/null && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

if [ -t 1 ]; then G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; B=$'\e[1m'; N=$'\e[0m'; else G=; R=; Y=; B=; N=; fi
say()  { printf '%s\n' "$*"; }
step() { printf '\n%s== %s ==%s\n' "$B" "$*" "$N"; }
ok()   { printf '  %s[OK]%s %s\n' "$G" "$N" "$*"; }
warn() { printf '  %s[!!]%s %s\n' "$Y" "$N" "$*"; }
bad()  { printf '  %s[LEFT]%s %s\n' "$R" "$N" "$*"; }
lines() { tr '\t' '\n' | sed '/^$/d;/^None$/d'; }          # AWS CLI text output -> one item per line

# ------------------------------------------------------------------ helpers that ask AWS
stack_status() {   # prints the stack status, or NONE when the stack does not exist
  local out
  if out=$(aws cloudformation describe-stacks --stack-name "$STACK" \
            --query "Stacks[0].StackStatus" --output text 2>&1); then
    printf '%s\n' "$out"
  elif grep -q "does not exist" <<<"$out"; then
    echo NONE
  else
    echo "ERROR"; printf '%s\n' "$out" >&2
  fi
}

stack_resource() {  # $1 = logical id -> physical id, only while that resource is not yet deleted
  aws cloudformation list-stack-resources --stack-name "$STACK" --output text \
    --query "StackResourceSummaries[?LogicalResourceId=='$1' && ResourceStatus!='DELETE_COMPLETE'].PhysicalResourceId" \
    2>/dev/null | lines | head -1
}

stack_resources_of_type() {  # $1 = resource type -> physical ids not yet deleted
  aws cloudformation list-stack-resources --stack-name "$STACK" --output text \
    --query "StackResourceSummaries[?ResourceType=='$1' && ResourceStatus!='DELETE_COMPLETE'].PhysicalResourceId" \
    2>/dev/null | lines
}

find_buckets() {   # the stack's bucket while the stack knows it, else any bucket with our name prefix
  local b
  b="$(stack_resource DataBucket)"
  if [ -n "$b" ]; then echo "$b"; return; fi
  aws s3api list-buckets --output text \
    --query "Buckets[?starts_with(Name, '$BUCKET_PREFIX')].Name" 2>/dev/null | lines
}

policy_exists() { aws iot get-policy --policy-name "$POLICY" >/dev/null 2>&1; }

project_things() {  # things created by scripts/provision_device.sh are named CAR-NNN
  aws iot list-things --output text --query "things[?starts_with(thingName, 'CAR-')].thingName" \
    2>/dev/null | lines | grep -E '^CAR-[0-9]{3}$' || true
}

certs_to_remove() { # every certificate attached to our policy or to one of our things (unique)
  {
    if policy_exists; then
      aws iot list-targets-for-policy --policy-name "$POLICY" --query targets --output text 2>/dev/null | lines
    fi
    local t
    for t in $(project_things); do
      aws iot list-thing-principals --thing-name "$t" --query principals --output text 2>/dev/null | lines
    done
  } | grep ':cert/' | sort -u || true
}

DETACHED=0   # set to 1 when this run detached something from the device policy

retry() {  # retry N SECONDS cmd... -> runs cmd until it succeeds; last error stays in $TMP/err
  local n="$1" s="$2"; shift 2
  local i
  for ((i = 1; i <= n; i++)); do
    if "$@" >"$TMP/out" 2>"$TMP/err"; then return 0; fi
    grep -qiE "not ?found|does not exist" "$TMP/err" && return 0   # already gone = success
    [ "$i" -lt "$n" ] && sleep "$s"
  done
  return 1
}

# ------------------------------------------------------------------ cleanup steps
stop_local_producers() {
  if command -v pkill >/dev/null 2>&1; then
    pkill -f "simulator.py" 2>/dev/null && ok "stopped simulator.py" || ok "no simulator.py running"
    pkill -f "device_mqtt.py" 2>/dev/null && ok "stopped device_mqtt.py" || ok "no device_mqtt.py running"
  else
    warn "pkill not found: press Ctrl+C in any tab that still runs simulator.py"
  fi
}

disable_rules() {
  local r found=0
  for r in $(stack_resources_of_type "AWS::IoT::TopicRule"); do
    found=1
    if aws iot disable-topic-rule --rule-name "$r" >/dev/null 2>&1; then ok "disabled rule $r"
    else warn "could not disable rule $r (the stack delete removes it anyway)"; fi
  done
  [ "$found" = 1 ] || ok "no IoT rules left"
}

remove_devices() {
  local certs things c t id others p
  certs="$(certs_to_remove)"
  things="$(project_things)"
  if [ -z "$certs$things" ]; then ok "no device certificates or CAR-NNN things left"; return 0; fi
  for c in $certs; do
    id="${c##*/}"
    for t in $(aws iot list-principal-things --principal "$c" --query things --output text 2>/dev/null | lines); do
      aws iot detach-thing-principal --thing-name "$t" --principal "$c" >/dev/null 2>&1 \
        && ok "detached certificate ${id:0:12}... from thing $t"
    done
    others=""
    for p in $(aws iot list-attached-policies --target "$c" --query "policies[].policyName" --output text 2>/dev/null | lines); do
      if [ "$p" = "$POLICY" ]; then
        aws iot detach-policy --policy-name "$POLICY" --target "$c" >/dev/null 2>&1 \
          && ok "detached $POLICY from certificate ${id:0:12}..." && DETACHED=1
      else
        others="$others $p"
      fi
    done
    if [ -n "$others" ]; then
      warn "certificate ${id:0:12}... also has policy:$others (not from this project), so it is kept"
      continue
    fi
    aws iot update-certificate --certificate-id "$id" --new-status INACTIVE >/dev/null 2>&1
    if retry 6 5 aws iot delete-certificate --certificate-id "$id" --force-delete; then
      ok "deleted certificate ${id:0:12}..."
    else
      warn "certificate ${id:0:12}... not deleted yet: $(head -c 200 "$TMP/err")"
    fi
  done
  for t in $things; do   # detaching is eventually consistent, so retry for up to a minute
    if retry 12 5 aws iot delete-thing --thing-name "$t"; then ok "deleted thing $t"
    else warn "thing $t not deleted yet: $(head -c 200 "$TMP/err")"; fi
  done
}

wait_for_firehose() {
  if [ -n "$(stack_resource DeliveryStream)" ]; then
    # v5.0: the buffer interval is a stack parameter (60 s demo, up to 900 s production); wait at most 5 min,
    # a later file is still handled by the DELETE_FAILED retry, which empties the bucket again.
    local iv
    iv=$(aws cloudformation describe-stacks --stack-name "$STACK" --output text \
         --query "Stacks[0].Parameters[?ParameterKey=='BufferIntervalSeconds'].ParameterValue" 2>/dev/null | lines)
    case "$iv" in ''|*[!0-9]*) iv=60 ;; esac
    [ "$iv" -gt 300 ] && iv=300
    [ $((iv + 15)) -gt "$FLUSH_WAIT" ] && FLUSH_WAIT=$((iv + 15))
    say "  Waiting $FLUSH_WAIT s (Firehose writes its buffer every ${iv} s)..."
    sleep "$FLUSH_WAIT"
    ok "buffer time has passed"
  else
    ok "no delivery stream left, nothing to wait for"
  fi
}

empty_bucket() {   # $1 = bucket name; removes objects, old versions, delete markers, unfinished uploads
  local b="$1" json k id
  if ! aws s3api head-bucket --bucket "$b" >/dev/null 2>&1; then ok "bucket $b is already gone"; return 0; fi
  say "  Emptying s3://$b ..."
  aws s3 rm "s3://$b" --recursive --only-show-errors
  while :; do  # only matters if versioning was ever switched on; harmless otherwise
    json="$(aws s3api list-object-versions --bucket "$b" --max-items 1000 --output json 2>/dev/null)" || break
    printf '%s' "$json" | python3 -c '
import json, sys
d = json.loads(sys.stdin.read() or "{}")
objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in (d.get("Versions") or []) + (d.get("DeleteMarkers") or [])]
if objs:
    print(json.dumps({"Objects": objs[:1000], "Quiet": True}))' >"$TMP/del.json" || break
    [ -s "$TMP/del.json" ] || break
    aws s3api delete-objects --bucket "$b" --delete "file://$TMP/del.json" >/dev/null || break
  done
  aws s3api list-multipart-uploads --bucket "$b" --query "Uploads[].[Key,UploadId]" --output text 2>/dev/null \
    | sed '/^None$/d;/^$/d' | while IFS=$'\t' read -r k id; do
        aws s3api abort-multipart-upload --bucket "$b" --key "$k" --upload-id "$id" >/dev/null 2>&1
      done
  if [ -z "$(aws s3 ls "s3://$b" --recursive 2>/dev/null | head -1)" ]; then ok "bucket $b is empty"
  else warn "bucket $b still has objects (something is still writing to it)"; fi
}

empty_buckets() {
  local b any=0
  for b in $(find_buckets); do any=1; empty_bucket "$b"; done
  [ "$any" = 1 ] || ok "no data bucket left"
}

force_delete_policy() {  # used only when CloudFormation could not delete the device policy itself
  policy_exists || return 0
  local t v i
  for t in $(aws iot list-targets-for-policy --policy-name "$POLICY" --query targets --output text 2>/dev/null | lines); do
    aws iot detach-policy --policy-name "$POLICY" --target "$t" >/dev/null 2>&1
  done
  for v in $(aws iot list-policy-versions --policy-name "$POLICY" --output text \
              --query "policyVersions[?isDefaultVersion==\`false\`].versionId" 2>/dev/null | lines); do
    aws iot delete-policy-version --policy-name "$POLICY" --policy-version-id "$v" >/dev/null 2>&1
  done
  say "  Deleting $POLICY (AWS can need up to 5 minutes after a detach)..."
  for ((i = 1; i <= POLICY_RETRY; i++)); do
    if aws iot delete-policy --policy-name "$POLICY" >/dev/null 2>"$TMP/err" || ! policy_exists; then
      ok "deleted $POLICY"; return 0
    fi
    sleep "$POLL"
  done
  warn "$POLICY still exists: $(head -c 200 "$TMP/err")"
}

wait_stack() {  # waits while the stack is *_IN_PROGRESS, printing every status change
  local st last="" waited=0
  while :; do
    st="$(stack_status)"
    if [ "$st" != "$last" ]; then say "  stack status: $st (after ${waited}s)"; last="$st"; fi
    case "$st" in
      *_IN_PROGRESS) ;;
      *) return 0 ;;
    esac
    if [ "$waited" -ge "$MAX_WAIT" ]; then warn "still $st after ${MAX_WAIT}s"; return 1; fi
    sleep "$POLL"; waited=$((waited + POLL))
  done
}

show_failures() {
  aws cloudformation describe-stack-events --stack-name "$STACK" --max-items 40 --output text \
    --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" 2>/dev/null \
    | sort -u -k1,1 | while IFS=$'\t' read -r res why; do warn "$res: ${why:0:220}"; done
}

delete_stack() {
  local attempt st
  for attempt in 1 2 3; do
    st="$(stack_status)"
    case "$st" in
      NONE) ok "stack $STACK does not exist (deleted)"; return 0 ;;
      ERROR) warn "could not read the stack status (see the error above)"; return 1 ;;
      *_IN_PROGRESS) say "  another operation is running ($st); waiting for it to finish"; wait_stack ;;
    esac
    st="$(stack_status)"
    if [ "$st" != NONE ]; then
      # A policy detached minutes ago can still block CloudFormation (AWS needs up to 5 min),
      # so delete it ourselves first; CloudFormation then treats it as already deleted.
      if [ "$attempt" = 1 ] && { [ "$DETACHED" = 1 ] || [ "$st" = DELETE_FAILED ]; }; then force_delete_policy; fi
      say "  delete attempt $attempt (current status $st)"
      aws cloudformation delete-stack --stack-name "$STACK" || { warn "delete-stack call failed"; return 1; }
      sleep 2
      wait_stack
      st="$(stack_status)"
    fi
    if [ "$st" = NONE ]; then ok "stack $STACK deleted"; return 0; fi
    if [ "$st" = DELETE_FAILED ]; then
      warn "CloudFormation could not delete everything. Reasons:"
      show_failures
      say "  Fixing the usual causes (devices, device policy, bucket), then trying again..."
      remove_devices
      force_delete_policy
      for b in $(find_buckets); do empty_bucket "$b"; done
    fi
  done
  warn "stack is still $(stack_status) after 3 attempts; run this script again in a few minutes"
  return 1
}

remove_orphans() {  # things that can survive a stack delete; only names that belong to this project
  local b g found=0
  for b in $(find_buckets); do
    found=1; empty_bucket "$b"
    aws s3api delete-bucket --bucket "$b" >/dev/null 2>&1 && ok "deleted left-over bucket $b"
  done
  if policy_exists; then found=1; force_delete_policy; fi
  for g in $(aws logs describe-log-groups --log-group-name-prefix "/$STACK" --query "logGroups[].logGroupName" --output text 2>/dev/null | lines) \
           $(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$STACK" --query "logGroups[].logGroupName" --output text 2>/dev/null | lines); do
    found=1
    aws logs delete-log-group --log-group-name "$g" >/dev/null 2>&1 && ok "deleted left-over log group $g"
  done
  [ "$found" = 1 ] || ok "nothing left over outside the stack"
}

# ------------------------------------------------------------------ verification
LEFT=0
check() {  # check "label" "items found (empty = clean)"
  if [ -z "$2" ]; then ok "$1: none"
  else bad "$1: $(printf '%s' "$2" | tr '\n' ' ')"; LEFT=$((LEFT + 1)); fi
}

verify() {
  step "Verification: everything this project creates, checked one by one in $REGION"
  LEFT=0
  local st items
  st="$(stack_status)"; [ "$st" = NONE ] && st=""
  check "CloudFormation stack $STACK" "$st"
  check "S3 data bucket (${BUCKET_PREFIX}*)" "$(aws s3api list-buckets --output text --query "Buckets[?starts_with(Name, '$BUCKET_PREFIX')].Name" 2>/dev/null | lines)"
  if policy_exists; then items="$POLICY"; else items=""; fi
  check "IoT device policy" "$items"
  check "IoT things CAR-NNN" "$(project_things)"
  check "IoT topic rules vehicle_*" "$(aws iot list-topic-rules --output text --query "rules[?starts_with(ruleName, 'vehicle_')].ruleName" 2>/dev/null | lines)"
  check "DynamoDB table" "$(aws dynamodb list-tables --output text --query "TableNames[?starts_with(@, 'vehicle-')]" 2>/dev/null | lines)"
  check "Lambda function" "$(aws lambda list-functions --output text --query "Functions[?starts_with(FunctionName, '$STACK')].FunctionName" 2>/dev/null | lines)"
  check "Kinesis data stream" "$(aws kinesis list-streams --output text --query "StreamNames[?starts_with(@, '$STACK')]" 2>/dev/null | lines)"
  check "SQS dead-letter queue" "$(aws sqs list-queues --queue-name-prefix "$STACK" --output text --query "QueueUrls[]" 2>/dev/null | grep -v '^None$' | lines)"
  check "Firehose delivery stream" "$(aws firehose list-delivery-streams --limit 10000 --output text --query "DeliveryStreamNames[?starts_with(@, '$STACK')]" 2>/dev/null | lines)"
  check "SNS topics" "$(aws sns list-topics --output text --query "Topics[?contains(TopicArn, ':$STACK-')].TopicArn" 2>/dev/null | lines)"
  check "CloudWatch alarms" "$(aws cloudwatch describe-alarms --alarm-name-prefix "$STACK" --output text --query "MetricAlarms[].AlarmName" 2>/dev/null | lines)"
  check "CloudWatch dashboard" "$(aws cloudwatch list-dashboards --dashboard-name-prefix "$STACK" --output text --query "DashboardEntries[].DashboardName" 2>/dev/null | lines)"
  check "CloudWatch log groups" "$( { aws logs describe-log-groups --log-group-name-prefix "/$STACK" --output text --query "logGroups[].logGroupName" 2>/dev/null; echo; aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$STACK" --output text --query "logGroups[].logGroupName" 2>/dev/null; } | lines)"
  check "Glue database vehicle_telemetry" "$(aws glue get-databases --output text --query "DatabaseList[?Name=='vehicle_telemetry'].Name" 2>/dev/null | lines)"
  check "Athena workgroup" "$(aws athena list-work-groups --output text --query "WorkGroups[?Name=='$STACK'].Name" 2>/dev/null | lines)"
  check "IAM roles $STACK-*" "$(aws iam list-roles --output text --query "Roles[?starts_with(RoleName, '$STACK-')].RoleName" 2>/dev/null | lines)"
  if [ -n "$(aws dynamodb list-backups --backup-type SYSTEM --output text --query "BackupSummaries[?starts_with(TableName, 'vehicle-')].BackupName" 2>/dev/null | lines)" ]; then
    say "  (info) DynamoDB keeps a free 'system backup' of the deleted table for 35 days; it costs nothing and expires by itself."
  fi
  if [ "$LEFT" -eq 0 ]; then
    printf '\n%sALL CLEAN: nothing from %s is left in %s.%s\n' "$G$B" "$STACK" "$REGION" "$N"
    return 0
  fi
  printf '\n%s%s item type(s) still exist. Run: bash scripts/teardown.sh   (it is safe to run again)%s\n' "$R$B" "$LEFT" "$N"
  return 1
}

# ------------------------------------------------------------------ main
MODE=delete; ASK=1
for a in "$@"; do
  case "$a" in
    --verify) MODE=verify ;;
    --yes|-y) ASK=0 ;;
    -h|--help) sed -n '2,6p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option $a (use --yes or --verify)"; exit 2 ;;
  esac
done

command -v aws >/dev/null || { echo "The AWS CLI is not installed. Run this in AWS CloudShell."; exit 1; }
ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" \
  || { echo "AWS credentials are not working in this shell. Open AWS CloudShell and try again."; exit 1; }
say "${B}Account${N} ...${ACCOUNT: -4}   ${B}Region${N} $REGION   ${B}Stack${N} $STACK"

if [ "$MODE" = verify ]; then verify; exit $?; fi

STATUS="$(stack_status)"
say "Stack status now: $STATUS"
say "Data bucket:      $(find_buckets | tr '\n' ' ')"
say "Device things:    $(project_things | tr '\n' ' ')"
say "This deletes the whole project from $REGION: data, tables, rules, devices, alarms, logs."
if [ "$ASK" = 1 ]; then
  read -r -p "Type DELETE (capital letters) to continue: " answer
  [ "$answer" = DELETE ] || { echo "Nothing was changed."; exit 1; }
fi

step "1/7 Stop simulators and device publishers running in this CloudShell"; stop_local_producers
step "2/7 Switch off the IoT rules so no new data arrives while we clean up"; disable_rules
step "3/7 Remove device certificates and things (asks AWS, not the local certs/ folder)"; remove_devices
step "4/7 Let Firehose write its last buffer before the bucket is emptied"; wait_for_firehose
step "5/7 Empty the data bucket (found automatically; \$BUCKET is not needed)"; empty_buckets
step "6/7 Delete the CloudFormation stack (status is printed while it runs)"; delete_stack
step "7/7 Remove anything that can outlive the stack, and local device keys"
remove_orphans
for d in "$REPO_DIR/certs" "$PWD/certs"; do
  [ -d "$d" ] && rm -rf "$d" && ok "deleted local private keys in $d"
done
verify; rc=$?
cat <<EOF

Still to do by hand (outside AWS):
  1. Delete the LLM API key: https://aistudio.google.com -> API keys -> delete.
  2. In this terminal run:  unset LLM_API_KEY LLM_MODEL
  3. Tomorrow: Billing and Cost Management -> Bills: only a few rupees for today.
EOF
exit $rc
