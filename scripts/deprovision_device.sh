#!/usr/bin/env bash
# Remove ONE device completely: every certificate attached to the thing, the policy attachments,
# and the thing itself. It asks AWS what is attached, so it also removes certificates from earlier
# sessions whose files are no longer in certs/ (the cause of "Thing ... is still attached to one or
# more principals"). Safe to run again. For a full tear-down use scripts/teardown.sh instead.
# Usage: bash scripts/deprovision_device.sh CAR-101
set -uo pipefail
THING="${1:?usage: deprovision_device.sh CAR-101}"
REGION="${STACK_REGION:-ap-south-1}"
POLICY="${DEVICE_POLICY:-vehicle-device-policy}"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
[[ "$THING" =~ ^CAR-[0-9]{3}$ ]] || { echo "Thing name must look like CAR-101"; exit 1; }

lines() { tr '\t' '\n' | sed '/^$/d;/^None$/d'; }

remove_cert() {  # $1 = certificate ARN
  local c="$1" id="${1##*/}" t
  for t in $(aws iot list-principal-things --principal "$c" --query things --output text 2>/dev/null | lines); do
    aws iot detach-thing-principal --thing-name "$t" --principal "$c" >/dev/null 2>&1
  done
  aws iot detach-policy --policy-name "$POLICY" --target "$c" >/dev/null 2>&1
  aws iot update-certificate --certificate-id "$id" --new-status INACTIVE >/dev/null 2>&1
  for _ in 1 2 3 4 5 6; do   # detaching is eventually consistent
    if aws iot delete-certificate --certificate-id "$id" --force-delete >/dev/null 2>&1; then
      echo "  deleted certificate ${id:0:12}..."; return 0
    fi
    aws iot describe-certificate --certificate-id "$id" >/dev/null 2>&1 || return 0   # already gone
    sleep 5
  done
  echo "  WARNING: certificate ${id:0:12}... could not be deleted yet; run this script again in a minute"
}

CERTS=""
if aws iot describe-thing --thing-name "$THING" >/dev/null 2>&1; then
  CERTS="$(aws iot list-thing-principals --thing-name "$THING" --query principals --output text | lines | grep ':cert/')"
fi
# a certificate recorded locally but no longer attached to the thing (half-finished earlier run)
if [ -f "certs/$THING/cert_arn.txt" ]; then CERTS="$(printf '%s\n%s\n' "$CERTS" "$(cat "certs/$THING/cert_arn.txt")")"; fi

for C in $(printf '%s\n' "$CERTS" | sed '/^$/d' | sort -u); do remove_cert "$C"; done

if aws iot describe-thing --thing-name "$THING" >/dev/null 2>&1; then
  for _ in $(seq 1 12); do
    if aws iot delete-thing --thing-name "$THING" >/dev/null 2>/tmp/deprov_err; then break; fi
    sleep 5
  done
  if aws iot describe-thing --thing-name "$THING" >/dev/null 2>&1; then
    echo "ERROR: $THING still exists: $(head -c 200 /tmp/deprov_err)"; exit 1
  fi
fi
rm -rf "certs/$THING"
echo "Removed $THING."
