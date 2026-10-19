#!/usr/bin/env bash
# Create an IoT "thing" + X.509 certificate for ONE vehicle and attach the least-privilege device policy.
# Running it again is safe: if the device already exists with its key in certs/<thing>/, nothing changes.
# If the thing exists with a certificate whose private key is NOT here (an earlier session), it stops and
# tells you to remove it first; otherwise the old certificate would stay attached and block the tear-down.
# Usage: bash scripts/provision_device.sh CAR-101
set -euo pipefail
THING="${1:?usage: provision_device.sh CAR-101}"
REGION="${STACK_REGION:-ap-south-1}"
POLICY="${DEVICE_POLICY:-vehicle-device-policy}"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
DIR="certs/$THING"
[[ "$THING" =~ ^CAR-[0-9]{3}$ ]] || { echo "Thing name must look like CAR-101"; exit 1; }

if aws iot describe-thing --thing-name "$THING" >/dev/null 2>&1; then
  EXISTING="$(aws iot list-thing-principals --thing-name "$THING" --query principals --output text | tr '\t' '\n' | sed '/^$/d;/^None$/d')"
  if [ -n "$EXISTING" ]; then
    if [ -f "$DIR/cert_arn.txt" ] && [ -f "$DIR/private.key" ] && grep -qxF "$(cat "$DIR/cert_arn.txt")" <<<"$EXISTING"; then
      echo "$THING is already provisioned and its key is in $DIR/. Nothing to do."
      exit 0
    fi
    echo "ERROR: $THING already exists in AWS with a certificate whose private key is not in $DIR/"
    echo "       (left over from an earlier session). Remove it first, then run this again:"
    echo "       bash scripts/deprovision_device.sh $THING"
    exit 1
  fi
fi

mkdir -p "$DIR"
aws iot create-thing --thing-name "$THING" >/dev/null
CERT_ARN=$(aws iot create-keys-and-certificate --set-as-active \
  --certificate-pem-outfile "$DIR/cert.pem" --public-key-outfile "$DIR/public.key" \
  --private-key-outfile "$DIR/private.key" --query certificateArn --output text)
echo "$CERT_ARN" > "$DIR/cert_arn.txt"     # written at once, so a failed step below can still be cleaned up
chmod 600 "$DIR/private.key"
aws iot attach-policy --policy-name "$POLICY" --target "$CERT_ARN"
aws iot attach-thing-principal --thing-name "$THING" --principal "$CERT_ARN"
curl -sS -o "$DIR/AmazonRootCA1.pem" https://www.amazontrust.com/repository/AmazonRootCA1.pem
echo "Provisioned $THING. Files in $DIR/ (NEVER commit certs/; the private key IS the device identity)."
