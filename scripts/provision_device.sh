#!/usr/bin/env bash
# Create an IoT "thing" + X.509 certificate for ONE vehicle and attach the least-privilege device policy.
# Usage: bash scripts/provision_device.sh CAR-101
set -euo pipefail
THING="${1:?usage: provision_device.sh CAR-101}"
REGION="${AWS_REGION:-ap-south-1}"
DIR="certs/$THING"
[[ "$THING" =~ ^CAR-[0-9]{3}$ ]] || { echo "Thing name must look like CAR-101"; exit 1; }
mkdir -p "$DIR"
aws iot create-thing --thing-name "$THING" --region "$REGION" >/dev/null
CERT_ARN=$(aws iot create-keys-and-certificate --set-as-active \
  --certificate-pem-outfile "$DIR/cert.pem" --public-key-outfile "$DIR/public.key" \
  --private-key-outfile "$DIR/private.key" --query certificateArn --output text --region "$REGION")
aws iot attach-policy --policy-name vehicle-device-policy --target "$CERT_ARN" --region "$REGION"
aws iot attach-thing-principal --thing-name "$THING" --principal "$CERT_ARN" --region "$REGION"
curl -sS -o "$DIR/AmazonRootCA1.pem" https://www.amazontrust.com/repository/AmazonRootCA1.pem
chmod 600 "$DIR/private.key"
echo "$CERT_ARN" > "$DIR/cert_arn.txt"
echo "Provisioned $THING. Files in $DIR/ (NEVER commit certs/; the private key IS the device identity)."
