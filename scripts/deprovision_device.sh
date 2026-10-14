#!/usr/bin/env bash
# Remove a device completely. Run this for every provisioned device BEFORE deleting the stack
# (the stack's device policy cannot be deleted while a certificate is still attached to it).
# Usage: bash scripts/deprovision_device.sh CAR-101
set -euo pipefail
THING="${1:?usage: deprovision_device.sh CAR-101}"
REGION="${AWS_REGION:-ap-south-1}"
CERT_ARN="$(cat "certs/$THING/cert_arn.txt")"
CERT_ID="${CERT_ARN##*/}"
aws iot detach-thing-principal --thing-name "$THING" --principal "$CERT_ARN" --region "$REGION"
aws iot detach-policy --policy-name vehicle-device-policy --target "$CERT_ARN" --region "$REGION"
aws iot update-certificate --certificate-id "$CERT_ID" --new-status INACTIVE --region "$REGION"
aws iot delete-certificate --certificate-id "$CERT_ID" --region "$REGION"
aws iot delete-thing --thing-name "$THING" --region "$REGION"
rm -rf "certs/$THING"
echo "Removed $THING."
