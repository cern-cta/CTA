#!/bin/bash

# Simple Retrieval Process Test
# 1. Check existing archived files
# 2. Generate tokens  
# 3. Test recall_final script
# 4. Monitor file status changes

set -euo pipefail

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [RETRIEVAL-TEST] $*"
}

# Step 0: Setup CTA client environment for token generation
log "Step 0: Setting up CTA client environment..."

# Copy client_helper.sh to CTA client pod if it doesn't exist
if ! kubectl exec -n dev cta-client-0 -- test -f /root/client_helper.sh >/dev/null 2>&1; then
    log "Copying client_helper.sh to CTA client pod for token generation..."
    if [[ -f "/home/cirunner/shared/CTA/continuousintegration/orchestration/tests/client_helper.sh" ]]; then
        kubectl cp /home/cirunner/shared/CTA/continuousintegration/orchestration/tests/client_helper.sh dev/cta-client-0:/root/client_helper.sh
        kubectl exec -n dev cta-client-0 -- chmod +x /root/client_helper.sh
        log "✅ client_helper.sh copied and made executable"
    else
        log "⚠️ client_helper.sh not found at /home/cirunner/shared/CTA/continuousintegration/orchestration/tests/"
        log "Please check if the shared CTA directory is accessible"
        exit 1
    fi
else
    log "✅ client_helper.sh already exists in CTA client pod"
fi

# Step 1: Check existing archived files
log "Step 1: Checking existing archived files in EOS..."
kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos ls /eos/ctaeos/preprod/ | head -5

# Get first available file for testing
ARCHIVED_FILE=$(kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos ls /eos/ctaeos/preprod/ | head -1)
if [[ -z "${ARCHIVED_FILE}" ]]; then
    log "ERROR: No archived files found. Upload some files with S3 Glacier first."
    exit 1
fi

log "Using archived file for test: ${ARCHIVED_FILE}"

# Step 2: Check file status (should be offline d0::t1)
log "Step 2: Checking file status..."
kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos ls -y /eos/ctaeos/preprod/ | grep "${ARCHIVED_FILE}"

# Step 3: Generate poweruser tokens
log "Step 3: Generating poweruser tokens..."
./generate_and_transfer_poweruser_token.sh

# Step 4: Create recall file list
log "Step 4: Creating recall file list..."
kubectl exec -n noobaa noobaa-core-0 -- bash -c "
echo '/eos/ctaeos/preprod/${ARCHIVED_FILE}' > /tmp/recall_test.txt
echo 'Recall file list created:'
cat /tmp/recall_test.txt
"

# Step 5: Execute recall_final script
log "Step 5: Executing recall_final script..."
kubectl exec -n noobaa noobaa-core-0 -- /tmp/glacier/bin/recall_final /tmp/recall_test.txt

# Step 6: Monitor file status until online or timeout
log "Step 6: Monitoring file status changes..."
for i in {1..6}; do
    log "Check ${i}/6: File status"
    STATUS=$(kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos ls -y /eos/ctaeos/preprod/ | grep "${ARCHIVED_FILE}" || true)
    echo "${STATUS}"
    
    # Check if file is now online (d1::t1)
    if echo "${STATUS}" | grep -q "d1::t1"; then
        log "🎉 SUCCESS: File is now online (d1::t1) - file staged to disk!"
        break
    fi
    
    sleep 10
done

# Step 7: Test file download to local storage
log "Step 7: Downloading file to local storage..."

# Get token from NooBaa pod
TOKEN=$(kubectl exec -n noobaa noobaa-core-0 -- cut -d: -f2- /tmp/eos_power_token)

# Download file directly to local storage
LOCAL_FILE="./retrieved_${ARCHIVED_FILE}_$(date +%s)"
log "Downloading to local file: ${LOCAL_FILE}"


curl -s -L --insecure \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://eos-mgm-0.eos-mgm.dev.svc.cluster.local:8443/eos/ctaeos/preprod/${ARCHIVED_FILE}" \
  -o "${LOCAL_FILE}"

# Verify local download
if [[ -f "${LOCAL_FILE}" ]] && [[ -s "${LOCAL_FILE}" ]]; then
  log "✅ SUCCESS: File downloaded to local storage!"
  log "Local file: ${LOCAL_FILE}"
  log "File size: $(wc -c < "${LOCAL_FILE}") bytes"
  log "File content:"
  head -1 "${LOCAL_FILE}"
else
  log "❌ FAILED: File download to local storage unsuccessful"
fi

log "✅ Complete retrieval test finished!" 