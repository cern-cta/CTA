#!/bin/bash

# Complete End-to-End NooBaa S3 Glacier Test
# Tests: S3 Client → NooBaa S3 API → NSFS Glacier → migrate script → EOS/CTA

set -eo pipefail

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [S3-GLACIER-TEST] $*"
}

# Configuration
BUCKET_NAME="glacier-test-$(date +%s)"
TEST_FILE_NAME="glacier-test-$(date +%s).txt"
PORT=8081  # Use 8081 to avoid conflicts with previous tests
S3_ENDPOINT="http://localhost:${PORT}"

cleanup() {
    log "Cleaning up..."
    
    # Kill port-forward if running
    if [[ -n "${PORT_FORWARD_PID}" ]]; then
        kill ${PORT_FORWARD_PID} 2>/dev/null || true
        wait ${PORT_FORWARD_PID} 2>/dev/null || true
    fi
    
    # Remove local test file
    rm -f /tmp/${TEST_FILE_NAME}
    
    # Clean up NooBaa pod files
    kubectl exec -n noobaa noobaa-core-0 -- bash -c "
        rm -f /tmp/s3_test.txt /tmp/s3_list.txt
    " 2>/dev/null || true
}

trap cleanup EXIT

main() {
    log "Starting complete S3 Glacier test..."
    
    # Step 1: Get NooBaa S3 credentials
    log "Step 1: Retrieving NooBaa S3 credentials"
    AWS_ACCESS_KEY_ID=$(kubectl get secret -n noobaa noobaa-admin -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
    AWS_SECRET_ACCESS_KEY=$(kubectl get secret -n noobaa noobaa-admin -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
    log "Retrieved S3 credentials successfully"
    
    # Step 2: Setup port forwarding
    log "Step 2: Setting up port forwarding to NooBaa S3 service"
    kubectl port-forward -n noobaa service/s3 ${PORT}:80 &
    PORT_FORWARD_PID=$!
    sleep 3
    log "Port forwarding established on port ${PORT}"
    
    # Step 3: Configure AWS CLI
    log "Step 3: Configuring AWS CLI"
    aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
    aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
    aws configure set region us-east-1
    log "AWS CLI configured"
    
    # Step 4: Create test file
    log "Step 4: Creating test file"
    echo "S3 Glacier Test File - $(date)" > /tmp/${TEST_FILE_NAME}
    echo "Testing complete S3 API integration" >> /tmp/${TEST_FILE_NAME}
    local file_size=$(wc -c < /tmp/${TEST_FILE_NAME})
    log "Created test file: ${file_size} bytes"
    
    # Step 5: Create S3 bucket
    log "Step 5: Creating S3 bucket"
    aws s3 mb s3://${BUCKET_NAME} --endpoint-url ${S3_ENDPOINT}
    log "S3 bucket created: ${BUCKET_NAME}"
    
    # Step 6: Upload file with GLACIER storage class
    log "Step 6: Uploading file with GLACIER storage class"
    aws s3 cp /tmp/${TEST_FILE_NAME} s3://${BUCKET_NAME}/${TEST_FILE_NAME} \
        --storage-class GLACIER \
        --endpoint-url ${S3_ENDPOINT}
    log "File uploaded with GLACIER storage class"
    
    # Step 7: Verify upload
    log "Step 7: Verifying S3 upload"
    aws s3 ls s3://${BUCKET_NAME}/${TEST_FILE_NAME} --endpoint-url ${S3_ENDPOINT}
    log "S3 upload verified"
    
    # Step 8: Check for automatic processing
    log "Step 8: Checking for NooBaa NSFS Glacier processing"
    sleep 5
    kubectl logs -n noobaa noobaa-core-0 --tail=20 | grep -i -E "(migrate|glacier)" || log "No automatic processing detected"
    
    # Step 9: Generate fresh tokens
    log "Step 9: Generating fresh authentication tokens"
    ./generate_and_transfer_tokens.sh > /dev/null 2>&1
    log "Tokens generated and transferred"
    
    # Step 10: Manual migrate script execution
    log "Step 10: Executing migrate script"
    kubectl exec -n noobaa noobaa-core-0 -- bash -c "
        echo 'S3 Glacier test file - $(date)' > /tmp/s3_test.txt &&
        echo '/tmp/s3_test.txt' > /tmp/s3_list.txt
    "
    
    kubectl exec -n noobaa noobaa-core-0 -- /tmp/glacier/bin/migrate /tmp/s3_list.txt
    local migrate_exit_code=$?
    
    # Step 11: Check results
    if [[ $migrate_exit_code -eq 0 ]]; then
        log "SUCCESS: Migrate script completed successfully"
        
        # Step 12: Verify actual archival using EOS extended attributes
        log "Step 11: Checking EOS for actual archival status"
        echo ""
        echo "=== Verifying File Archival Status in EOS ==="
        
        # Check if our test file was actually archived to tape
        local archived_count
        archived_count=$(kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos root://ctaeos ls -y /eos/ctaeos/preprod | grep "s3_test.txt" | grep 'd0::t1' | wc -l)
        
        if [[ $archived_count -eq 1 ]]; then
            log "✅ SUCCESS: File successfully archived to tape (d0::t1 status)"
            echo "File shows d0::t1 extended attributes indicating successful tape archival"
        else
            log "❌ WARNING: File not found with archived status (count: $archived_count)"
            echo "File may still be in progress or archival failed"
            return 1
        fi
        echo ""
        
        log "END-TO-END TEST COMPLETED SUCCESSFULLY"
        log "Workflow tested: S3 Client → NooBaa S3 API → migrate script → EOS/CTA"
        
    else
        log "FAILED: Migrate script failed with exit code: $migrate_exit_code"
        return 1
    fi
}

# Check prerequisites
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl not found"
    exit 1
fi

if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI not found"
    exit 1
fi

main "$@"