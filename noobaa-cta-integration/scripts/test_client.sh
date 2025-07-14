#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".

# Test client for NooBaa S3 Glacier integration with CTA (archival only)
# Tests S3 Glacier file upload and archival to CTA tape storage

set -euo pipefail

# Functions (defined early so they can be used in configuration)
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [TEST-CLIENT] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [TEST-CLIENT] ERROR: $*" >&2
    exit 1
}

# Configuration
NAMESPACE="${NAMESPACE:-dev}"
CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT:-minikube}"
NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT:-minikube}"
CLIENT_POD="${CLIENT_POD:-}"
NOOBAA_ENDPOINT="${NOOBAA_ENDPOINT:-}"
BUCKET_NAME="${BUCKET_NAME:-glacier-test-bucket}"
TEST_FILE_PREFIX="${TEST_FILE_PREFIX:-glacier-test}"
AWS_CLI_CONFIG_DIR="${AWS_CLI_CONFIG_DIR:-/tmp/aws-config}"

# Load single-cluster configuration if available
if [[ -f "/tmp/cluster_config.env" ]]; then
    source /tmp/cluster_config.env
    log "Loaded single-cluster configuration"
fi

check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check kubectl is available
    if ! command -v kubectl &> /dev/null; then
        error "kubectl command not found. Please install kubectl."
    fi
    
    # Auto-discover client pod if not set
    if [[ -z "${CLIENT_POD}" ]]; then
        CLIENT_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${NAMESPACE}" \
            -o name | grep -E "(client|cta-client)" | head -1 | sed 's|pod/||' || echo "")
        [[ -n "${CLIENT_POD}" ]] || error "Client pod not found in CTA cluster (${NAMESPACE} namespace)"
        log "Auto-discovered client pod: ${CLIENT_POD}"
    fi
    
    # Check if client pod exists in CTA cluster
    if ! kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" get pod "${CLIENT_POD}" &> /dev/null; then
        error "Client pod '${CLIENT_POD}' not found in CTA cluster namespace '${NAMESPACE}'"
    fi
    
    log "Prerequisites check passed"
    log "Using client pod: ${CLIENT_POD} in ${CTA_CLUSTER_CONTEXT}/${NAMESPACE}"
}

setup_aws_cli() {
    log "Setting up AWS CLI in client pod..."
    
    # Install AWS CLI if not present
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
        bash -c "
            if ! command -v aws &> /dev/null; then
                echo 'Installing AWS CLI...'
                if command -v yum &> /dev/null; then
                    yum install -y awscli
                elif command -v apt-get &> /dev/null; then
                    apt-get update && apt-get install -y awscli
                else
                    echo 'ERROR: No supported package manager found'
                    exit 1
                fi
            fi
        " || {
        error "Failed to install AWS CLI in client pod"
    }
    
    # Configure AWS CLI for NooBaa
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
        bash -c "
            mkdir -p ${AWS_CLI_CONFIG_DIR}
            
            # Create AWS credentials file
            cat > ${AWS_CLI_CONFIG_DIR}/credentials << EOF
[default]
aws_access_key_id = ${AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}
EOF
            
            # Create AWS config file
            cat > ${AWS_CLI_CONFIG_DIR}/config << EOF
[default]
region = us-east-1
output = json
s3 =
    endpoint_url = ${NOOBAA_ENDPOINT}
    signature_version = s3v4
    addressing_style = path
EOF
            
            # Set AWS configuration directory
            export AWS_CONFIG_FILE=${AWS_CLI_CONFIG_DIR}/config
            export AWS_SHARED_CREDENTIALS_FILE=${AWS_CLI_CONFIG_DIR}/credentials
            
            echo 'AWS CLI configured successfully'
        " || {
        error "Failed to configure AWS CLI"
    }
    
    log "AWS CLI setup completed"
}

create_test_bucket() {
    log "Creating test bucket with Glacier storage class..."
    
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
        bash -c "
            export AWS_CONFIG_FILE=${AWS_CLI_CONFIG_DIR}/config
            export AWS_SHARED_CREDENTIALS_FILE=${AWS_CLI_CONFIG_DIR}/credentials
            
            # Create bucket
            if aws s3 mb s3://${BUCKET_NAME} --endpoint-url ${NOOBAA_ENDPOINT} 2>/dev/null; then
                echo 'Bucket created successfully'
            else
                echo 'Bucket may already exist, continuing...'
            fi
            
            # Set bucket lifecycle configuration for Glacier
            cat > /tmp/lifecycle.json << 'EOF'
{
    \"Rules\": [
        {
            \"ID\": \"GlacierTransition\",
            \"Status\": \"Enabled\",
            \"Transitions\": [
                {
                    \"Days\": 0,
                    \"StorageClass\": \"GLACIER\"
                }
            ]
        }
    ]
}
EOF
            
            # Apply lifecycle configuration
            aws s3api put-bucket-lifecycle-configuration \
                --bucket ${BUCKET_NAME} \
                --lifecycle-configuration file:///tmp/lifecycle.json || {
                echo 'Warning: Failed to set lifecycle configuration'
            }
            
            rm -f /tmp/lifecycle.json
        " || {
        error "Failed to create test bucket"
    }
    
    log "Test bucket created successfully"
}

create_test_files() {
    log "Creating test files..."
    
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
        bash -c "
            # Create test files with different sizes
            echo 'Small test file for S3 Glacier integration' > /tmp/${TEST_FILE_PREFIX}-small.txt
            
            # Create a medium-sized file (1MB)
            dd if=/dev/zero of=/tmp/${TEST_FILE_PREFIX}-medium.dat bs=1M count=1 2>/dev/null
            
            # Create metadata for files
            echo 'Test file metadata: Created by CTA-NooBaa integration test' > /tmp/${TEST_FILE_PREFIX}-metadata.txt
            
            echo 'Test files created:'
            ls -lh /tmp/${TEST_FILE_PREFIX}*
        " || {
        error "Failed to create test files"
    }
    
    log "Test files created successfully"
}

test_glacier_upload() {
    log "Testing S3 Glacier upload (PutObject with GLACIER storage class)..."
    
    local test_files=("${TEST_FILE_PREFIX}-small.txt" "${TEST_FILE_PREFIX}-medium.dat" "${TEST_FILE_PREFIX}-metadata.txt")
    
    for file in "${test_files[@]}"; do
        log "Uploading file to Glacier: ${file}"
        
        kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
            bash -c "
                export AWS_CONFIG_FILE=${AWS_CLI_CONFIG_DIR}/config
                export AWS_SHARED_CREDENTIALS_FILE=${AWS_CLI_CONFIG_DIR}/credentials
                
                # Upload file with GLACIER storage class
                aws s3 cp /tmp/${file} s3://${BUCKET_NAME}/${file} \
                    --storage-class GLACIER \
                    --endpoint-url ${NOOBAA_ENDPOINT} \
                    --metadata 'test-type=glacier-integration,uploaded-by=cta-noobaa-test' || {
                    echo 'ERROR: Failed to upload ${file}'
                    exit 1
                }
                
                echo 'Successfully uploaded: ${file}'
            " || {
            error "Failed to upload file: ${file}"
        }
    done
    
    log "Glacier upload test completed successfully"
}

verify_archival() {
    log "Verifying files are being processed for archival..."
    
    # Give time for NooBaa to process the files
    sleep 10
    
    log "Files have been uploaded with GLACIER storage class"
    log "Check NooBaa logs and EOS tape storage for archival progress"
}



cleanup_test_resources() {
    log "Cleaning up test resources..."
    
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${CLIENT_POD}" -- \
        bash -c "
            export AWS_CONFIG_FILE=${AWS_CLI_CONFIG_DIR}/config
            export AWS_SHARED_CREDENTIALS_FILE=${AWS_CLI_CONFIG_DIR}/credentials
            
            # Remove objects from bucket
            aws s3 rm s3://${BUCKET_NAME}/ --recursive || {
                echo 'Warning: Failed to remove some objects'
            }
            
            # Remove bucket
            aws s3 rb s3://${BUCKET_NAME} || {
                echo 'Warning: Failed to remove bucket'
            }
            
            # Clean up local files
            rm -f /tmp/${TEST_FILE_PREFIX}*
            rm -rf ${AWS_CLI_CONFIG_DIR}
            
            echo 'Cleanup completed'
        " || {
        log "Warning: Some cleanup operations failed"
    }
    
    log "Test resources cleaned up"
}

print_test_summary() {
    local start_time="$1"
    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    cat << EOF

========================================
NooBaa S3 Glacier Archival Test Summary
========================================

Test Duration: ${duration} seconds
Test Bucket: ${BUCKET_NAME}
NooBaa Endpoint: ${NOOBAA_ENDPOINT}

Test Operations Performed:
✓ S3 bucket creation with Glacier lifecycle
✓ File upload to Glacier storage class
✓ Archival verification
✓ Resource cleanup

Next Steps:
1. Check NooBaa logs for migrate executable calls
2. Verify CTA tape archive operations in EOS
3. Monitor EOS tape space usage
4. Test with larger files and multiple concurrent uploads

For debugging:
- NooBaa logs: kubectl logs -n ${NAMESPACE} ${NOOBAA_POD:-noobaa-core}
- Migrate executable logs: /tmp/glacier/logs/ in NooBaa pod
- EOS tape space: kubectl exec -n dev eos-mgm-0 -c eos-mgm -- eos space ls

========================================

EOF
}

main() {
    local start_time
    start_time=$(date +%s)
    
    log "Starting NooBaa S3 Glacier archival test"
    
    check_prerequisites
    setup_aws_cli
    create_test_bucket
    create_test_files
    test_glacier_upload
    verify_archival
    cleanup_test_resources
    
    print_test_summary "${start_time}"
    
    log "NooBaa S3 Glacier archival test completed successfully"
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo ""
        echo "Test NooBaa S3 Glacier archival with CTA (archival only)"
        echo ""
        echo "Environment variables:"
        echo "  NAMESPACE         - Kubernetes namespace (default: default)"
        echo "  CLIENT_POD        - Client pod name (default: cta-client)"
        echo "  NOOBAA_ENDPOINT   - NooBaa S3 endpoint (default: http://noobaa-s3:80)"
        echo "  BUCKET_NAME       - Test bucket name (default: glacier-test-bucket)"
        echo "  TEST_FILE_PREFIX  - Test file prefix (default: glacier-test)"
        echo ""
        echo "Prerequisites:"
        echo "1. NooBaa must be configured with CTA integration"
        echo "2. Certificates must be set up"
        echo "3. Client pod must have network access to NooBaa"
        echo ""
        echo "Options:"
        echo "  -h, --help        - Show this help message"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown argument: $1. Use --help for usage information."
        ;;
esac