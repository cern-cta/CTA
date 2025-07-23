#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".

# Generate EOS tokens in EOS pod and transfer them to NooBaa pod
# This script runs from the host and coordinates token generation/transfer

set -euo pipefail

# Configuration
CTA_NAMESPACE="${CTA_NAMESPACE:-dev}"
NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE:-noobaa}"
EOS_MGM_HOST="${EOS_MGM_HOST:-ctaeos}"
TOKEN_VALIDITY_HOURS="${TOKEN_VALIDITY_HOURS:-23}"
TOKEN_CACHE_FILE="${TOKEN_CACHE_FILE:-/tmp/eos_power_token}"

# Logging functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [TOKEN-GEN] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [TOKEN-GEN] ERROR: $*" >&2
}

# Find CTA client pod (better user context than MGM pod)
find_cta_client_pod() {
    local client_pod
    
    # Try to find CTA client pod first
    client_pod=$(kubectl get pods -n "${CTA_NAMESPACE}" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -E '^cta-client-[0-9]+$' | head -1)
    
    if [[ -z "${client_pod}" ]]; then
        error "No CTA client pod found in namespace ${CTA_NAMESPACE}"
        return 1
    fi
    
    echo "${client_pod}"
}

# Find NooBaa core pod
find_noobaa_pod() {
    local noobaa_pod
    
    # Try to find noobaa-core-0 specifically first
    noobaa_pod=$(kubectl get pods -n "${NOOBAA_NAMESPACE}" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -E '^noobaa-core-[0-9]+$' | head -1)
    if [[ -n "${noobaa_pod}" ]]; then
        echo "${noobaa_pod}"
        return 0
    fi
    
    # Try common NooBaa pod names
    for pod_pattern in "noobaa-core" "noobaa-operator"; do
        noobaa_pod=$(kubectl get pods -n "${NOOBAA_NAMESPACE}" -l app="${pod_pattern}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [[ -n "${noobaa_pod}" ]]; then
            echo "${noobaa_pod}"
            return 0
        fi
    done
    
    # Fallback: get any pod with noobaa in the name but exclude backing store pods
    noobaa_pod=$(kubectl get pods -n "${NOOBAA_NAMESPACE}" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -i noobaa | grep -v 'backing-store' | head -1)
    
    if [[ -z "${noobaa_pod}" ]]; then
        error "No NooBaa pod found in namespace ${NOOBAA_NAMESPACE}"
        return 1
    fi
    
    echo "${noobaa_pod}"
}

# Generate token in CTA client pod
generate_token_in_client() {
    local client_pod="$1"
    local now later
    
    log "Generating EOS user token in CTA client pod ${client_pod}..."
    
    now=$(date +%s)
    later=$((now + TOKEN_VALIDITY_HOURS * 3600))
    
    # Generate token using eos command in CTA client pod (has proper user1:eosusers context)
    local token_command="eos root://${EOS_MGM_HOST} token --tree --path '/eos/ctaeos' --expires ${later} --owner user1 --group eosusers --permission rwx"
    
    local token
    token=$(kubectl exec -n "${CTA_NAMESPACE}" "${client_pod}" -- bash -c "${token_command}" 2>/dev/null) || {
        error "Failed to generate EOS user token in CTA client pod ${client_pod}"
        return 1
    }
    
    if [[ -z "${token}" ]]; then
        error "Empty token returned from EOS"
        return 1
    fi
    
    # Return token with expiry timestamp (redirect to stderr to avoid capturing in token data)
    log "Token generated successfully" >&2
    echo "${later}:${token}"
}

# Transfer token to NooBaa pod
transfer_token_to_noobaa() {
    local noobaa_pod="$1"
    local token_data="$2"
    
    log "Transferring token to NooBaa pod ${noobaa_pod}..."
    
    # Create token cache file in NooBaa pod
    kubectl exec -n "${NOOBAA_NAMESPACE}" "${noobaa_pod}" -c core -- bash -c "
        echo '${token_data}' > '${TOKEN_CACHE_FILE}' && 
        chmod 600 '${TOKEN_CACHE_FILE}' &&
        echo 'Token cached successfully'
    " || {
        error "Failed to transfer token to NooBaa pod ${noobaa_pod}"
        return 1
    }
    
    log "Token successfully cached in ${TOKEN_CACHE_FILE}"
}

# Verify token in NooBaa pod
verify_token_in_noobaa() {
    local noobaa_pod="$1"
    
    log "Verifying token in NooBaa pod ${noobaa_pod}..."
    
    # Check if token file exists and is readable
    local verification_result
    verification_result=$(kubectl exec -n "${NOOBAA_NAMESPACE}" "${noobaa_pod}" -c core -- bash -c "
        if [[ -f '${TOKEN_CACHE_FILE}' ]]; then
            cached_token=\$(cat '${TOKEN_CACHE_FILE}')
            expiry_time=\$(echo \"\${cached_token}\" | cut -d: -f1)
            current_time=\$(date +%s)
            remaining_time=\$((expiry_time - current_time))
            
            echo \"Token file exists\"
            echo \"Expiry timestamp: \${expiry_time}\"
            echo \"Current timestamp: \${current_time}\"
            echo \"Remaining validity: \${remaining_time} seconds\"
            
            if [[ \${remaining_time} -gt 3600 ]]; then
                echo \"Token is valid\"
                exit 0
            else
                echo \"Token is expired or expires soon\"
                exit 1
            fi
        else
            echo \"Token file does not exist\"
            exit 1
        fi
    ") || {
        error "Token verification failed"
        return 1
    }
    
    log "Token verification result:"
    echo "${verification_result}"
}

# Main function
main() {
    log "Starting token generation and transfer process..."
    
    # Find required pods
    local client_pod noobaa_pod
    client_pod=$(find_cta_client_pod) || exit 1
    noobaa_pod=$(find_noobaa_pod) || exit 1
    
    log "Using CTA client pod: ${client_pod} (namespace: ${CTA_NAMESPACE})"
    log "Using NooBaa pod: ${noobaa_pod} (namespace: ${NOOBAA_NAMESPACE})"
    
    # Generate token in CTA client pod
    local token_data
    token_data=$(generate_token_in_client "${client_pod}" 2>&1 | grep -E '^[0-9]+:' | tail -1) || exit 1
    
    if [[ -z "${token_data}" ]]; then
        error "Failed to capture token data"
        exit 1
    fi
    
    # Transfer token to NooBaa pod
    transfer_token_to_noobaa "${noobaa_pod}" "${token_data}" || exit 1
    
    # Verify token in NooBaa pod
    verify_token_in_noobaa "${noobaa_pod}" || exit 1
    
    log "Token generation and transfer completed successfully"
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo "Options:"
        echo "  --help, -h    Show this help message"
        echo ""
        echo "Environment variables:"
        echo "  CTA_NAMESPACE          CTA namespace (default: dev)"
        echo "  NOOBAA_NAMESPACE       NooBaa namespace (default: noobaa)"
        echo "  EOS_MGM_HOST          EOS MGM hostname (default: ctaeos)"
        echo "  TOKEN_VALIDITY_HOURS   Token validity in hours (default: 23)"
        echo "  TOKEN_CACHE_FILE      Token cache file path (default: /tmp/eos_power_token)"
        exit 0
        ;;
    *)
        main "$@"
        ;;
esac