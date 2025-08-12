#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".

# Generate poweruser tokens for CTA retrieval operations and transfer to NooBaa pod
# Based on /continuousintegration/orchestration/tests/client_rest_api.sh#L36
# This script runs from the host and coordinates token generation/transfer

set -euo pipefail

# Configuration
CTA_NAMESPACE="${CTA_NAMESPACE:-dev}"
NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE:-noobaa}"
EOS_MGM_HOST="${EOS_MGM_HOST:-ctaeos}"
TOKEN_VALIDITY_HOURS="${TOKEN_VALIDITY_HOURS:-24}"
TOKEN_CACHE_FILE="/tmp/eos_power_token"

# Logging functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [POWERUSER-TOKEN] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [POWERUSER-TOKEN] ERROR: $*" >&2
}

# Find CTA client pod
find_cta_client_pod() {
    local client_pod
    
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
    
    noobaa_pod=$(kubectl get pods -n "${NOOBAA_NAMESPACE}" -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -E '^noobaa-core-[0-9]+$' | head -1)
    
    if [[ -z "${noobaa_pod}" ]]; then
        error "No NooBaa core pod found in namespace ${NOOBAA_NAMESPACE}"
        return 1
    fi
    
    echo "${noobaa_pod}"
}

# Generate poweruser token in CTA client pod
generate_poweruser_token() {
    local client_pod="$1"
    
    log "Generating poweruser token with poweruser1:powerusers credentials in pod ${client_pod}"
    
    # Generate token using kubectl exec (no need to copy script)
    local token_output
    token_output=$(kubectl exec -n "${CTA_NAMESPACE}" "${client_pod}" -- bash -c "
        set -eo pipefail
        source /root/client_helper.sh
        
        # Initialize poweruser kerberos
        eospower_kinit >/dev/null 2>&1
        
        # Calculate expiration time
        now=\$(date +%s)
        expires=\$(echo \"\${now}+${TOKEN_VALIDITY_HOURS}*3600\" | bc)
        
        # Generate token using eospower_eos function (poweruser1 credentials)
        token=\$(eospower_eos root://${EOS_MGM_HOST} token --tree --path '/eos/ctaeos' --expires \"\${expires}\")
        
        # Output formatted token
        echo \"\${expires}:\${token}\"
    ") || {
        error "Failed to generate poweruser token in pod ${client_pod}"
        return 1
    }
    
    log "✓ Generated poweruser token successfully"
    echo "${token_output}"
}

# Verify token credentials in CTA client pod
verify_token() {
    local client_pod="$1" 
    local token="$2"
    
    log "Verifying token credentials in pod ${client_pod}"
    
    # Extract just the token part (skip expiration timestamp)
    local token_only=$(echo "${token}" | cut -d: -f2-)
    
    # Verify token using kubectl exec
    local verification_result
    verification_result=$(kubectl exec -n "${CTA_NAMESPACE}" "${client_pod}" -- bash -c "
        source /root/client_helper.sh
        eos root://${EOS_MGM_HOST} token --token '${token_only}' 2>/dev/null || echo 'DECODE_FAILED'
    ") || {
        error "Failed to verify token in pod ${client_pod}"
        return 1
    }
    
    if [[ "${verification_result}" == "DECODE_FAILED" ]] || [[ -z "${verification_result}" ]]; then
        error "Failed to decode token"
        return 1
    fi
    
    # Check for poweruser1:powerusers credentials
    if echo "${verification_result}" | grep -q '"owner": "poweruser1"' && echo "${verification_result}" | grep -q '"group": "powerusers"'; then
        log "✓ Token has correct poweruser1:powerusers credentials"
        return 0
    else
        error "Token does not have poweruser1:powerusers credentials"
        echo "${verification_result}"
        return 1
    fi
}

# Transfer token to NooBaa pod
transfer_to_noobaa() {
    local noobaa_pod="$1"
    local token="$2"
    
    log "Transferring token to NooBaa pod ${noobaa_pod}"
    
    # Create temporary file with token
    local temp_file=$(mktemp)
    echo "${token}" > "${temp_file}"
    
    # Copy to NooBaa pod
    kubectl cp "${temp_file}" "${NOOBAA_NAMESPACE}/${noobaa_pod}:${TOKEN_CACHE_FILE}" || {
        error "Failed to copy token to NooBaa pod"
        rm -f "${temp_file}"
        return 1
    }
    
    # Clean up temporary file
    rm -f "${temp_file}"
    
    log "✓ Token transferred to NooBaa pod successfully"
    return 0
}

# Verify token exists in NooBaa pod
verify_noobaa_token() {
    local noobaa_pod="$1"
    
    log "Verifying token in NooBaa pod ${noobaa_pod}"
    
    # Check if token file exists and has content
    local token_content
    token_content=$(kubectl exec -n "${NOOBAA_NAMESPACE}" "${noobaa_pod}" -- cat "${TOKEN_CACHE_FILE}" 2>/dev/null || echo "")
    
    if [[ -z "${token_content}" ]]; then
        error "Token not found in NooBaa pod"
        return 1
    fi
    
    log "✓ Token found in NooBaa pod"
    log "Token preview: $(echo "${token_content}" | cut -c1-50)..."
    
    return 0
}

# Main function
main() {
    log "Starting poweruser token generation and transfer"
    
    # Find required pods
    local client_pod
    client_pod=$(find_cta_client_pod) || {
        error "Cannot find CTA client pod"
        exit 1
    }
    
    local noobaa_pod
    noobaa_pod=$(find_noobaa_pod) || {
        error "Cannot find NooBaa pod"
        exit 1
    }
    
    log "Using CTA client pod: ${client_pod}"
    log "Using NooBaa pod: ${noobaa_pod}"
    
    # Generate poweruser token in CTA client pod
    local token
    token=$(generate_poweruser_token "${client_pod}") || {
        error "Token generation failed"
        exit 1
    }
    
    # Verify token has correct credentials
    if ! verify_token "${client_pod}" "${token}"; then
        error "Token verification failed"
        exit 1
    fi
    
    # Transfer token to NooBaa pod
    if ! transfer_to_noobaa "${noobaa_pod}" "${token}"; then
        error "Token transfer failed"
        exit 1
    fi
    
    # Verify token exists in NooBaa pod
    if ! verify_noobaa_token "${noobaa_pod}"; then
        error "Token verification in NooBaa pod failed"
        exit 1
    fi
    
    log "✅ Poweruser token generation and transfer completed successfully"
    log "Token cached at: ${TOKEN_CACHE_FILE} in NooBaa pod ${noobaa_pod}"
    
    return 0
}

# Run main function
main "$@"