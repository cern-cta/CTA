#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration

# Single-cluster configuration for CTA-NooBaa integration
# Both CTA and NooBaa in minikube cluster

set -euo pipefail

# Cluster Configuration
CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT:-minikube}"
NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT:-minikube}"
CTA_NAMESPACE="${CTA_NAMESPACE:-dev}"
NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE:-noobaa}"

# Pod names (will be detected automatically)
EOS_MGM_POD=""
NOOBAA_POD=""
CLIENT_POD=""

# Network Configuration
CTA_CLUSTER_IP=""
NOOBAA_CLUSTER_IP=""

# Functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [CLUSTER-CONFIG] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [CLUSTER-CONFIG] ERROR: $*" >&2
    exit 1
}

# Get minikube cluster IP
get_cluster_ip() {
    local cluster_context="$1"
    local profile_name="${cluster_context}"
    
    # Both contexts use minikube profile
    profile_name="minikube"
    
    minikube ip --profile="${profile_name}" 2>/dev/null || {
        error "Failed to get IP for cluster profile: ${profile_name}"
    }
}

# Discover pod names automatically
discover_pods() {
    log "Discovering pod names in both namespaces..."
    
    # Find EOS MGM pod in CTA cluster (dev namespace)
    EOS_MGM_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
        -o name | grep -E "(eos-mgm|mgm)" | head -1 | sed 's|pod/||' || echo "")
    
    if [[ -z "${EOS_MGM_POD}" ]]; then
        # Try alternative names
        EOS_MGM_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
            --no-headers | awk '{print $1}' | grep -E "(eos|cta)" | head -1 || echo "")
    fi
    
    # Find NooBaa pod in NooBaa namespace
    NOOBAA_POD=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get pods -n "${NOOBAA_NAMESPACE}" \
        -o name | grep "noobaa-core" | head -1 | sed 's|pod/||' || echo "")
    
    # Find client pod in CTA namespace
    CLIENT_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
        -o name | grep -E "(client|cta-client)" | head -1 | sed 's|pod/||' || echo "")
    
    log "Discovered pods:"
    log "  EOS MGM Pod (${CTA_CLUSTER_CONTEXT}): ${EOS_MGM_POD:-'NOT FOUND'}"
    log "  NooBaa Pod (${NOOBAA_CLUSTER_CONTEXT}): ${NOOBAA_POD:-'NOT FOUND'}"
    log "  Client Pod (${CTA_CLUSTER_CONTEXT}): ${CLIENT_POD:-'NOT FOUND'}"
}

# Get cluster IPs
get_cluster_ips() {
    log "Getting cluster IP address..."
    
    # Both in same cluster, so same IP
    CTA_CLUSTER_IP=$(get_cluster_ip "${CTA_CLUSTER_CONTEXT}")
    NOOBAA_CLUSTER_IP="${CTA_CLUSTER_IP}"
    
    log "Cluster IP:"
    log "  Minikube Cluster: ${CTA_CLUSTER_IP}"
}

# Test connectivity between namespaces
test_connectivity() {
    log "Testing inter-namespace connectivity..."
    
    # Test from NooBaa namespace to CTA namespace
    if [[ -n "${NOOBAA_POD}" ]]; then
        log "Testing NooBaa → CTA connectivity..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" exec "${NOOBAA_POD}" -c core -- \
            ping -c 1 "${CTA_CLUSTER_IP}" &>/dev/null && {
            log "✓ NooBaa can reach CTA services"
        } || {
            log "✗ NooBaa cannot reach CTA services"
        }
        
        # Test EOS HTTP API endpoint
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" exec "${NOOBAA_POD}" -c core -- \
            curl -k --connect-timeout 5 "https://${CTA_CLUSTER_IP}:8443/.well-known/wlcg-tape-rest-api" &>/dev/null && {
            log "✓ NooBaa can reach EOS HTTP TAPE REST API"
        } || {
            log "✗ NooBaa cannot reach EOS HTTP TAPE REST API"
        }
    fi
}

# Get NooBaa S3 credentials automatically
get_noobaa_credentials() {
    log "Retrieving NooBaa S3 credentials..."
    
    # Get NooBaa S3 service details
    local s3_nodeport=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get svc -n "${NOOBAA_NAMESPACE}" s3 -o jsonpath='{.spec.ports[?(@.name=="s3")].nodePort}' 2>/dev/null || echo "")
    local s3_http_nodeport=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get svc -n "${NOOBAA_NAMESPACE}" s3 -o jsonpath='{.spec.ports[?(@.port==80)].nodePort}' 2>/dev/null || echo "")
    
    # Use HTTP endpoint to avoid SSL issues
    if [[ -n "${s3_http_nodeport}" ]]; then
        NOOBAA_ENDPOINT="http://${NOOBAA_CLUSTER_IP}:${s3_http_nodeport}"
    elif [[ -n "${s3_nodeport}" ]]; then
        NOOBAA_ENDPOINT="https://${NOOBAA_CLUSTER_IP}:${s3_nodeport}"
    else
        log "Warning: Could not determine NooBaa S3 endpoint"
        NOOBAA_ENDPOINT=""
    fi
    
    # Get AWS credentials from NooBaa admin secret
    local aws_access_key=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get secret -n "${NOOBAA_NAMESPACE}" noobaa-admin -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' 2>/dev/null | base64 -d || echo "")
    local aws_secret_key=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get secret -n "${NOOBAA_NAMESPACE}" noobaa-admin -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' 2>/dev/null | base64 -d || echo "")
    
    if [[ -n "${aws_access_key}" && -n "${aws_secret_key}" ]]; then
        AWS_ACCESS_KEY_ID="${aws_access_key}"
        AWS_SECRET_ACCESS_KEY="${aws_secret_key}"
        log "✓ NooBaa S3 credentials retrieved successfully"
        log "✓ NooBaa S3 endpoint: ${NOOBAA_ENDPOINT}"
    else
        log "✗ Failed to retrieve NooBaa S3 credentials"
        AWS_ACCESS_KEY_ID=""
        AWS_SECRET_ACCESS_KEY=""
    fi
}

# Export configuration for other scripts
export_config() {
    log "Exporting configuration..."
    
    cat > /tmp/cluster_config.env << EOF
# Single-cluster configuration
export CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT}"
export NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT}"
export CTA_NAMESPACE="${CTA_NAMESPACE}"
export NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE}"

# Pod names
export EOS_MGM_POD="${EOS_MGM_POD}"
export NOOBAA_POD="${NOOBAA_POD}"
export CLIENT_POD="${CLIENT_POD}"

# Cluster IP (same for both namespaces)
export CTA_CLUSTER_IP="${CTA_CLUSTER_IP}"
export NOOBAA_CLUSTER_IP="${CTA_CLUSTER_IP}"

# EOS configuration
export EOS_MGM_HOST="${CTA_CLUSTER_IP}"
export EOS_PORT="8443"
export EOS_ENDPOINT="https://${CTA_CLUSTER_IP}:8443"

# NooBaa S3 configuration
export NOOBAA_ENDPOINT="${NOOBAA_ENDPOINT}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
EOF
    
    chmod +x /tmp/cluster_config.env
    log "Configuration exported to: /tmp/cluster_config.env"
    log "Source this file in other scripts: source /tmp/cluster_config.env"
}

# Show current kubectl contexts
show_contexts() {
    log "Current kubectl contexts:"
    kubectl config get-contexts
    echo
    log "Current context: $(kubectl config current-context)"
}

# Validate cluster setup
validate_setup() {
    log "Validating single-cluster setup..."
    
    # Check if cluster is accessible
    kubectl --context="${CTA_CLUSTER_CONTEXT}" get nodes &>/dev/null || {
        error "Cannot access cluster context: ${CTA_CLUSTER_CONTEXT}"
    }
    
    # Both use same cluster context
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get nodes &>/dev/null || {
        error "Cannot access cluster context: ${NOOBAA_CLUSTER_CONTEXT}"
    }
    
    log "✓ Cluster is accessible"
    
    # Check if required pods exist in their respective namespaces
    [[ -n "${EOS_MGM_POD}" ]] || error "EOS MGM pod not found in ${CTA_NAMESPACE} namespace"
    [[ -n "${NOOBAA_POD}" ]] || error "NooBaa pod not found in ${NOOBAA_NAMESPACE} namespace"
    
    log "✓ Required pods found in their namespaces"
}

# Main function
main() {
    log "Setting up single-cluster configuration for CTA-NooBaa integration"
    
    show_contexts
    discover_pods
    get_cluster_ips
    validate_setup
    test_connectivity
    get_noobaa_credentials
    export_config
    
    log "Single-cluster configuration completed successfully"
    log ""
    log "Next steps:"
    log "1. Source the configuration: source /tmp/cluster_config.env"
    log "2. Run modified setup scripts"
    log "3. Test the integration"
}

# Handle help option
case "${1:-}" in
    --help|-h)
        cat << EOF
Usage: $0 [options]

Configure single-cluster setup for CTA-NooBaa integration.

Environment Variables:
  CTA_CLUSTER_CONTEXT      CTA cluster context (default: minikube)
  NOOBAA_CLUSTER_CONTEXT   NooBaa cluster context (default: minikube)
  CTA_NAMESPACE            CTA namespace (default: dev)
  NOOBAA_NAMESPACE         NooBaa namespace (default: noobaa)

This script:
1. Discovers pod names in both namespaces
2. Gets cluster IP address
3. Tests cross-cluster connectivity
4. Exports configuration for other scripts

Options:
  -h, --help              Show this help message

EOF
        exit 0
        ;;
esac

# Execute main function
main "$@"