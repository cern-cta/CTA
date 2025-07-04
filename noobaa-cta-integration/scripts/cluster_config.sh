#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration

# Cross-cluster configuration for CTA-NooBaa integration
# CTA in minikube cluster, NooBaa in minikube01 cluster

set -euo pipefail

# Cluster Configuration
CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT:-minikube}"
NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT:-minikube01}"
CTA_NAMESPACE="${CTA_NAMESPACE:-dev}"
NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE:-default}"

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
    
    # Extract profile from context (minikube/minikube01)
    if [[ "${cluster_context}" == "minikube01" ]]; then
        profile_name="minikube01"
    elif [[ "${cluster_context}" == "minikube" ]]; then
        profile_name="minikube"
    fi
    
    minikube ip --profile="${profile_name}" 2>/dev/null || {
        error "Failed to get IP for cluster profile: ${profile_name}"
    }
}

# Discover pod names automatically
discover_pods() {
    log "Discovering pod names in both clusters..."
    
    # Find EOS MGM pod in CTA cluster (dev namespace)
    EOS_MGM_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
        -o name | grep -E "(eos-mgm|mgm)" | head -1 | sed 's|pod/||' || echo "")
    
    if [[ -z "${EOS_MGM_POD}" ]]; then
        # Try alternative names
        EOS_MGM_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
            --no-headers | awk '{print $1}' | grep -E "(eos|cta)" | head -1 || echo "")
    fi
    
    # Find NooBaa pod in NooBaa cluster (default namespace)
    NOOBAA_POD=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get pods -n "${NOOBAA_NAMESPACE}" \
        -o name | grep "noobaa-core" | head -1 | sed 's|pod/||' || echo "")
    
    # Find client pod in CTA cluster (dev namespace)
    CLIENT_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
        -o name | grep -E "(client|cta-client)" | head -1 | sed 's|pod/||' || echo "")
    
    log "Discovered pods:"
    log "  EOS MGM Pod (${CTA_CLUSTER_CONTEXT}): ${EOS_MGM_POD:-'NOT FOUND'}"
    log "  NooBaa Pod (${NOOBAA_CLUSTER_CONTEXT}): ${NOOBAA_POD:-'NOT FOUND'}"
    log "  Client Pod (${CTA_CLUSTER_CONTEXT}): ${CLIENT_POD:-'NOT FOUND'}"
}

# Get cluster IPs
get_cluster_ips() {
    log "Getting cluster IP addresses..."
    
    CTA_CLUSTER_IP=$(get_cluster_ip "${CTA_CLUSTER_CONTEXT}")
    NOOBAA_CLUSTER_IP=$(get_cluster_ip "${NOOBAA_CLUSTER_CONTEXT}")
    
    log "Cluster IPs:"
    log "  CTA Cluster (${CTA_CLUSTER_CONTEXT}): ${CTA_CLUSTER_IP}"
    log "  NooBaa Cluster (${NOOBAA_CLUSTER_CONTEXT}): ${NOOBAA_CLUSTER_IP}"
}

# Test connectivity between clusters
test_connectivity() {
    log "Testing cross-cluster connectivity..."
    
    # Test from NooBaa cluster to CTA cluster
    if [[ -n "${NOOBAA_POD}" ]]; then
        log "Testing NooBaa → CTA connectivity..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" exec "${NOOBAA_POD}" -- \
            ping -c 1 "${CTA_CLUSTER_IP}" &>/dev/null && {
            log "✓ NooBaa can reach CTA cluster"
        } || {
            log "✗ NooBaa cannot reach CTA cluster"
        }
        
        # Test EOS HTTP API endpoint
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" exec "${NOOBAA_POD}" -- \
            curl -k --connect-timeout 5 "https://${CTA_CLUSTER_IP}:8443/.well-known/wlcg-tape-rest-api" &>/dev/null && {
            log "✓ NooBaa can reach EOS HTTP TAPE REST API"
        } || {
            log "✗ NooBaa cannot reach EOS HTTP TAPE REST API"
        }
    fi
}

# Export configuration for other scripts
export_config() {
    log "Exporting configuration..."
    
    cat > /tmp/cluster_config.env << EOF
# Cross-cluster configuration
export CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT}"
export NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT}"
export CTA_NAMESPACE="${CTA_NAMESPACE}"
export NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE}"

# Pod names
export EOS_MGM_POD="${EOS_MGM_POD}"
export NOOBAA_POD="${NOOBAA_POD}"
export CLIENT_POD="${CLIENT_POD}"

# Cluster IPs
export CTA_CLUSTER_IP="${CTA_CLUSTER_IP}"
export NOOBAA_CLUSTER_IP="${NOOBAA_CLUSTER_IP}"

# EOS configuration for cross-cluster
export EOS_MGM_HOST="${CTA_CLUSTER_IP}"
export EOS_PORT="8443"
export EOS_ENDPOINT="https://${CTA_CLUSTER_IP}:8443"
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
    log "Validating cross-cluster setup..."
    
    # Check if both clusters are accessible
    kubectl --context="${CTA_CLUSTER_CONTEXT}" get nodes &>/dev/null || {
        error "Cannot access CTA cluster context: ${CTA_CLUSTER_CONTEXT}"
    }
    
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get nodes &>/dev/null || {
        error "Cannot access NooBaa cluster context: ${NOOBAA_CLUSTER_CONTEXT}"
    }
    
    log "✓ Both clusters are accessible"
    
    # Check if required pods exist in their respective namespaces
    [[ -n "${EOS_MGM_POD}" ]] || error "EOS MGM pod not found in CTA cluster (${CTA_NAMESPACE} namespace)"
    [[ -n "${NOOBAA_POD}" ]] || error "NooBaa pod not found in NooBaa cluster (${NOOBAA_NAMESPACE} namespace)"
    
    log "✓ Required pods found in their namespaces"
}

# Main function
main() {
    log "Setting up cross-cluster configuration for CTA-NooBaa integration"
    
    show_contexts
    discover_pods
    get_cluster_ips
    validate_setup
    test_connectivity
    export_config
    
    log "Cross-cluster configuration completed successfully"
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

Configure cross-cluster setup for CTA-NooBaa integration.

Environment Variables:
  CTA_CLUSTER_CONTEXT      CTA cluster context (default: minikube)
  NOOBAA_CLUSTER_CONTEXT   NooBaa cluster context (default: minikube01)
  NAMESPACE                Kubernetes namespace (default: default)

This script:
1. Discovers pod names in both clusters
2. Gets cluster IP addresses
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