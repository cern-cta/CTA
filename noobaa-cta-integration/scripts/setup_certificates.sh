#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration

# Setup certificates for NooBaa pod to communicate with EOS via HTTPS

set -euo pipefail

# Functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [CERT-SETUP] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [CERT-SETUP] ERROR: $*" >&2
    exit 1
}

# Configuration
CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT:-minikube}"
NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT:-minikube}"
CTA_NAMESPACE="${CTA_NAMESPACE:-dev}"
NOOBAA_NAMESPACE="${NOOBAA_NAMESPACE:-noobaa}"
EOS_MGM_POD="${EOS_MGM_POD:-}"
NOOBAA_POD="${NOOBAA_POD:-}"
EOS_CERT_PATH="/etc/grid-security/certificates"
NOOBAA_CERT_PATH="/tmp/grid-security/certificates"

# Load single-cluster configuration if available
if [[ -f "/tmp/cluster_config.env" ]]; then
    source /tmp/cluster_config.env
    log "Loaded single-cluster configuration"
fi

check_prerequisites() {
    log "Checking prerequisites for single-cluster setup..."
    
    # Check kubectl is available
    if ! command -v kubectl &> /dev/null; then
        error "kubectl command not found. Please install kubectl."
    fi
    
    # Validate cluster context
    kubectl config get-contexts "${CTA_CLUSTER_CONTEXT}" &>/dev/null || {
        error "Cluster context '${CTA_CLUSTER_CONTEXT}' not found"
    }
    
    # Both use same cluster context
    kubectl config get-contexts "${NOOBAA_CLUSTER_CONTEXT}" &>/dev/null || {
        error "Cluster context '${NOOBAA_CLUSTER_CONTEXT}' not found"
    }
    
    # Auto-discover pod names if not set
    if [[ -z "${EOS_MGM_POD}" ]]; then
        EOS_MGM_POD=$(kubectl --context="${CTA_CLUSTER_CONTEXT}" get pods -n "${CTA_NAMESPACE}" \
            -o name | grep -E "(eos|mgm|ctaeos)" | head -1 | sed 's|pod/||' || echo "")
        [[ -n "${EOS_MGM_POD}" ]] || error "EOS MGM pod not found in ${CTA_NAMESPACE} namespace"
        log "Auto-discovered EOS MGM pod: ${EOS_MGM_POD}"
    fi
    
    if [[ -z "${NOOBAA_POD}" ]]; then
        NOOBAA_POD=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get pods -n "${NOOBAA_NAMESPACE}" \
            -o name | grep "noobaa-core" | head -1 | sed 's|pod/||' || echo "")
        [[ -n "${NOOBAA_POD}" ]] || error "NooBaa core pod not found in ${NOOBAA_NAMESPACE} namespace"
        log "Auto-discovered NooBaa pod: ${NOOBAA_POD}"
    fi
    
    # Check if pods exist in their respective namespaces
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${CTA_NAMESPACE}" get pod "${EOS_MGM_POD}" &> /dev/null || {
        error "EOS MGM pod '${EOS_MGM_POD}' not found in ${CTA_NAMESPACE} namespace"
    }
    
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" get pod "${NOOBAA_POD}" &> /dev/null || {
        error "NooBaa pod '${NOOBAA_POD}' not found in ${NOOBAA_NAMESPACE} namespace"
    }
    
    log "Prerequisites check passed for single-cluster setup"
}

setup_certificates() {
    log "Setting up certificates for inter-namespace communication..."
    
    # Create temporary directory for certificates
    local temp_cert_dir="/tmp/certificates-$(date +%s)"
    mkdir -p "${temp_cert_dir}"
    
    # Copy certificates from EOS MGM pod (CTA namespace) to local temp directory
    log "Copying certificates from EOS MGM pod: ${EOS_MGM_POD} (${CTA_CLUSTER_CONTEXT}/${CTA_NAMESPACE})"
    kubectl --context="${CTA_CLUSTER_CONTEXT}" -n "${CTA_NAMESPACE}" cp \
        "${EOS_MGM_POD}:${EOS_CERT_PATH}/" "${temp_cert_dir}/" -c eos-mgm || {
        error "Failed to copy certificates from EOS MGM pod in CTA namespace"
    }
    
    # Create certificate directory in NooBaa pod first
    log "Creating certificate directory in NooBaa pod: ${NOOBAA_POD}"
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" exec "${NOOBAA_POD}" -c core -- \
        mkdir -p "${NOOBAA_CERT_PATH}" || {
        error "Failed to create certificate directory in NooBaa pod"
    }
    
    # Copy certificates from temp directory to NooBaa pod (NooBaa namespace)
    log "Copying certificates to NooBaa pod: ${NOOBAA_POD} (${NOOBAA_CLUSTER_CONTEXT}/${NOOBAA_NAMESPACE})"
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" cp \
        "${temp_cert_dir}/." "${NOOBAA_POD}:${NOOBAA_CERT_PATH}/" -c core || {
        error "Failed to copy certificates to NooBaa pod in NooBaa namespace"
    }
    
    # Cleanup temporary directory
    rm -rf "${temp_cert_dir}"
    
    log "Inter-namespace certificate setup completed successfully"
}

verify_certificates() {
    log "Verifying certificate installation in NooBaa namespace..."
    
    # Check if certificates are accessible in NooBaa pod
    local cert_count
    cert_count=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" exec "${NOOBAA_POD}" -c core -- \
        ls -1 "${NOOBAA_CERT_PATH}" 2>/dev/null | wc -l || echo "0")
    
    if [ "${cert_count}" -eq 0 ]; then
        error "No certificates found in NooBaa pod at ${NOOBAA_CERT_PATH}"
    fi
    
    log "Certificate verification passed. Found ${cert_count} certificate files in NooBaa namespace."
    
    # Test connectivity to CTA namespace (if cluster IPs are available)
    if [[ -n "${CTA_CLUSTER_IP:-}" ]]; then
        log "Testing connectivity from NooBaa to CTA services..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NOOBAA_NAMESPACE}" exec "${NOOBAA_POD}" -c core -- \
            curl -k --connect-timeout 10 "https://${CTA_CLUSTER_IP}:8443/.well-known/wlcg-tape-rest-api" &>/dev/null && {
            log "✓ NooBaa can reach EOS HTTP TAPE REST API"
        } || {
            log "⚠ NooBaa cannot reach EOS HTTP TAPE REST API (this may be expected if EOS is not yet running)"
        }
    fi
}

main() {
    log "Starting certificate setup for NooBaa-CTA integration"
    
    check_prerequisites
    setup_certificates
    verify_certificates
    
    log "Certificate setup completed successfully"
    log "NooBaa pod can now authenticate with EOS using X509 certificates"
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo ""
        echo "Setup X509 certificates for NooBaa pod to communicate with EOS"
        echo ""
        echo "Environment variables:"
        echo "  NAMESPACE    - Kubernetes namespace (default: default)"
        echo "  EOS_MGM_POD  - EOS MGM pod name (default: ctaeos)"
        echo "  NOOBAA_POD   - NooBaa pod name (default: noobaa-core)"
        echo ""
        echo "Options:"
        echo "  -h, --help   - Show this help message"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown argument: $1. Use --help for usage information."
        ;;
esac
