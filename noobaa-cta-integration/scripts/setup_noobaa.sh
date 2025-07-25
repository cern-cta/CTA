#!/bin/bash

# @project      The CERN Tape Archive (CTA) - NooBaa Integration
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".

# Setup NooBaa for CTA integration (archival functionality only)
# Configures authentication, installs migrate executable, and sets up NSFS Glacier

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_ROOT="$(dirname "${SCRIPT_DIR}")"
NAMESPACE="${NAMESPACE:-noobaa}"
NOOBAA_CLUSTER_CONTEXT="${NOOBAA_CLUSTER_CONTEXT:-minikube}"
CTA_CLUSTER_CONTEXT="${CTA_CLUSTER_CONTEXT:-minikube}"
NOOBAA_POD="${NOOBAA_POD:-}"
NOOBAA_CONTAINER="${NOOBAA_CONTAINER:-core}"
EXECUTABLES_DIR="${INTEGRATION_ROOT}/executables"
CONFIG_DIR="${INTEGRATION_ROOT}/config"

# Load single-cluster configuration if available
if [[ -f "/tmp/cluster_config.env" ]]; then
    source /tmp/cluster_config.env
fi

# Paths inside NooBaa container (use user-writable directories)
NOOBAA_BIN_DIR="/tmp/glacier/bin"
NOOBAA_CONFIG_DIR="/tmp/glacier/config"
NOOBAA_LOG_DIR="/tmp/glacier/logs"

# Functions
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [NOOBAA-SETUP] $*"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [NOOBAA-SETUP] ERROR: $*" >&2
    exit 1
}

check_prerequisites() {
    log "Checking prerequisites for single-cluster setup..."
    
    # Check kubectl is available
    if ! command -v kubectl &> /dev/null; then
        error "kubectl command not found. Please install kubectl."
    fi
    
    # Validate cluster context
    kubectl config get-contexts "${NOOBAA_CLUSTER_CONTEXT}" &>/dev/null || {
        error "Cluster context '${NOOBAA_CLUSTER_CONTEXT}' not found"
    }
    
    # Auto-discover NooBaa pod if not set
    if [[ -z "${NOOBAA_POD}" ]]; then
        NOOBAA_POD=$(kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" get pods -n "${NAMESPACE}" \
            -o name | grep "noobaa-core" | head -1 | sed 's|pod/||' || echo "")
        [[ -n "${NOOBAA_POD}" ]] || error "NooBaa core pod not found in ${NAMESPACE} namespace"
        log "Auto-discovered NooBaa pod: ${NOOBAA_POD}"
    fi
    
    # Check if NooBaa pod exists
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" get pod "${NOOBAA_POD}" &> /dev/null || {
        error "NooBaa pod '${NOOBAA_POD}' not found in ${NAMESPACE} namespace"
    }
    
    # Check if executables directory exists
    if [[ ! -d "${EXECUTABLES_DIR}" ]]; then
        error "Executables directory not found: ${EXECUTABLES_DIR}"
    fi
    
    log "Prerequisites check passed"
}

check_dependencies() {
    log "Checking available dependencies in NooBaa pod..."
    
    # Check what's already available
    local required_commands=("jq" "bc" "curl")
    local missing_commands=()
    
    for cmd in "${required_commands[@]}"; do
        if ! kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            which "${cmd}" &>/dev/null; then
            missing_commands+=("${cmd}")
        else
            log "✓ ${cmd} is available"
        fi
    done
    
    # Check for EOS client tools (these might not be available without root)
    if ! kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        which "eos" &>/dev/null; then
        log "⚠ EOS client not found - will use curl-based approach"
    else
        log "✓ EOS client is available"
    fi
    
    if [[ ${#missing_commands[@]} -gt 0 ]]; then
        log "⚠ Missing commands: ${missing_commands[*]}"
        log "⚠ Will attempt to continue without package installation"
        log "⚠ Some features may be limited"
    else
        log "✓ All basic dependencies are available"
    fi
}

install_dependencies() {
    log "Checking if we can install missing dependencies..."
    
    # Try to check if we have root privileges
    if kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        bash -c "whoami" 2>/dev/null | grep -q "root"; then
        log "Root privileges detected, attempting package installation..."
        
        # Install required packages
        local packages="jq bc curl"
        
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            bash -c "
                # Update package list
                if command -v yum &> /dev/null; then
                    yum update -y
                    yum install -y ${packages}
                elif command -v apt-get &> /dev/null; then
                    apt-get update
                    apt-get install -y ${packages}
                else
                    echo 'WARNING: No supported package manager found'
                    exit 0
                fi
            " || {
            log "⚠ Package installation failed, continuing without installation"
        }
    else
        log "⚠ No root privileges - skipping package installation"
        log "⚠ Assuming required tools are already available"
    fi
    
    log "Dependency check completed"
}

create_directories() {
    log "Creating required directories in NooBaa pod..."
    
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        bash -c "
            mkdir -p ${NOOBAA_BIN_DIR}
            mkdir -p ${NOOBAA_CONFIG_DIR}
            mkdir -p ${NOOBAA_LOG_DIR}
            chmod 755 ${NOOBAA_BIN_DIR}
            chmod 755 ${NOOBAA_CONFIG_DIR}
            chmod 755 ${NOOBAA_LOG_DIR}
        " || {
        error "Failed to create directories in NooBaa pod"
    }
    
    log "Directories created successfully"
}

install_executables() {
    log "Installing CTA integration executables..."
    
    # List of executables to install (archival only)
    local executables=("migrate")
    
    for executable in "${executables[@]}"; do
        local local_path="${EXECUTABLES_DIR}/${executable}"
        local remote_path="${NOOBAA_BIN_DIR}/${executable}"
        
        if [[ ! -f "${local_path}" ]]; then
            error "Executable not found: ${local_path}"
        fi
        
        log "Installing executable: ${executable}"
        
        # Copy executable to NooBaa pod
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" cp "${local_path}" "${NOOBAA_POD}:${remote_path}" -c "${NOOBAA_CONTAINER}" || {
            error "Failed to copy executable: ${executable}"
        }
        
        # Make it executable
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            chmod +x "${remote_path}" || {
            error "Failed to set executable permissions: ${executable}"
        }
    done
    
    log "Executables installed successfully"
}

create_noobaa_config() {
    log "Creating NooBaa configuration for Glacier..."
    
    # Use Kubernetes DNS name for EOS MGM host (works across cluster restarts)
    local eos_mgm_host="eos-mgm-0.eos-mgm.dev.svc.cluster.local"
    
    log "Using EOS MGM hostname: ${eos_mgm_host}"
    
    # Create NooBaa configuration file with Glacier settings
    local config_content
    read -r -d '' config_content << EOF || true
{
    "NSFS_GLACIER_TAPECLOUD_BIN_DIR": "${NOOBAA_BIN_DIR}",
    "NSFS_GLACIER_BACKEND": "TAPECLOUD",
    "NSFS_GLACIER_ENABLED": true,
    "EOS_MGM_HOST": "${eos_mgm_host}",
    "EOS_PORT": "8443",
    "CERT_PATH": "/tmp/grid-security/certificates",
    "TOKEN_CACHE_FILE": "/tmp/eos_power_token",
    "TOKEN_VALIDITY_HOURS": 23,
    "CURL_TIMEOUT": 300,
    "LOG_LEVEL": "INFO",
    "NSFS_ROOT": "/data/nsfs",
    "LOW_SPACE_THRESHOLD_PERCENT": 10,
    "EXPIRED_MARKER_AGE_DAYS": 30,
    "CROSS_CLUSTER_SETUP": true,
    "CTA_CLUSTER_IP": "${CTA_CLUSTER_IP:-}",
    "NOOBAA_CLUSTER_CONTEXT": "${NOOBAA_CLUSTER_CONTEXT}",
    "CTA_CLUSTER_CONTEXT": "${CTA_CLUSTER_CONTEXT}"
}
EOF
    
    # Write config to temporary file
    local temp_config="/tmp/noobaa_glacier_config.json"
    echo "${config_content}" > "${temp_config}"
    
    # Copy config to NooBaa pod
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" cp "${temp_config}" "${NOOBAA_POD}:${NOOBAA_CONFIG_DIR}/glacier_config.json" -c "${NOOBAA_CONTAINER}" || {
        error "Failed to copy configuration file"
    }
    
    # Cleanup
    rm -f "${temp_config}"
    
    log "NooBaa configuration created successfully"
}

setup_environment() {
    log "Setting up environment variables in NooBaa pod..."
    
    # Create environment setup script
    local env_script_content
    env_script_content=$(cat << EOF
#!/bin/bash
# NooBaa CTA Integration Environment Setup

# Source the glacier configuration
if [[ -f ${NOOBAA_CONFIG_DIR}/glacier_config.json ]]; then
    if command -v jq &>/dev/null; then
        export EOS_MGM_HOST=\$(jq -r '.EOS_MGM_HOST // "ctaeos"' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "ctaeos")
        export CERT_PATH=\$(jq -r '.CERT_PATH // "/tmp/grid-security/certificates"' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "/tmp/grid-security/certificates")
        export TOKEN_CACHE_FILE=\$(jq -r '.TOKEN_CACHE_FILE // "/tmp/eos_power_token"' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "/tmp/eos_power_token")
        export TOKEN_VALIDITY_HOURS=\$(jq -r '.TOKEN_VALIDITY_HOURS // 23' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "23")
        export CURL_TIMEOUT=\$(jq -r '.CURL_TIMEOUT // 300' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "300")
        export NSFS_ROOT=\$(jq -r '.NSFS_ROOT // "/data/nsfs"' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "/data/nsfs")
        export LOW_SPACE_THRESHOLD_PERCENT=\$(jq -r '.LOW_SPACE_THRESHOLD_PERCENT // 10' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "10")
        export EXPIRED_MARKER_AGE_DAYS=\$(jq -r '.EXPIRED_MARKER_AGE_DAYS // 30' ${NOOBAA_CONFIG_DIR}/glacier_config.json 2>/dev/null || echo "30")
    else
        # Fallback if jq is not available
        export EOS_MGM_HOST="ctaeos"
        export CERT_PATH="/tmp/grid-security/certificates"
        export TOKEN_CACHE_FILE="/tmp/eos_power_token"
        export TOKEN_VALIDITY_HOURS="23"
        export CURL_TIMEOUT="300"
        export NSFS_ROOT="/data/nsfs"
        export LOW_SPACE_THRESHOLD_PERCENT="10"
        export EXPIRED_MARKER_AGE_DAYS="30"
    fi
fi

# Add glacier executables to PATH
export PATH="${NOOBAA_BIN_DIR}:\${PATH}"

# Setup logging
export LOG_FILE_PREFIX="${NOOBAA_LOG_DIR}"
EOF
)
    
    # Write environment script to temporary file
    local temp_env_script="/tmp/glacier_env.sh"
    echo "${env_script_content}" > "${temp_env_script}"
    
    # Copy to NooBaa pod
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" cp "${temp_env_script}" "${NOOBAA_POD}:${NOOBAA_CONFIG_DIR}/glacier_env.sh" -c "${NOOBAA_CONTAINER}" || {
        error "Failed to copy environment script"
    }
    
    # Make it executable
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        chmod +x "${NOOBAA_CONFIG_DIR}/glacier_env.sh" || {
        error "Failed to set executable permissions on environment script"
    }
    
    # Add to shell profile
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        bash -c "echo 'source ${NOOBAA_CONFIG_DIR}/glacier_env.sh' >> /root/.bashrc" || {
        log "Warning: Could not add environment script to .bashrc"
    }
    
    # Cleanup
    rm -f "${temp_env_script}"
    
    log "Environment setup completed successfully"
}

verify_installation() {
    log "Verifying installation..."
    
    # Check if executables are accessible (archival only)
    local executables=("migrate")
    
    for executable in "${executables[@]}"; do
        if kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            ls "${NOOBAA_BIN_DIR}/${executable}" &>/dev/null; then
            log "✓ ${executable} installed successfully"
        else
            error "Executable not found: ${executable}"
        fi
    done
    
    # Check if required commands are available (warn instead of error)
    local required_commands=("jq" "bc" "curl")
    local optional_commands=("eos" "eospower_eos")
    
    for cmd in "${required_commands[@]}"; do
        if ! kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            which "${cmd}" &>/dev/null; then
            log "⚠ Warning: Required command not found: ${cmd}"
        else
            log "✓ ${cmd} found"
        fi
    done
    
    for cmd in "${optional_commands[@]}"; do
        if ! kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
            which "${cmd}" &>/dev/null; then
            log "⚠ Warning: Optional command not found: ${cmd} (will use curl-based approach)"
        else
            log "✓ ${cmd} found"
        fi
    done
    
    # Test configuration loading
    if kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        ls "${NOOBAA_CONFIG_DIR}/glacier_config.json" &>/dev/null; then
        log "✓ Configuration file created successfully"
    else
        log "⚠ Warning: Configuration file not found"
    fi
    
    if kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" exec "${NOOBAA_POD}" -c "${NOOBAA_CONTAINER}" -- \
        ls "${NOOBAA_CONFIG_DIR}/glacier_env.sh" &>/dev/null; then
        log "✓ Environment script created successfully"
    else
        log "⚠ Warning: Environment script not found"
    fi
    
    log "Installation verification passed"
}

apply_nsfs_configuration() {
    log "Applying NSFS configuration for Glacier functionality..."
    
    # Apply NSFS backing store and bucket class configuration
    if [[ -f "${CONFIG_DIR}/glacier-bucket-class.yaml" ]]; then
        log "Applying NSFS backing store and bucket class..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" apply -f "${CONFIG_DIR}/glacier-bucket-class.yaml" || {
            error "Failed to apply NSFS backing store configuration"
        }
    else
        error "NSFS backing store configuration not found: ${CONFIG_DIR}/glacier-bucket-class.yaml"
    fi
    
    # Apply NSFS Glacier configuration
    if [[ -f "${CONFIG_DIR}/nsfs-glacier-config.yaml" ]]; then
        log "Applying NSFS Glacier configuration..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" apply -f "${CONFIG_DIR}/nsfs-glacier-config.yaml" || {
            error "Failed to apply NSFS Glacier configuration"
        }
    else
        error "NSFS Glacier configuration not found: ${CONFIG_DIR}/nsfs-glacier-config.yaml"
    fi
    
    # Wait for backing store to be ready
    log "Waiting for NSFS backing store to be ready..."
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" wait --for=condition=Available \
        backingstore/glacier-nsfs-backing-store --timeout=300s || {
        log "Warning: NSFS backing store may not be ready yet"
    }
    
    # Wait for bucket class to be ready
    log "Waiting for bucket class to be ready..."
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" wait --for=condition=Available \
        bucketclass/glacier-bucket-class --timeout=300s || {
        log "Warning: Bucket class may not be ready yet"
    }
    
    log "NSFS configuration applied successfully"
}

configure_noobaa_glacier_system() {
    log "Configuring NooBaa system for Glacier support..."
    
    # Apply the NooBaa configuration override
    if [[ -f "${CONFIG_DIR}/nsfs-glacier-config.yaml" ]]; then
        log "Applying NooBaa Glacier system configuration..."
        kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" apply -f "${CONFIG_DIR}/nsfs-glacier-config.yaml" || {
            error "Failed to apply NooBaa Glacier system configuration"
        }
    else
        error "NooBaa Glacier system configuration not found: ${CONFIG_DIR}/nsfs-glacier-config.yaml"
    fi
    
    # Patch the NooBaa core StatefulSet to mount the config override
    log "Patching NooBaa core StatefulSet to mount Glacier configuration..."
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" patch statefulset noobaa-core --type='merge' -p='
{
  "spec": {
    "template": {
      "spec": {
        "containers": [
          {
            "name": "noobaa-core",
            "env": [
              {
                "name": "NSFS_GLACIER_ENABLED",
                "value": "true"
              },
              {
                "name": "NSFS_GLACIER_LOGS_ENABLED", 
                "value": "true"
              },
              {
                "name": "NSFS_GLACIER_BACKEND",
                "value": "TAPECLOUD"
              },
              {
                "name": "NSFS_GLACIER_TAPECLOUD_BIN_DIR",
                "value": "/tmp/glacier/bin"
              },
              {
                "name": "NSFS_GLACIER_LOGS_DIR",
                "value": "/tmp/glacier/logs"
              }
            ]
          }
        ]
      }
    }
  }
}' || {
        log "Warning: Failed to patch NooBaa core StatefulSet for Glacier configuration"
    }
    
    # Wait for the StatefulSet to be ready after patching
    log "Waiting for NooBaa core to restart with Glacier configuration..."
    kubectl --context="${NOOBAA_CLUSTER_CONTEXT}" -n "${NAMESPACE}" rollout status statefulset/noobaa-core --timeout=300s || {
        log "Warning: NooBaa core restart may not have completed"
    }
    
    log "NooBaa system configured for Glacier support"
}

main() {
    log "Starting NooBaa setup for CTA integration"
    
    check_prerequisites
    check_dependencies
    install_dependencies
    create_directories
    install_executables
    create_noobaa_config
    setup_environment
    apply_nsfs_configuration
    configure_noobaa_glacier_system
    verify_installation
    
    log "NooBaa archival setup completed successfully"
    log ""
    log "Next steps:"
    log "1. Configure token authentication for EOS file uploads"
    log "2. Test file archival with migrate script"
    log "3. Test S3 Glacier workflow with NSFS backing store"
    log ""
    log "Configuration location in NooBaa pod: ${NOOBAA_CONFIG_DIR}"
    log "Migrate executable location in NooBaa pod: ${NOOBAA_BIN_DIR}/migrate"
    log "Log directory in NooBaa pod: ${NOOBAA_LOG_DIR}"
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo ""
        echo "Setup NooBaa for CTA integration (archival functionality only)"
        echo ""
        echo "Environment variables:"
        echo "  NAMESPACE       - Kubernetes namespace (default: default)"
        echo "  NOOBAA_POD      - NooBaa pod name (default: noobaa-core)"
        echo "  NOOBAA_CONTAINER - NooBaa container name (default: noobaa-core)"
        echo ""
        echo "Options:"
        echo "  -h, --help      - Show this help message"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown argument: $1. Use --help for usage information."
        ;;
esac