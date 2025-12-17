#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Test script for cta-admin-grpc authentication methods
# Tests JWT and Kerberos authentication with config file and environment variable overrides

set -e

# Configuration
TEST_CONFIG_FILE="/tmp/cta-cli-grpc-test.conf"
LOG_FILE="/tmp/cta-admin-grpc-auth-test.log"
TEST_PASSED=0
TEST_FAILED=0


log_header() {
    echo "" | tee -a "${LOG_FILE}"
    echo "========================================" | tee -a "${LOG_FILE}"
    echo "$1" | tee -a "${LOG_FILE}"
    echo "========================================" | tee -a "${LOG_FILE}"
}

test_version_command() {
    local test_name="$1"
    local expected_result="$2"  # "success" or "failure"
    local result="success"

    if ! cta-admin-grpc --config ${TEST_CONFIG_FILE} version; then
        result="failure"
    fi

    if [ $result == $expected_result ]; then
        echo "Test $test_name succeeded!"
        return 0
    else
        echo "Test $test_name failed!"
        return 1
    fi
}

create_config_file() {
    local auth_method=$1
    cat > "${TEST_CONFIG_FILE}" <<EOF
# Test configuration file for cta-admin-grpc
cta.endpoint cta-frontend-grpc:10956
grpc.tls.chain_cert_path /etc/grpc-certs/ca.crt
grpc.tls.enabled true
grpc.cta_admin_auth_method $auth_method
grpc.jwt_token_path /etc/grid-security/jwt-token-grpc
EOF
}

cleanup() {
    rm -f "${TEST_CONFIG_FILE}"
    unset CTA_ADMIN_GRPC_AUTH_METHOD
}

# Ensure cleanup on exit
trap cleanup EXIT

# ============================================
# Test 1: JWT authentication in config file
# ============================================
log_header "Test 1: JWT authentication specified in config file"
create_config_file "jwt"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
unset CTA_ADMIN_GRPC_AUTH_METHOD
test_version_command "JWT auth via config file" "success"

# ============================================
# Test 2: Kerberos authentication in config file
# ============================================
log_header "Test 2: Kerberos authentication specified in config file"
create_config_file "krb5"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
unset CTA_ADMIN_GRPC_AUTH_METHOD
test_version_command "Kerberos auth via config file" "success"

# ============================================
# Test 3: Override config with JWT env variable
# ============================================
log_header "Test 3: Override Kerberos config with JWT environment variable"
create_config_file "krb5"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
export CTA_ADMIN_GRPC_AUTH_METHOD="jwt"
test_version_command "JWT auth via env variable (overriding krb5 in config)" "success"

# ============================================
# Test 4: Override config with Kerberos env variable
# ============================================
log_header "Test 4: Override JWT config with Kerberos environment variable"
create_config_file "jwt"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
export CTA_ADMIN_GRPC_AUTH_METHOD="krb5"
test_version_command "Kerberos auth via env variable (overriding jwt in config)" "success"

# ============================================
# Test 5: No auth method in config, use JWT via env
# ============================================
log_header "Test 5: No auth method in config, specify JWT via environment variable"
create_config_file ""
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
export CTA_ADMIN_GRPC_AUTH_METHOD="jwt"
test_version_command "JWT auth via env variable (no config setting)" "success"

# ============================================
# Test 6: No auth method in config, use Kerberos via env
# ============================================
log_header "Test 6: No auth method in config, specify Kerberos via environment variable"
create_config_file ""
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
export CTA_ADMIN_GRPC_AUTH_METHOD="krb5"
test_version_command "Kerberos auth via env variable (no config setting)" "success"

# ============================================
# Test 7: No auth method specified anywhere (default behavior)
# ============================================
log_header "Test 7: No auth method specified (should use default: krb5)"
create_config_file ""
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
unset CTA_ADMIN_GRPC_AUTH_METHOD
test_version_command "Default auth method (should be krb5)" "success"

# ============================================
# Test 8: Invalid auth method in config
# ============================================
log_header "Test 8: Invalid auth method in config file"
create_config_file "invalid_method"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
unset CTA_ADMIN_GRPC_AUTH_METHOD
test_version_command "Invalid auth method in config" "failure"

# ============================================
# Test 9: Invalid auth method via environment variable
# ============================================
log_header "Test 9: Invalid auth method via environment variable"
create_config_file "jwt"
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
export CTA_ADMIN_GRPC_AUTH_METHOD="invalid_env_method"
test_version_command "Invalid auth method via env variable" "failure"

# ============================================
# Test 10: Empty auth method in config
# ============================================
log_header "Test 10: Empty auth method in config file"
cat > "${TEST_CONFIG_FILE}" <<EOF
cta.endpoint cta-frontend-grpc:10956
grpc.tls.chain_cert_path /etc/grpc-certs/ca.crt
grpc.tls.enabled true
grpc.cta_admin_auth_method
EOF
export CTA_CLI_GRPC_CONFIG="${TEST_CONFIG_FILE}"
unset CTA_ADMIN_GRPC_AUTH_METHOD
test_version_command "Empty auth method in config" "success" # fallback to kerberos

echo "All tests passed!"
