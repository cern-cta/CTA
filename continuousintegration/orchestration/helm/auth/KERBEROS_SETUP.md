# Kerberos Integration with Keycloak in CTA CI

This document describes the Kerberos integration setup for Keycloak in the CTA CI environment.

## Overview

The CTA CI now supports Kerberos authentication through Keycloak user federation. This setup includes:

1. **KDC (Kerberos Distribution Center)** - Running as a separate pod
2. **Keycloak with Kerberos User Federation** - Configured to authenticate users against the KDC
3. **Test Users** - Pre-configured Kerberos principals for testing

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Client      │───▶│    Keycloak     │───▶│       KDC       │
│                 │    │  (User Fed)     │    │   (TEST.CTA)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Configuration Details

### Kerberos Realm
- **Realm Name**: `TEST.CTA`
- **KDC Host**: `auth-kdc.{namespace}.svc.cluster.local:88`
- **Admin Server**: `auth-kdc.{namespace}.svc.cluster.local:749`

### Test Users Created
The setup automatically creates the following test Kerberos principals:

1. **testuser1** - Password: `testpassword1`
2. **testuser2** - Password: `testpassword2`
3. **ctauser** - Password: `ctapassword`

### Keycloak Configuration

The Keycloak User Federation provider is configured with:

- **Provider Type**: Kerberos
- **Kerberos Realm**: `TEST.CTA`
- **Server Principal**: `HTTP/auth-keycloak.{namespace}.svc.cluster.local@TEST.CTA`
- **Keytab Location**: `/etc/keycloak.keytab`
- **Allow Kerberos Authentication**: Enabled
- **Update Profile First Login**: Enabled

## Deployment Process

The Kerberos integration is deployed through several Helm hooks:

1. **KDC Pod** - Creates the Kerberos database and starts KDC services
2. **Keytab Setup Job** - Extracts the keytab from KDC and creates a Kubernetes secret
3. **Keycloak Configuration Job** - Sets up the Kerberos user federation in Keycloak

## Files Modified/Added

### New Files:
- `templates/keycloak-keytab-setup-job.yaml` - Job to setup Keycloak keytab
- `scripts/setup_kerberos_keytab.sh` - Script for manual keytab setup
- `KERBEROS_SETUP.md` - This documentation

### Modified Files:
- `templates/keycloak-pod.yaml` - Added krb5.conf and keytab mounts
- `templates/keycloak-configure-job.yaml` - Added Kerberos user federation setup
- `templates/secrets-service-account.yaml` - Added permissions for pods and kubectl
- `scripts/kdc.sh` - Added test users and keytab generation

## Testing the Integration

### Prerequisites
1. Deploy the auth chart: `helm install auth ./helm/auth`
2. Wait for all pods to be running
3. Ensure the Keycloak configuration job completes successfully

### Testing Authentication

1. **Access Keycloak Admin Console**:
   ```bash
   kubectl port-forward svc/auth-keycloak 8080:8080
   ```
   Navigate to: http://localhost:8080

2. **Test Kerberos User Federation**:
   - Go to Admin Console → User Federation
   - Verify the Kerberos provider is listed and enabled
   - Test synchronization of users from Kerberos

3. **Test User Authentication**:
   - Try logging in with one of the test users (e.g., testuser1/testpassword1)
   - Verify the user is authenticated via Kerberos

## Troubleshooting

### Common Issues:

1. **Keytab not found**:
   - Check if the keycloak-keytab secret exists: `kubectl get secret keycloak-keytab`
   - Verify the keytab setup job completed successfully

2. **Kerberos authentication fails**:
   - Check KDC logs: `kubectl logs auth-kdc`
   - Verify krb5.conf is correctly mounted in Keycloak pod
   - Check Keycloak logs for Kerberos-related errors

3. **User federation sync issues**:
   - Verify the Kerberos realm configuration in Keycloak
   - Check if the service principal is correctly configured
   - Test connectivity between Keycloak and KDC

### Logs to Check:
```bash
# KDC logs
kubectl logs auth-kdc

# Keycloak logs
kubectl logs auth-keycloak

# Setup job logs
kubectl logs job/auth-setup-keycloak-keytab-job
kubectl logs job/auth-configure-keycloak-job
```

## Security Considerations

- The setup uses test passwords for demo purposes
- In production, use proper password policies and secure keytab management
- Consider implementing proper Kerberos ticket lifetime policies
- Regularly rotate service principals and keytabs