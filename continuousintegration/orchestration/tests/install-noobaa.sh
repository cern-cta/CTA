#!/bin/bash

set -e

# Detect OS and ARCH
OS="linux" # Change to "darwin" for macOS
ARCH="amd64" # Change to "arm64" if needed

# Cluster configuration
NOOBAA_CLUSTER_PROFILE="minikube"
NOOBAA_NAMESPACE="noobaa"

# Install dependencies
echo "Installing required packages..."
command -v jq >/dev/null 2>&1 || { echo >&2 "Installing jq..."; sudo dnf install -y jq; }

# Download latest NooBaa version
echo "Fetching latest NooBaa version..."
VERSION=$(curl -s https://api.github.com/repos/noobaa/noobaa-operator/releases/latest | jq -r '.name')

# Download and install NooBaa CLI
echo "Downloading NooBaa CLI..."
curl -LO https://github.com/noobaa/noobaa-operator/releases/download/$VERSION/noobaa-operator-$VERSION-$OS-$ARCH.tar.gz
tar -xvzf noobaa-operator-$VERSION-$OS-$ARCH.tar.gz
chmod +x noobaa-operator
# Use local executable instead of system installation
export PATH="$PWD:$PATH"
rm noobaa-operator-$VERSION-$OS-$ARCH.tar.gz

# Use existing CTA minikube cluster for NooBaa installation
echo "Using existing CTA minikube cluster for NooBaa..."
echo "Cluster profile: $NOOBAA_CLUSTER_PROFILE"

# Check if cluster is running - exit with warning if not
echo "Checking if cluster $NOOBAA_CLUSTER_PROFILE is running..."
if ! minikube status --profile="$NOOBAA_CLUSTER_PROFILE" >/dev/null 2>&1; then
    echo ""
    echo "⚠️  WARNING: Minikube cluster '$NOOBAA_CLUSTER_PROFILE' is not running!"
    echo ""
    echo "Please start your CTA cluster first by running:"
    echo "  minikube start --profile=$NOOBAA_CLUSTER_PROFILE"
    echo ""
    echo "Then run this script again."
    echo ""
    exit 1
fi

cluster_status=$(minikube status --profile="$NOOBAA_CLUSTER_PROFILE" 2>/dev/null)
if [[ "$cluster_status" != *"Running"* ]]; then
    echo ""
    echo "⚠️  WARNING: Minikube cluster '$NOOBAA_CLUSTER_PROFILE' is not in Running state!"
    echo ""
    echo "Current status:"
    echo "$cluster_status"
    echo ""
    echo "Please ensure your CTA cluster is fully running, then try again."
    echo ""
    exit 1
fi

echo "✅ Cluster $NOOBAA_CLUSTER_PROFILE is running properly"

# Switch to the CTA cluster context
echo "Switching to CTA cluster context..."
kubectl config use-context "$NOOBAA_CLUSTER_PROFILE"

# Create NooBaa namespace
echo "Creating NooBaa namespace: $NOOBAA_NAMESPACE"
kubectl create namespace "$NOOBAA_NAMESPACE" || echo "Namespace '$NOOBAA_NAMESPACE' may already exist"

# Set up NooBaa in default namespace of dedicated cluster
echo "Setting up NooBaa in cluster: $NOOBAA_CLUSTER_PROFILE"
echo "Namespace: $NOOBAA_NAMESPACE"

# Verify we're in the correct cluster context
echo "Current context: $(kubectl config current-context)"

# Install NooBaa operator with CERN registry images
echo "Installing NooBaa operator with CERN pull-through cache images..."
./noobaa-operator install --namespace=$NOOBAA_NAMESPACE --noobaa-image='registry.cern.ch/docker.io/noobaa/noobaa-core:5.19.0' --operator-image='registry.cern.ch/docker.io/noobaa/noobaa-operator:5.19.0'

# Wait for pods to restart with new images
echo "Waiting for NooBaa pods to be ready with correct images..."
kubectl wait --for=condition=ready pod -l app=noobaa-operator -n $NOOBAA_NAMESPACE --timeout=300s || echo "Warning: noobaa-operator pod may not be ready"
kubectl wait --for=condition=ready pod -l app=noobaa-core -n $NOOBAA_NAMESPACE --timeout=300s || echo "Warning: noobaa-core pod may not be ready"

# Show system status
echo "NooBaa status:"
./noobaa-operator status --namespace=$NOOBAA_NAMESPACE

# Show cluster information
echo ""
echo "Cluster Information:"
echo "Profile: $NOOBAA_CLUSTER_PROFILE"
echo "Context: $(kubectl config current-context)"
echo "Cluster IP: $(minikube ip --profile=$NOOBAA_CLUSTER_PROFILE)"
echo "Namespace: $NOOBAA_NAMESPACE"

# Show how to access NooBaa
echo ""
echo "To access NooBaa in the future:"
echo "1. Switch context: kubectl config use-context $NOOBAA_CLUSTER_PROFILE"
echo "2. Check status: ./noobaa-operator status --namespace=$NOOBAA_NAMESPACE"
echo "3. Get cluster IP: minikube ip --profile=$NOOBAA_CLUSTER_PROFILE"
