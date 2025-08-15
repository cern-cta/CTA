#!/bin/bash

set -e

# Install dependencies
echo "Installing required packages..."
command -v jq >/dev/null 2>&1 || { echo >&2 "Installing jq..."; sudo dnf install -y jq; }

# Download latest NooBaa version
echo "Fetching latest NooBaa version..."
VERSION=$(curl -s https://api.github.com/repos/noobaa/noobaa-operator/releases/latest | jq -r '.name')

# Download and install NooBaa CLI
echo "Downloading NooBaa CLI..."
curl -LO https://github.com/noobaa/noobaa-operator/releases/download/$VERSION/noobaa-operator-$VERSION-linux-amd64.tar.gz
tar -xvzf noobaa-operator-$VERSION-linux-amd64.tar.gz
chmod +x noobaa-operator
# Use local executable instead of system installation
export PATH="$PWD:$PATH"
rm noobaa-operator-$VERSION-linux-amd64.tar.gz

# Install NooBaa operator in default namespace
echo "Installing NooBaa operator..."
./noobaa-operator install --namespace='noobaa' --noobaa-image='registry.cern.ch/docker.io/noobaa/noobaa-core:5.19.0' --operator-image='registry.cern.ch/docker.io/noobaa/noobaa-operator:5.19.0'

echo "Giving 30 secs to noobaa to setup the backingstore..."
sleep 30

# Show system status
echo "NooBaa status:"
./noobaa-operator status --namespace=noobaa

kubectl -n noobaa get pods
