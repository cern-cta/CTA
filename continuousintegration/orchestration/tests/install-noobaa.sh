#!/bin/bash

set -e

# Detect OS and ARCH
OS="linux" # Change to "darwin" for macOS
ARCH="amd64" # Change to "arm64" if needed

# Install dependencies
echo "Installing required packages..."
command -v jq >/dev/null 2>&1 || { echo >&2 "Installing jq..."; sudo apt-get install -y jq; }

# Download latest NooBaa version
echo "Fetching latest NooBaa version..."
VERSION=$(curl -s https://api.github.com/repos/noobaa/noobaa-operator/releases/latest | jq -r '.name')

# Download and install NooBaa CLI
echo "Downloading NooBaa CLI..."
curl -LO https://github.com/noobaa/noobaa-operator/releases/download/$VERSION/noobaa-operator-$VERSION-$OS-$ARCH.tar.gz
tar -xvzf noobaa-operator-$VERSION-$OS-$ARCH.tar.gz
chmod +x noobaa-operator
sudo mv noobaa-operator /usr/local/bin/noobaa
rm noobaa-operator-$VERSION-$OS-$ARCH.tar.gz

# Download and install Minikube
echo "Installing Minikube..."
curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64

# Start Minikube
echo "Starting Minikube..."
minikube start

# Set up NooBaa on Kubernetes
echo "Creating 'noobaa' namespace..."
kubectl create ns noobaa || echo "Namespace 'noobaa' already exists"
kubectl config set-context --current --namespace=noobaa

# Install NooBaa operator
echo "Installing NooBaa..."
noobaa install

# Show system status
echo "NooBaa status:"
noobaa status
