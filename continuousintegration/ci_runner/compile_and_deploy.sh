#!/bin/bash

# Help message
usage() {
    echo "Usage: $0 [-r] [-s <source_directory>] [-d]"
    echo ""
    echo "Flags:"
    echo "  -r, --reset         Shut down the compilation pod and start a new one to ensure a fresh build."
    echo "  -s, --source-dir    Specify the location of the CTA project directory."
    echo "  -d, --redeploy      Redeploy the existing Minikube configuration with the newly compiled RPMs."
    exit 1
}

# Default values
RESET=false
SOURCE_DIR=""
REDEPLOY=false
NAMESPACE="dev"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--reset) RESET=true ;;
        -s|--source-dir) SOURCE_DIR="$2"; shift ;;
        -d|--redeploy) REDEPLOY=true ;;
        *) usage ;;
    esac
    shift
done

# Check if SOURCE_DIR specified
if [[ -z "$SOURCE_DIR" ]]; then
    echo "Error: Source directory must be specified with the -s or --source-dir flag."
    usage
fi

# Function to reset the compilation pod
reset_pod() {
    echo "Shutting down the existing compilation pod..."
    kubectl delete job cta-compile-job -n $NAMESPACE

    echo "Starting a new compilation pod..."
    kubectl create -f cta-compile-job.yaml -n $NAMESPACE
}

# Function to compile the CTA project
compile_cta() {
    echo "Compiling the CTA project from source directory: $SOURCE_DIR"
    kubectl cp "$SOURCE_DIR" cta-compile-pod:/src/cta -n $NAMESPACE
    kubectl exec -it cta-compile-pod -n $NAMESPACE -- /bin/bash -c "cd /src/cta && make"
}

# Function to redeploy the Minikube configuration
redeploy_minikube() {
    echo "Redeploying Minikube configuration with newly compiled RPMs..."
    ./redeploy.sh
}

################################################################################

# Main script execution
if $RESET; then
    reset_pod
fi

compile_cta

if $REDEPLOY; then
    redeploy_minikube
fi

echo "Script execution completed."
