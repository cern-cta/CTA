#!/bin/bash

## on the cta-cli-0 pod run the following
NAMESPACE=dev
CTA_CLI_POD=cta-cli-0
kubectl exec -n ${NAMESPACE} ${CTA_CLI_POD} -c cta-cli -- yum install -y cta-admin-grpc-stream
kubectl exec -n ${NAMESPACE} ${CTA_CLI_POD} -c cta-cli -- cta-admin-grpc-stream tape ls --all
