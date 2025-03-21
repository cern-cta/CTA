#!/bin/bash

## on the cta-cli-0 pod run the following
NAMESPACE=dev
CTA_CLI_POD=cta-cli-0
DB_POD=cta-catalogue-postgres
kubectl exec -n ${NAMESPACE} ${CTA_CLI_POD} -c cta-cli -- yum install -y cta-admin-grpc-stream
kubectl exec -n ${NAMESPACE} ${CTA_CLI_POD} -c cta-cli -- cta-admin-grpc-stream tape ls --all

## populate the DB
kubectl cp -n ${NAMESPACE} dump_after_ar_test ${DB_POD}:dump_after_prepare_test
kubectl exec -n ${NAMESPACE} ${DB_POD} -- psql -U cta -d cta < dump_after_prepare_test
