#!/bin/bash

kubectl -n dev exec ctafrontend -- bash -c 'if [ "$(rpm -qa cta-objectstore-tools | wc -l)" -lt "1" ]; then yum install -y cta-objectstore-tools; fi'
kubectl -n dev exec ctafrontend -- cta-objectstore-dump-object $1 | sed -n '/Body dump:/,$p' | sed '1d' | jq '.'