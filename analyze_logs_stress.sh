#!/bin/bash

podname_base="cta-taped-stk-s001l01d0"

for podno in $(seq 1 4)
do
    podname="$podname_base$podno-0"
    echo $podname
    kubectl cp analyze_logs.sh $podname:/analyze_logs.sh -n dev
done


# kubectl exec -it -n dev cta-client-0 -- bash -c 'dnf install -y python3-pip'
podname_base="cta-taped-stk-s001l01d0"
for podno in $(seq 1 4)
do
    podname="$podname_base$podno-0"
    echo $podname
    kubectl exec -n dev $podname -- bash -c "bash analyze_logs.sh"
done