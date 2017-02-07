#!/bin/bash


LIBRARY_DIR="/opt/librarydevice"


kubectl get pv | grep \/claimlibrary\ | grep -v Bound |  while read line; do
  library_device=$(echo $line | awk '{print $1}')
  pv_status=$(echo $line | awk '{print $4}')
  echo "Deleting PV ${library_device} with status ${pv_status}"
  kubectl delete pv ${library_device} && \
  kubectl create -f /opt/kubernetes/CTA/library/resource/${library_device}_librarydevice_resource.yaml && \
  echo OK
done
