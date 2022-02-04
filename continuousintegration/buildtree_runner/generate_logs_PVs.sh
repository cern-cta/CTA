#!/bin/bash

# Hostpath for log PVs
LOG_PATH=/exports/logs
# Number of log PVs that must be created
NUM_LOG_PV=10

tmpdir=$(mktemp -d)

echo "Deleting LOGS persitent volumes from kubernetes"
for pv in $(kubectl get persistentvolumes -l type=logs | tail -n+2 | awk '{print $1}'); do
  kubectl delete persistentvolume ${pv}
done

echo "Creating LOGS persitent volumes into kubernetes"
for ((i=0;i<${NUM_LOG_PV};i++)); do
  num=$(printf %.2d ${i})
  mkdir -p ${LOG_PATH}/${num}
cat <<EOF > ${tmpdir}/pv${num}.yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: log${num}
  labels:
    type: logs
spec:
  capacity:
    storage: 2Gi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Recycle
  hostPath:
    path: ${LOG_PATH}/${num}
EOF
  kubectl create -f ${tmpdir}/pv${num}.yaml
done

kubectl get persistentvolumes -l type=logs
rm -fr ${tmpdir}
