#!/bin/bash

# Hostpath for stg PVs
STG_PATH=/exports/stg
# Number of stg PVs that must be created
NUM_STG_PV=10

tmpdir=$(mktemp -d)

echo "Deleting STG persitent volumes from kubernetes"
for pv in $(kubectl get persistentvolumes -l type=stg | tail -n+2 | awk '{print $1}'); do
  kubectl delete persistentvolume ${pv}
done

echo "Creating STG persitent volumes into kubernetes"
for ((i=0;i<${NUM_STG_PV};i++)); do
  num=$(printf %.2d ${i})
  mkdir -p ${STG_PATH}/${num}
cat <<EOF > ${tmpdir}/pv${num}.yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: stg${num}
  labels:
    type: stg
spec:
  capacity:
    storage: 3Gi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Recycle
  hostPath:
    path: ${STG_PATH}/${num}
EOF
  kubectl create -f ${tmpdir}/pv${num}.yaml
done

kubectl get persistentvolumes -l type=stg
rm -fr ${tmpdir}
