#!/bin/bash -e

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

LIBRARY_DIR=/opt/kubernetes/CTA/library

mkdir -p ${LIBRARY_DIR} ${LIBRARY_DIR}/config ${LIBRARY_DIR}/resource


echo "Deleting MHVTL persitent volumes from kubernetes"
for pv in $(kubectl get persistentvolumes -l config=library,type=mhvtl | tail -n+2 | awk '{print $1}'); do
  kubectl delete persistentvolume ${pv}
  rm -f ${LIBRARY_DIR}/config/library-config-${pv}.yaml
  rm -f ${LIBRARY_DIR}/resource/${pv}_librarydevice_resource.yaml
done

tempdir=$(mktemp -d)

# lsscsi adds some trailing spaces for padding on short device names like 'sg4' so remove those before grepping
lsscsi -g | sed -e 's/\s\+$//' > ${tempdir}/lsscsi-g.dump


for device in $(grep mediumx ${tempdir}/lsscsi-g.dump | awk {'print $7'} | sed -e 's%/dev/%%'); do

  line=$(grep ${device}\$ ${tempdir}/lsscsi-g.dump);
  scsi_host="$(echo $line | sed -e 's/^.//' | cut -d\: -f1)"
  scsi_channel="$(echo $line | cut -d\: -f2)"

  drivenames=$(grep "^.${scsi_host}:${scsi_channel}:" ${tempdir}/lsscsi-g.dump | grep tape | sed -e 's/^.[0-9]\+:\([0-9]\+\):\([0-9]\+\):.*/ULT\1\2/' | xargs -itoto echo -n " toto")
  drivedevices=$(grep "^.${scsi_host}:${scsi_channel}:" ${tempdir}/lsscsi-g.dump | grep tape | awk '{print $6}' | sed -e 's%/dev/%n%' | xargs -itoto echo -n " toto")

cat <<EOF > ${LIBRARY_DIR}/config/library-config-${device}.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: library-config
  labels:
    config: library
    type: mhvtl
data:
  library.type: mhvtl
  library.name: $(echo ${line} | awk '{print $4}')
  library.device: ${device}
  library.drivenames: ($(echo ${drivenames}|sed -e 's/^ //'))
  library.drivedevices: ($(echo ${drivedevices}|sed -e 's/^ //'))
  library.tapes: ($(mtx -f /dev/${device} status | grep Storage\ Element | grep Full | sed -e 's/.*VolumeTag=//;s/ //g;s/\(......\).*/\1/' | xargs -itoto echo -n " toto"| sed -e 's/^ //'))
EOF

  cat <<EOF > ${LIBRARY_DIR}/resource/${device}_librarydevice_resource.yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${device}
  labels:
    config: library
    type: mhvtl
  annotations:
    volume.beta.kubernetes.io/storage-class: "librarydevice"
spec:
  capacity:
    storage: 1Mi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Recycle
  nfs:
    path: /tmp
    server: 127.0.0.1
EOF

kubectl create -f ${LIBRARY_DIR}/resource/${device}_librarydevice_resource.yaml

done


  cat <<EOF > ${LIBRARY_DIR}/library_claim.yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: claimlibrary
  annotations:
    volume.beta.kubernetes.io/storage-class: "librarydevice"
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Mi
EOF

rm -fr ${tempdir}
