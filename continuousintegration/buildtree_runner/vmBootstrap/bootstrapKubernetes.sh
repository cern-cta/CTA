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

echo Setting up cri-o
modprobe overlay
modprobe br_netfilter
cat <<EOF > /etc/sysctl.d/99-kubernetes-cri.conf
net.bridge.bridge-nf-call-iptables  = 1
net.ipv4.ip_forward                 = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF
sysctl --system
yum clean all
yum install -y cri-o
systemctl daemon-reload
systemctl enable crio
systemctl start crio
systemctl status crio

echo Installing kubernetes
yum install -y kubeadm kubelet kubectl

echo Prepare kubernetes environment
curl -o /opt/calico.yaml https://docs.projectcalico.org/manifests/calico.yaml

echo Configuring cluster
kubeadm init --config=./kubeadm-crio.yaml --upload-certs --node-name $HOSTNAME | tee /root/kubeadm-init.out
kubectl apply -f /root/calico.yaml
kubectl taint nodes --all node-role.kubernetes.io/master-
