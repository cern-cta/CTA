# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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

# CTA generic image for system tests
# FROM must be the first command in Dockerfile!!

FROM buildtree-runner-stage1

# my environment variables
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods" \
    LOCALORCHESTRATIONDIR="/opt/ci" \
    BASEDIR="continuousintegration/docker/ctafrontend/cc7" \
    CTAREPODIR="/tmp/repo"

# Add pod specific configuration
ADD ${BASEDIR}/config ${LOCALORCHESTRATIONDIR}

# Add orchestration run scripts locally
ADD ${BASEDIR}/opt /opt

# Docker image run setup
ADD ${BASEDIR}/run.sh /

# Add custom rc.local to massage kubernetes resources for
# the taget docker container
ADD ${BASEDIR}/rc.local /etc/rc.d

# Make sure it can be executed by root
RUN chmod 744 /etc/rc.local

# Update versionlock if needed
ADD ${BASEDIR}/etc/yum/pluginconf.d /etc/yum/pluginconf.d

#Install the libs necessary for EOS to run
RUN yum install -y eos-protobuf3 libisa-l libisa-l_crypto libisa-l_crypto-devel xxhash-devel grpc

# disable selinux so that systemd works if the container is booted
# with an image that already contains selinux
ADD ${BASEDIR}/etc_selinux_config /etc/selinux/config

# One more thing so that we can use systemd
VOLUME [ "/sys/fs/cgroup" ]

# Mark the image as a buildtree image
RUN touch /etc/doubleBuildtreeRunner

# And finally run systemd if the entrypoint is not overriden by docker
ENTRYPOINT ["/usr/sbin/init"]
