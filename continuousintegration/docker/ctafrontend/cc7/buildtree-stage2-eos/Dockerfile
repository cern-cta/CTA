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

# Update versionlock if needed
ADD ${BASEDIR}/etc/yum/pluginconf.d /etc/yum/pluginconf.d

COPY ${BASEDIR}/etc/yum.repos.d /etc/yum.repos.d

ADD ${BASEDIR}/opt /opt

RUN /opt/run/bin/cta-versionlock --file /etc/yum/pluginconf.d/versionlock.list config eos5; \
    yum-config-manager --disable cta-ci-eos; \
    yum-config-manager --enable cta-ci-eos-5; \
    yum-config-manager --enable eos-diopside-dep; \
    yum-config-manager --enable eos-diopside;

RUN /opt/run/bin/cta-versionlock --file /etc/yum/pluginconf.d/versionlock.list config xrootd5; \
    yum-config-manager --disable cta-ci-xroot; \
    yum-config-manager --enable cta-ci-xrootd5;

# Install eos packages.
RUN yum install -y\
      eos-client \
      eos-server \
  && \
    yum clean all \
  && \
    rm -rf /var/cache/yum

# Docker image run setup
ADD ${BASEDIR}/run.sh /

ENTRYPOINT ["/run.sh"]
