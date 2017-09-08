# ******************************************************************************
#
# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# ****************************************************************************/

# CTA generic image for system tests
# FROM must be the first command in Dockerfile!!

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base

# my environment variables
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods" \
    LOCALORCHESTRATIONDIR="/opt/ci" \
    BASEDIR="continuousintegration/docker/ctafrontend/cc7" \
    CTAREPODIR="/tmp/repo"

# Manage repos before running yum
ADD ${BASEDIR}/etc /etc

# yum update could be added at some point.
RUN yum install -y \
      yum-plugin-downloadonly \
      yum-plugin-priorities \
      yum-plugin-versionlock \
      createrepo epel-release \
  && \
    yum clean all


# where do we want to get xroot from?
# epel version is more recent but eos-citrine-depend repo has a higher priority (4) than epel
# => need to set epel prio to 4: yum-config-manager --setopt="epel.priority=4"
# or if taking the older version from eos, eos-citrine-depend must be enabled so that
# the older version of xroot is installed by cta-cli and is not conflicting later when installing eos...
# Newer version of CTA requires xroot >= 4.4.1-1...
# therefore set epel priority to 4...
# Create local repo for cta artifacts and to cache RPMs locally


# Populate local repository and enable it, then install any rpm that is needed for
# any container. Finally add needed directories.
RUN yum-config-manager --enable epel --setopt="epel.priority=4" \
  && \
    yum-config-manager --enable castor \
  && \
    yum install -y\
      eos-client \
      eos-server \
      xrootd \
      xrootd-client \
      xrootd-debuginfo \
      xrootd-server \
      oracle-instantclient12.1-basic \
      oracle-instantclient12.1-meta \
      cgdb \
      less \
      psmisc \
      lynx \
      strace \
      ltrace \
      heimdal-server \
      heimdal-workstation \
      ceph-common \
      mt-st \
      mtx \
      lsscsi \
      sg3_utils \
      castor-rmc-server \
      protobuf3 \
      cryptopp \
      json-c \
      castor-rmc-server \
      shadow-utils \
      sudo \
  && \
    yum clean all \
  && \
    mkdir -pv /etc/cta

# Docker image run setup
ADD ${BASEDIR}/run.sh /

ENTRYPOINT ["/run.sh"]
