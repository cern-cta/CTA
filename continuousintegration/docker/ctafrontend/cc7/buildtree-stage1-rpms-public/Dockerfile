# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

# CTA generic image for system tests
# FROM must be the first command in Dockerfile!!

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base

# my environment variables
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods" \
    LOCALORCHESTRATIONDIR="/opt/ci" \
    BASEDIR="continuousintegration/docker/ctafrontend/cc7" \
    CTAREPODIR="/tmp/repo"

# yum update could be added at some point.
RUN yum install -y \
      wget \
  && \
    yum clean all

# Add repos for CTA dependencies
ADD ${BASEDIR}/etc/logrotate.d /etc/logrotate.d
ADD ${BASEDIR}/etc/yum /etc/yum
ADD ${BASEDIR}/etc/yum.repos.d-public /etc/yum.repos.d

RUN wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
RUN wget https://git.ceph.com/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph

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
    yum install -y\
      xrootd \
      xrootd-client \
      xrootd-debuginfo \
      xrootd-server \
      sudo \
      logrotate \
      oracle-instantclient19.3-basic \
      oracle-instantclient19.3-meta \
      oracle-instantclient19.3-sqlplus \
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
      protobuf3 \
      cryptopp \
      json-c \
      shadow-utils \
      sudo \
      zeromq \
      postgresql-libs \
      gtest \
      jemalloc \
      eos-folly \
      hiredis \
      jsoncpp \
      libmicrohttpd \
      jq \
      python36 \
  && \
    yum clean all \
  && \
    rm -rf /var/cache/yum \
  && \
    mkdir -pv /etc/cta

# Mark the image as a buildtree image
RUN touch /etc/buildtreeRunner

# Docker image run setup
ADD ${BASEDIR}/run.sh /

ENTRYPOINT ["/run.sh"]
