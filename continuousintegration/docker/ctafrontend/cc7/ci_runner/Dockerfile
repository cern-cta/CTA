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
    CTAREPODIR="/opt/repo"

# Manage repos before running yum
ADD ${BASEDIR}/etc /etc

RUN yum install -y \
      yum-plugin-downloadonly \
      yum-plugin-priorities \
      yum-plugin-versionlock \
      createrepo epel-release \
  && \
    # where do we want to get xroot from?
    # epel version is more recent but eos-citrine-depend repo has a higher priority (4) than epel
    # => need to set epel prio to 4: yum-config-manager --setopt="epel.priority=4"
    # or if taking the older version from eos, eos-citrine-depend must be enabled so that
    # the older version of xroot is installed by cta-cli and is not conflicting later when installing eos...
    # Newer version of CTA requires xroot >= 4.4.1-1...
    # therefore set epel priority to 4...
    # Create local repo for cta artifacts and to cache RPMs locally
    mkdir -p ${CTAREPODIR}/RPMS/x86_64 ${LOCALORCHESTRATIONDIR}

# Add previously built rpms
ADD build_rpm/RPM ${CTAREPODIR}

# Populate local repository and enable it
RUN yum-config-manager --enable epel --setopt="epel.priority=4" \
  && \
    yum install --downloadonly --downloaddir=${CTAREPODIR}/RPMS/x86_64 \
      eos-client \
      eos-server \
      xrootd-client \
      xrootd-debuginfo \
      xrootd-server \
  && \
    createrepo ${CTAREPODIR} \
  && \
    echo -e "[cta-artifacts]\nname=CTA artifacts\nbaseurl=file://${CTAREPODIR}\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta-artifacts.repo \
  && \
    # Install needed packages
    #RUN yum -y install cta-frontend cta-debuginfo cgdb less psmisc lynx strace ltrace
    # add cta-cli to get already some RPMs installed
    yum -y install cgdb \
      less \
      psmisc \
      lynx \
      strace \
      ltrace \
  && \
    yum clean all \
  && \
    rm -rf /var/cache/yum \
  ; \
    rm -f /etc/rc.d/rc.local

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

# disable selinux so that systemd works if the container is booted
# with an image that already contains selinux
ADD ${BASEDIR}/etc_selinux_config /etc/selinux/config

# One more thing so that we can use systemd
VOLUME [ "/sys/fs/cgroup" ]

# And finally run systemd if the entrypoint is not overriden by docker
ENTRYPOINT ["/usr/sbin/init"]
