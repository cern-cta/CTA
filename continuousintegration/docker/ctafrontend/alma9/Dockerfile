# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
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

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:20231201-2

# my environment variables
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods" \
    LOCALORCHESTRATIONDIR="/opt/ci" \
    BASEDIR="continuousintegration/docker/ctafrontend/alma9" \
    CTAREPODIR="/opt/repo"

# Manage repos before running yum
COPY ${BASEDIR}/repos/ /etc/yum.repos.d/
COPY ${BASEDIR}/../etc/ /etc/
# COPY ${BASEDIR}/yum/ /etc/yum/
COPY ${BASEDIR}/yum/pluginconf.d/versionlock.list  /etc/dnf/plugins/versionlock.list

# Add orchestration run scripts locally
COPY ${BASEDIR}/../opt /opt

RUN yum install -y \
      yum-plugin-versionlock \
      yum-utils \
      createrepo epel-release \
      wget jq bc \
  && \
    # where do we want to get xroot from?
    # epel version is more recent but eos-citrine-depend repo has a higher priority (4) than epel
    # => need to set epel prio to 4: yum-config-manager --setopt="epel.priority=4"
    # or if taking the older version from eos, eos-citrine-depend must be enabled so that
    # the older version of xroot is installed by cta-cli and is not conflicting later when installing eos...
    # Newer version of CTA requires xroot >= 4.4.1-1...
    # therefore set epel priority to 4...
    # Create local repo for cta artifacts and to cache RPMs locally
    mkdir -p ${CTAREPODIR}/RPMS/x86_64 ${LOCALORCHESTRATIONDIR} \
  && \
    # logrotate files must be 0644 or 0444
    # .rpmnew files are ignored %config (no replace)
    chmod 0644 /etc/logrotate.d/*

# Add previously built rpms
COPY build_rpm/RPM ${CTAREPODIR}

# Populate local repository and enable it
RUN yum-config-manager --enable epel --setopt="epel.priority=4" \
  && \
    createrepo ${CTAREPODIR} \
  && \
    echo -e "[cta-artifacts]\nname=CTA artifacts\nbaseurl=file://${CTAREPODIR}\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta-artifacts.repo \
  && \
    yum clean all \
  && \
    rm -rf /var/cache/yum \
  ; \
    rm -f /etc/rc.d/rc.local

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm; \
  wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm; \
  yum install -y oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm; \
  yum install -y oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm; \
  rm oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm;

# Check that CTA packages are in container (from previous artifacts)
RUN find ${CTAREPODIR}/RPMS/x86_64 | grep cta-taped && echo "cta-taped rpm is present: artifacts seems OK" || (echo "cta-taped rpm was not added from previously built artifact: this is a gitlab issue that must be investigated" 1>&2; exit 1)

# Check if we need to enable cta-ci-xrootd4 repository
RUN if find ${CTAREPODIR}/RPMS/x86_64 | grep cta-taped-4; then \
  /opt/run/bin/cta-versionlock --file /etc/yum/pluginconf.d/versionlock.list config xrootd4; \
  yum-config-manager --enable cta-ci-xroot; \
  yum-config-manager --disable cta-ci-xrootd5; \
fi

# Add pod specific configuration
COPY ${BASEDIR}/config ${LOCALORCHESTRATIONDIR}

# Docker image run setup
COPY ${BASEDIR}/run.sh /

# Add custom rc.local to massage kubernetes resources for
# the taget docker container
COPY ${BASEDIR}/rc.local /etc/rc.d

# Make sure it can be executed by root
RUN chmod 744 /etc/rc.local

# disable selinux so that systemd works if the container is booted
# with an image that already contains selinux
COPY ${BASEDIR}/etc_selinux_config /etc/selinux/config

# One more thing so that we can use systemd
VOLUME [ "/sys/fs/cgroup" ]

# And finally run systemd if the entrypoint is not overriden by docker
ENTRYPOINT ["/usr/sbin/init"]
