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

# CTA generic image containing all CTA RPMs
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base

ENV BASEDIR="continuousintegration/docker/ctafrontend/alma9" \
    CTAREPODIR="/opt/repo"

# Add orchestration run scripts locally
COPY ${BASEDIR}/../opt /opt

# Custom Yum repo setup
ARG YUM_REPOS_DIR=continuousintegration/docker/ctafrontend/alma9/etc/yum.repos.d/
ARG YUM_VERSIONLOCK_FILE=continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/versionlock.list

COPY ${YUM_REPOS_DIR} /etc/yum.repos.d/
COPY ${YUM_VERSIONLOCK_FILE} /etc/dnf/plugins/versionlock.list

# Install necessary packages
# Note that we can improve this by using a multi-stage build; the final image does not need all of the below packages.
RUN yum install -y \
      python3-dnf-plugin-versionlock \
      yum-utils \
      createrepo epel-release \
      jq bc \
      sqlite \
  && \
    # where do we want to get xroot from?
    # epel version is more recent but eos-citrine-depend repo has a higher priority (4) than epel
    # => need to set epel prio to 4: yum-config-manager --setopt="epel.priority=4"
    # or if taking the older version from eos, eos-citrine-depend must be enabled so that
    # the older version of xroot is installed by cta-cli and is not conflicting later when installing eos...
    # Newer version of CTA requires xroot >= 4.4.1-1...
    # therefore set epel priority to 4...
    # Create local repo for cta artifacts and to cache RPMs locally
    mkdir -p ${CTAREPODIR}/RPMS/x86_64 \
  && \
    # logrotate files must be 0644 or 0444
    # .rpmnew files are ignored %config (no replace)
    chmod 0644 /etc/logrotate.d/*

# Add previously built rpms
COPY image_rpms ${CTAREPODIR}/RPMS/x86_64

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

# Check that CTA packages are in container (from previous artifacts)
RUN find ${CTAREPODIR}/RPMS/x86_64 | grep cta-taped && echo "cta-taped rpm is present: artifacts seems OK" || (echo "cta-taped rpm was not added from previously built artifact: this is a gitlab issue that must be investigated" 1>&2; exit 1)

RUN yum-config-manager --enable cta-artifacts