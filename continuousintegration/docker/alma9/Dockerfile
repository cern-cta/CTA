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

ENV BASEDIR="continuousintegration/docker/alma9" \
    CTAREPODIR="/opt/repo"

# Add orchestration run scripts locally
COPY ${BASEDIR}/../opt /opt

# Custom Yum repo setup
ARG YUM_REPOS_DIR=continuousintegration/docker/alma9/etc/yum.repos.d/
ARG YUM_VERSIONLOCK_FILE=continuousintegration/docker/alma9/etc/yum/pluginconf.d/versionlock.list

COPY ${YUM_REPOS_DIR} /etc/yum.repos.d/
COPY ${YUM_VERSIONLOCK_FILE} /etc/dnf/plugins/versionlock.list

# Install necessary packages
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      yum-utils \
      createrepo epel-release \
      jq bc \
      sqlite \
      krb5-workstation \
  && \
    # logrotate files must be 0644 or 0444
    # .rpmnew files are ignored %config (no replace)
    chmod 0644 /etc/logrotate.d/* \
  && \
    mkdir -p ${CTAREPODIR}/RPMS/x86_64

COPY image_rpms ${CTAREPODIR}/RPMS/x86_64

# Populate local repository and enable it
RUN dnf config-manager --enable epel --setopt="epel.priority=4" \
  && \
    createrepo ${CTAREPODIR} \
  && \
    echo -e "[cta-artifacts]\nname=CTA artifacts\nbaseurl=file://${CTAREPODIR}\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta-artifacts.repo \
  && \
    dnf clean all \
  && \
    rm -rf /var/cache/yum; \
    rm -rf /var/cache/dnf; \
    rm -f /etc/rc.d/rc.local

# Check that CTA packages are in container (from previous artifacts)
RUN find ${CTAREPODIR}/RPMS/x86_64 | grep cta-taped && echo "cta-taped rpm is present: artifacts seems OK" || (echo "cta-taped rpm was not added from previously built artifact: this is a gitlab issue that must be investigated" 1>&2; exit 1)

RUN dnf config-manager --enable cta-artifacts
