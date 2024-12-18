# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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
COPY ${BASEDIR}/etc/yum.repos.d/ /etc/yum/yum.repos.d/


# Variable to specify the tag to be used for CTA RPMs from the cta-ci-repo
# Format: X.YY.ZZ.A-B
ARG PUBLIC_REPO_VER
ARG VERSIONLOCK_PATH

# Install necessary packages
RUN yum install -y \
      python3-dnf-plugin-versionlock \
      yum-utils \
      createrepo epel-release \
      jq bc \
      sqlite \
      wget \
  && \
    # logrotate files must be 0644 or 0444
    # .rpmnew files are ignored %config (no replace)
    chmod 0644 /etc/logrotate.d/*


# Install cta-release
RUN yum install -y cta-release-${PUBLIC_REPO_VER}.el9 \
  && \
    yum clean all \
  && \
    rm -rf /var/cache/yum \
  ; \
    rm -f /etc/rc.d/rc.local

