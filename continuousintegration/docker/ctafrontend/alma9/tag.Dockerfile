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

# Variable to specify the tag to be used for CTA RPMs from the cta-ci-repo
# Format: X.YY.ZZ.A-B
ARG PUBLIC_REPO_VER=FALSE

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
      wget \
  && \
    # logrotate files must be 0644 or 0444
    # .rpmnew files are ignored %config (no replace)
    chmod 0644 /etc/logrotate.d/* \
  && \
    mkdir -p ${CTAREPODIR}/RPMS/x86_64 \
  && \
    # Install oracle instant client into the container.
    wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm && \
      yum install -y oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm && \
      yum install -y oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm

# Download RPMs into local repository and enable it
RUN yum-config-manager --enable epel --setopt="epel.priority=4" \
   && \
     yum download \
        --destdir ${CTAREPODIR}/RPMS/x86_64/ \
        --repofrompath cta-public-testing,https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64/ \
        cta-*-${PUBLIC_REPO_VER}.*.x86_64 \
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
RUN find ${CTAREPODIR}/RPMS/x86_64 | grep cta-taped && echo "cta-taped rpm is present" || (echo "cta-taped RPM was not found after an attempt to download from CTA Public repository. Please investigate." 1>&2; exit 1)

RUN yum-config-manager --enable cta-artifacts