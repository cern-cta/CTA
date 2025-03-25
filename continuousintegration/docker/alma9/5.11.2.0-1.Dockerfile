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
# This image is similar to the one created by Dockerfile, except that this uses RPMs for a remote repo instead of local ones
# As a result, any installs on this image will also pull from public yum repos instead of private ones
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ENV BASEDIR="continuousintegration/docker/alma9" \
    CTAREPODIR="/opt/repo"

# Add orchestration run scripts locally
COPY ${BASEDIR}/../opt /opt
COPY ${BASEDIR}/etc/yum.repos.d/cta-public-testing.repo /etc/yum.repos.d/cta-public-testing.repo

ENV PUBLIC_REPO_VER=5.11.2.0-1

# Install necessary packages
RUN dnf install -y \
      python3-dnf-plugin-versionlock \
      yum-utils \
      epel-release \
      jq \
      bc \
      sqlite \
      wget \
      krb5-workstation && \
    # logrotate files must be 0644 or 0444
    chmod 0644 /etc/logrotate.d/*

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm; \
    wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm; \
    rm oracle-instantclient-basic-21.12.0.0.0-1.el9.x86_64.rpm oracle-instantclient-devel-21.12.0.0.0-1.el9.x86_64.rpm;


# We add the cta user so that we can consistently reference it in the Helm chart when changing keytab ownership
RUN useradd -m -u 1000 -g tape cta

# Install cta-release and clean up
RUN dnf config-manager --enable epel --setopt="epel.priority=4" && \
    dnf config-manager --enable cta-public-testing && \
    dnf install -y "cta-release-${PUBLIC_REPO_VER}.el9" && \
    cta-versionlock apply && \
    dnf clean all --enablerepo=\* && \
    rm -rf /etc/rc.d/rc.local
