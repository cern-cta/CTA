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

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base

ARG EOS_VERSION=master

RUN yum install -y git which;

WORKDIR /eos

RUN set -ex; \
    git clone https://gitlab.cern.ch/dss/eos.git /eos; \
    git checkout ${EOS_VERSION}; \
    git submodule sync --recursive && git submodule update --init --recursive;

RUN set -ex; \
    cd /eos; \
    sed -e 's/sudo\ //g' ./utils/centos7-dev-environment.sh > /tmp/centos7-dev-environment.sh; \
    # Remove reboot and quarkdb installation
    sed '/reboot -h now/d' -i /tmp/centos7-dev-environment.sh; \
    sed '/install -y quarkdb/d' -i /tmp/centos7-dev-environment.sh; \
    chmod +x /tmp/centos7-dev-environment.sh; \
    /tmp/centos7-dev-environment.sh;

RUN set -ex; \
    rm -rf /eos; \
    yum clean all;

COPY entrypoint.sh /entrypoint.sh
COPY entrypoint_rpm.sh /entrypoint_rpm.sh

ENTRYPOINT [ "/entrypoint.sh" ]
