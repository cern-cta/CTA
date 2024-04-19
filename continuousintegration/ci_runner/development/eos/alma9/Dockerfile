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

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base

ARG EOS_VERSION=master

COPY ./repos/ /etc/yum.repos.d/

RUN mkdir -p shared;

RUN set -ex; \
    dnf -y update; \
    dnf -y install epel-release almalinux-release-devel; \
    dnf -y install git vim rsync cmake3 dnf-utils ccache openssh-server; \
    dnf -y group install "Development Tools";

RUN set -ex; \
    git clone https://gitlab.cern.ch/dss/eos.git /EOS_setup; \
    cd EOS_setup; \
    git checkout ${EOS_VERSION}; \
    git submodule sync --recursive && git submodule update --init --recursive;

RUN set -ex; \
    mkdir build_srpm; \
    cd build_srpm; \
    cmake3 -DPACKAGEONLY:Bool=true ../EOS_setup; \
    make srpm;

RUN set -ex; \
    yum-builddep -y --nogpgcheck build_srpm/SRPMS/*;

RUN set -ex; \
    rm -rf build_srpm; \
    rm -rf EOS_setup;

# Setup SSH server for remote access
RUN /usr/bin/ssh-keygen -A; \
    rm /run/nologin;

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
