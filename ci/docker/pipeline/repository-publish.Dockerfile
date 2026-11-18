# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest

RUN dnf install -y epel-release dnf-plugins-core && \
    dnf config-manager --set-enabled crb && \
    dnf install -y git curl jq python3 krb5-workstation rsync openssh-clients xrootd-client && \
    dnf clean all --enablerepo=\*

# Verify the client tools without contacting a repository or obtaining credentials.
RUN command -v kinit && command -v xrdfs && command -v xrdcp && \
    dnf repoquery --help >/dev/null
