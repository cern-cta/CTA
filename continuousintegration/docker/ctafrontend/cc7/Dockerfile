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
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods"
ENV LOCALORCHESTRATIONDIR="/opt/ci"
ENV BASEDIR="continuousintegration/docker/ctafrontend/cc7"
ENV CTAREPODIR="/tmp/repo"

# Manage repos
ADD ${BASEDIR}/etc/yum.repos.d/* /etc/yum.repos.d/
RUN yum install -y yum-plugin-priorities createrepo epel-release

# Create local repo for cta artifacts
RUN mkdir -p ${CTAREPODIR} ${LOCALORCHESTRATIONDIR}
# Add previously built rpms
ADD build_rpm/RPM ${CTAREPODIR}
# Populate local repository and enable it
RUN createrepo ${CTAREPODIR}; echo -e "[cta-artifacts]\nname=CTA artifacts\nbaseurl=file://${CTAREPODIR}\ngpgcheck=0\nenabled=1\npriority=2" > /etc/yum.repos.d/cta-artifacts.repo

# Install needed packages
#RUN yum -y install cta-frontend cta-debuginfo cgdb less psmisc lynx strace ltrace
# add cta-cli to get already some RPMs installed
RUN yum -y install cgdb less psmisc lynx strace ltrace cta-cli && yum clean all
#RUN yum clean all

# Add pod specific configuration
ADD ${BASEDIR}/config ${LOCALORCHESTRATIONDIR}

# Add orchestration init scripts locally
ADD ${ORCHESTRATIONDIR}/init ${LOCALORCHESTRATIONDIR}

# Docker image run setup
ADD ${BASEDIR}/run.sh /
ENTRYPOINT ["/run.sh"]
