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

ENV BASEDIR="continuousintegration/docker/ctafrontend/cc7"

# CTA frontend

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base


# Manage repos
ADD ${BASEDIR}/etc/yum.repos.d/* /etc/yum.repos.d/
RUN yum install -y yum-plugin-priorities epel-release

# Add previously built rpms
ADD build_rpm/RPM/RPMS /tmp
#RUN yum -y install cta-frontend cta-debuginfo

# Install needed packages
RUN yum -y install cgdb less psmisc lynx strace ltrace
RUN yum clean all

# Static configuration files
ADD ${BASEDIR}/etc/xrootd/xrootd-cta.cfg /etc/xrootd/xrootd-cta.cfg

# Docker image run setup
ADD ${BASEDIR}/run.sh /
ENTRYPOINT ["/run.sh"]
