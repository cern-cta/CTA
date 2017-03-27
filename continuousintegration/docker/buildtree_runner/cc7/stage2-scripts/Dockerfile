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

FROM buildtree-runner-stage1

# my environment variables
ENV ORCHESTRATIONDIR="continuousintegration/orchestration/pods" \
    LOCALORCHESTRATIONDIR="/opt/ci" \
    BASEDIR="continuousintegration/docker/buildtree_runner/cc7" \
    CTAREPODIR="/tmp/repo"

# Add pod specific configuration
ADD ${BASEDIR}/config ${LOCALORCHESTRATIONDIR}

# Add orchestration run scripts locally
ADD ${BASEDIR}/opt /opt

# Docker image run setup
ADD ${BASEDIR}/run.sh /

ENTRYPOINT ["/run.sh"]
