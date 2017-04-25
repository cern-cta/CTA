# ******************************************************************************
#
# The CERN Tape Archive (CTA) project
# Copyright (C) 2017  CERN
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

# Docker image to build XRootD RPMs

# FROM must be the first command in Dockerfile

FROM gitlab-registry.cern.ch/linuxsupport/cc7-base

# Set up default environment

RUN yum clean all && \
    yum update -y && \
    yum install -y \
      ccache \
      cgdb \
      cmake \
      gcc-c++ \
      gdb \
      git \
      ltrace \
      make \
      psmisc \
      rpm-build \
      strace \
      sudo \
      vim \
      yum-utils

# Get XRootD

RUN git clone https://github.com/xrootd/xrootd.git

# Check out SSIV2

RUN cd xrootd && git checkout xrdssiV2

# Copy in patched xrootd.spec.in

ADD xrootd.spec.in /xrootd/packaging/rhel/xrootd.spec.in
RUN cp xrootd/packaging/rhel/xrootd.spec.in xrootd.spec

# Install all necessary build dependencies

RUN yum-builddep -y xrootd.spec

# Create XRootD source RPM

RUN cd xrootd/packaging && ./makesrpm.sh

# Create final RPMs from the source RPM

RUN rpmbuild --define "_topdir ${PWD}/rpmbuild_xrootd" --rebuild xrootd/packaging/*.src.rpm

# Notes on XRootD version
#
# Newer version of CTA requires xroot >= 4.4.1-1
#
# epel version is more recent but eos-citrine-depend repo has a higher priority (4) than epel,
#   so: need to set epel priority to 4:
#
#       RUN yum-config-manager --enable epel --setopt="epel.priority=4" 
#
#   or: if taking the older version from eos, eos-citrine-depend must be enabled so that the older version of xroot is
#       installed by cta-cli and is not conflicting later when installing eos...
