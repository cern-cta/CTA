#!/bin/bash -e

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

cd ~/CTA

rpm_source=$1
rpm_dir="build_rpm/RPM/RPMS/x86_64"

image_tag=$2

if [ -z "${rpm_source}" ]; then
  echo "You should specify the path to the RPMs to be installed. Ex: ~/CTA-build/RPM/RPMS/x86_64";
  exit 1;
fi

if [ -z "${image_tag}" ]; then
  echo "You should specify the docker image tag. Ex: dev";
  exit 1;
fi

trap "rm -rf $rpm_dir" EXIT

mkdir -p $rpm_dir
cp -r $rpm_source $rpm_dir

sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/ci_runner/Dockerfile -t ctageneric:dev
