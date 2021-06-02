#!/bin/bash -e

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

PUBLIC=true
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from cern repositories
fi

echo Creating the docker image...
if [[ "$PUBLIC" == false ]] ; then
  ./prepareImageStage1-rpms.sh
else
  ./prepareImageStage1-rpms-public.sh
fi
./prepareImageStage2-eos.sh
./prepareImageStage3-scripts.sh
./prepareImageStage2b-scripts.sh
