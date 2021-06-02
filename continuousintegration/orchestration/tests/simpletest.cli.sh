#!/bin/bash

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

cta bs -u root --hostname $(hostname -i) -m "docker cli"

cta logicallibrary add --name VLSTK --comment "ctasystest"

cta tapepool add --name ctasystest --partialtapesnumber 5 --encrypted false --comment "ctasystest"

cta tape add --logicallibrary VLSTK --tapepool ctasystest --capacity 1000000000 --comment "ctasystest" --vid ${VID} --full false

cta storageclass add --instance root --name ctaStorageClass --numberofcopies 1 --comment "ctasystest"

cta archiveroute add --instance root --storageclass ctaStorageClass --copynb 1 --tapepool ctasystest --comment "ctasystest"

cta mountpolicy add --name ctasystest --archivepriority 1 --minarchiverequestage 1 --retrievepriority 1 --minretrieverequestage 1 --comment "ctasystest"

cta requestermountrule add --instance root --name root --mountpolicy ctasystest --comment "ctasystest"

cta drive up VDSTK1
