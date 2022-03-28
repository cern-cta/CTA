#!/bin/bash

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


VERSIONLOCK_LIST=/etc/yum/pluginconf.d/versionlock.list

versionlock_eos_4_disable () {
  sed  -i '/#*\s*EOS-4-START/,/#*\s*EOS-4-END/{/#*\s*EOS-4/!{s/^[^#]/#/g}}' $VERSIONLOCK_LIST
}

versionlock_eos_4_enable () {
  sed  -i '/#*\s*EOS-4-START/,/#*\s*EOS-4-END/{/#*\s*EOS-4/!{s/^#//g}}' $VERSIONLOCK_LIST
}

versionlock_eos_5_disable () {
  sed  -i '/#*\s*EOS-5-START/,/#*\s*EOS-5-END/{/#*\s*EOS-5/!{s/^[^#]/#/g}}' $VERSIONLOCK_LIST
}

versionlock_eos_5_enable () {
  sed  -i '/#*\s*EOS-5-START/,/#*\s*EOS-5-END/{/#*\s*EOS-5/!{s/^#//g}}' $VERSIONLOCK_LIST
}
