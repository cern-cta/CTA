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
