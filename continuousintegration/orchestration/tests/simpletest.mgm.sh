# example test

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

kubectl --namespace ${NAMESPACE} exec mgm xrdcp /etc/group root://localhost//eos/mgm/cta/toto

kubectl --namespace ${NAMESPACE} exec mgm eos file workflow /eos/mgm/cta/toto default closew

kubectl --namespace ${NAMESPACE} exec mgm eos file workflow /eos/mgm/cta/toto default prepare
