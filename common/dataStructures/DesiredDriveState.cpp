/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <sstream>

#include "DesiredDriveState.hpp"
#include "common/log/PriorityMaps.hpp"

namespace cta::common::dataStructures {

void DesiredDriveState::setReasonFromLogMsg(int logLevel, std::string_view msg) {
  reason = generateReasonFromLogMsg(logLevel, msg);
}

std::string DesiredDriveState::generateReasonFromLogMsg(int logLevel, std::string_view msg) {
  std::stringstream reason_s;
  reason_s << c_tpsrvPrefixComment << " " << cta::log::PriorityMaps::getPriorityText(logLevel) << " " << msg;
  return reason_s.str();
}

std::ostream& operator<<(std::ostream& os, const DesiredDriveState& obj) {
  return os << "(up="        << (obj.up        ? "true" : "false")
            << " forceDown=" << (obj.forceDown ? "true" : "false") << ")";
}

} // namespace cta::common::dataStructures
