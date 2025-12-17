/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DesiredDriveState.hpp"

#include "common/log/PriorityMaps.hpp"

#include <sstream>

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
  return os << "(up=" << (obj.up ? "true" : "false") << " forceDown=" << (obj.forceDown ? "true" : "false") << ")";
}

}  // namespace cta::common::dataStructures
