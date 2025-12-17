/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/EntryLog.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
EntryLog::EntryLog() : time(0) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
EntryLog::EntryLog(const std::string& username, const std::string& host, const time_t time)
    : username(username),
      host(host),
      time(time) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool EntryLog::operator==(const EntryLog& rhs) const {
  return username == rhs.username && host == rhs.host && time == rhs.time;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool EntryLog::operator!=(const EntryLog& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const EntryLog& obj) {
  os << "(username=" << obj.username << " host=" << obj.host << " time=" << obj.time << ")";
  return os;
}

}  // namespace cta::common::dataStructures
