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

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
EntryLog::EntryLog() : time(0) {}

EntryLog::EntryLog(const EntryLog& other) {
  username = other.username;
  host = other.host;
  time = other.time;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
EntryLog::EntryLog(const std::string& username, const std::string& host, const time_t time) :
username(username),
host(host),
time(time) {}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
EntryLog& EntryLog::operator=(const EntryLog& other) {
  username = other.username;
  host = other.host;
  time = other.time;
  return *this;
}

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

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
