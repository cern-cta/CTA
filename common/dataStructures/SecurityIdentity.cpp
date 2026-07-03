/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/SecurityIdentity.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool SecurityIdentity::operator==(const SecurityIdentity& rhs) const {
  return username == rhs.username && host == rhs.host && clientHost == rhs.clientHost;
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool SecurityIdentity::operator<(const SecurityIdentity& rhs) const {
  if (username == rhs.username) {
    if (host == rhs.host) {
      return clientHost < rhs.clientHost;
    } else {
      return host < rhs.host;
    }
  } else {
    return username < rhs.username;
  }
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const SecurityIdentity& obj) {
  os << "(username=" << obj.username << " host=" << obj.host << " clientHost=" << obj.clientHost << ")";
  return os;
}

}  // namespace cta::common::dataStructures
