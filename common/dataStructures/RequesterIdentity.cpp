/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RequesterIdentity.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RequesterIdentity::RequesterIdentity(const std::string& name, const std::string& group) : name(name), group(group) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool RequesterIdentity::operator==(const RequesterIdentity& rhs) const {
  return name == rhs.name && group == rhs.group;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool RequesterIdentity::operator!=(const RequesterIdentity& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const RequesterIdentity& obj) {
  os << "(name=" << obj.name << " group=" << obj.group << ")";
  return os;
}

}  // namespace cta::common::dataStructures
