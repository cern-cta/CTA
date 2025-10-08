/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ListStorageClassRequest::operator==(const ListStorageClassRequest &rhs) const {
  return requester==rhs.requester;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ListStorageClassRequest::operator!=(const ListStorageClassRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ListStorageClassRequest &obj) {
  os << "(requester=" << obj.requester << ")";
  return os;
}

} // namespace cta::common::dataStructures
