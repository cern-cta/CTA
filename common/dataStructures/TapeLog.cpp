/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/TapeLog.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeLog::TapeLog():
  time(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeLog::operator==(const TapeLog &rhs) const {
  return drive==rhs.drive
      && time==rhs.time;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapeLog::operator!=(const TapeLog &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeLog &obj) {
  os << "(drive=" << obj.drive
     << " time=" << obj.time << ")";
  return os;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const std::optional<TapeLog> &obj) {
  if(obj) {
    os << "(drive=" << obj.value().drive
       << " time=" << obj.value().time << ")";
  } else {
    os << "(N/A)";
  }
  return os;
}

} // namespace cta::common::dataStructures
