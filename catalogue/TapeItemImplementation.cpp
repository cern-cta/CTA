/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeItemWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include <iostream>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeItemWritten::operator==(const TapeItemWritten &rhs) const {
  return
      fSeq == rhs.fSeq &&
      vid == rhs.vid;
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool TapeItemWritten::operator<(const TapeItemWritten &rhs) const {
  return fSeq < rhs.fSeq;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeItemWritten &obj) {
  os <<
  "{"
  "vid=" << obj.vid << ","
  "fSeq=" << obj.fSeq << ","
  "tapeDrive=" << obj.tapeDrive <<
  "}";
  return os;
}

bool operator<(const TapeItemWrittenPointer& a, const TapeItemWrittenPointer& b) { return *a < *b; }


} // namespace cta::catalogue
