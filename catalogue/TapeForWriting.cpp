/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeForWriting.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeForWriting::operator==(const TapeForWriting &rhs) const {
  return vid == rhs.vid;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeForWriting &obj) {
  os <<
    "{"                  <<
    "vid="               << obj.vid << "," <<
    "lastFseq="          << obj.lastFSeq << "," <<
    "capacityInBytes="   << obj.capacityInBytes << "," <<
    "dataOnTapeInBytes=" << obj.dataOnTapeInBytes <<
    "}";

  return os;
}

} // namespace cta::catalogue
