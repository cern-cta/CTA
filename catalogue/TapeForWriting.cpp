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

#include "catalogue/TapeForWriting.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeForWriting::TapeForWriting() : lastFSeq(0), capacityInBytes(0), dataOnTapeInBytes(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeForWriting::operator==(const TapeForWriting& rhs) const {
  return vid == rhs.vid;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const TapeForWriting& obj) {
  os << "{"
     << "vid=" << obj.vid << ","
     << "lastFseq=" << obj.lastFSeq << ","
     << "capacityInBytes=" << obj.capacityInBytes << ","
     << "dataOnTapeInBytes=" << obj.dataOnTapeInBytes << "}";

  return os;
}

}  // namespace catalogue
}  // namespace cta
