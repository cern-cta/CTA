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

#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFile::TapeFile() : fSeq(0), blockId(0), fileSize(0), copyNb(0), creationTime(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFile::operator==(const TapeFile& rhs) const {
  return vid == rhs.vid && fSeq == rhs.fSeq && blockId == rhs.blockId && fileSize == rhs.fileSize &&
         copyNb == rhs.copyNb && creationTime == rhs.creationTime;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapeFile::operator!=(const TapeFile& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// TapeFile::matchesCopyNb
//------------------------------------------------------------------------------
bool TapeFile::matchesCopyNb(uint8_t cnb) const {
  return (cnb == copyNb);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const TapeFile& obj) {
  os << "(vid=" << obj.vid << " fSeq=" << obj.fSeq << " blockId=" << obj.blockId << " fileSize=" << obj.fileSize
     << " copyNb=" << static_cast<int>(obj.copyNb) << " creationTime=" << obj.creationTime;
  os << ")";
  return os;
}

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
