/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
TapeFile::TapeFile():
  fSeq(0),
  blockId(0),
  compressedSize(0),
  copyNb(0),
  creationTime(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFile::operator==(const TapeFile &rhs) const {
  return vid==rhs.vid
      && fSeq==rhs.fSeq
      && blockId==rhs.blockId
      && compressedSize==rhs.compressedSize
      && copyNb==rhs.copyNb
      && creationTime==rhs.creationTime;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapeFile::operator!=(const TapeFile &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// TapeFile::isActiveCopyNb
//------------------------------------------------------------------------------
bool TapeFile::isActiveCopyNb(uint32_t cnb) const {
  return (cnb == copyNb) && supersededByVid.empty();
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeFile &obj) {
  os << "(vid=" << obj.vid
     << " fSeq=" << obj.fSeq
     << " blockId=" << obj.blockId
     << " compressedSize=" << obj.compressedSize
     << " copyNb=" << obj.copyNb
     << " creationTime=" << obj.creationTime << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
