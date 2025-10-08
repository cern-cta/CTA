/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFile::TapeFile():
  fSeq(0),
  blockId(0),
  fileSize(0),
  copyNb(0),
  creationTime(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFile::operator==(const TapeFile &rhs) const {
  return vid==rhs.vid
      && fSeq==rhs.fSeq
      && blockId==rhs.blockId
      && fileSize==rhs.fileSize
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
// TapeFile::matchesCopyNb
//------------------------------------------------------------------------------
bool TapeFile::matchesCopyNb(uint8_t cnb) const {
  return (cnb == copyNb);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeFile &obj) {
  os << "(vid=" << obj.vid
     << " fSeq=" << obj.fSeq
     << " blockId=" << obj.blockId
     << " fileSize=" << obj.fileSize
     << " copyNb=" << static_cast<int>(obj.copyNb)
     << " creationTime=" << obj.creationTime;
  os << ")";
  return os;
}

} // namespace cta::common::dataStructures
