/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapeFileWritten.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeFileWritten::operator==(const TapeFileWritten& rhs) const {
  return TapeItemWritten::operator==(rhs) && archiveFileId == rhs.archiveFileId && diskInstance == rhs.diskInstance
         && diskFileId == rhs.diskFileId && diskFileOwnerUid == rhs.diskFileOwnerUid && diskFileGid == rhs.diskFileGid
         && size == rhs.size && checksumBlob == rhs.checksumBlob && storageClassName == rhs.storageClassName
         && blockId == rhs.blockId && copyNb == rhs.copyNb && tapeDrive == rhs.tapeDrive;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const TapeFileWritten& obj) {
  os << "{"
        "archiveFileId="
     << obj.archiveFileId
     << ","
        "diskInstance="
     << obj.diskInstance
     << ","
        "diskFileId="
     << obj.diskFileId
     << ","
        "diskFileOwnerUid="
     << obj.diskFileOwnerUid
     << ","
        "diskFileGid="
     << obj.diskFileGid
     << ","
        "size="
     << obj.size
     << ","
        "checksumBlob="
     << obj.checksumBlob
     << ","
        "storageClassName="
     << obj.storageClassName
     << ","
        "vid="
     << obj.vid
     << ","
        "fSeq="
     << obj.fSeq
     << ","
        "blockId="
     << obj.blockId
     << ","
        "copyNb="
     << obj.copyNb
     << ","
        "tapeDrive="
     << obj.tapeDrive << "}";
  return os;
}

}  // namespace cta::catalogue
