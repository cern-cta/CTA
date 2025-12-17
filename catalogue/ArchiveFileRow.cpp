/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/ArchiveFileRow.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFileRow::operator==(const ArchiveFileRow& rhs) const {
  return archiveFileId == rhs.archiveFileId && diskInstance == rhs.diskInstance && diskFileId == rhs.diskFileId
         && diskFileOwnerUid == rhs.diskFileOwnerUid && diskFileGid == rhs.diskFileGid && size == rhs.size
         && checksumBlob == rhs.checksumBlob && storageClassName == rhs.storageClassName;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const ArchiveFileRow& obj) {
  os << "{"
        "archiveFileId="
     << obj.archiveFileId << "diskInstance=" << obj.diskInstance << "diskFileId=" << obj.diskFileId
     << "diskFileOwnerUid=" << obj.diskFileOwnerUid << "diskFileGid=" << obj.diskFileGid << "size=" << obj.size
     << "checksumBlob=" << obj.checksumBlob << "storageClassName=" << obj.storageClassName << "}";
  return os;
}

}  // namespace cta::catalogue
