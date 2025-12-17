/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ArchiveFile.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

#include <algorithm>

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFile::operator==(const ArchiveFile& rhs) const {
  return archiveFileID == rhs.archiveFileID && diskFileId == rhs.diskFileId && diskInstance == rhs.diskInstance
         && fileSize == rhs.fileSize && checksumBlob == rhs.checksumBlob && storageClass == rhs.storageClass
         && diskFileInfo == rhs.diskFileInfo && tapeFiles == rhs.tapeFiles;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveFile::operator!=(const ArchiveFile& rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::at()
//------------------------------------------------------------------------------
TapeFile& ArchiveFile::TapeFilesList::at(uint8_t copyNb) {
  auto ret = std::find_if(begin(), end(), [=](TapeFile& tf) { return tf.matchesCopyNb(copyNb); });
  if (ret == end()) {
    throw cta::exception::Exception("In ArchiveFile::TapeFilesList::at(): not found.");
  }
  return *ret;
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::at() const
//------------------------------------------------------------------------------
const TapeFile& ArchiveFile::TapeFilesList::at(uint8_t copyNb) const {
  auto ret = std::find_if(cbegin(), cend(), [=](const TapeFile& tf) { return tf.matchesCopyNb(copyNb); });
  if (ret == end()) {
    throw cta::exception::Exception("In ArchiveFile::TapeFilesList::at(): not found.");
  }
  return *ret;
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::find()
//------------------------------------------------------------------------------
ArchiveFile::TapeFilesList::iterator ArchiveFile::TapeFilesList::find(uint8_t copyNb) {
  return std::find_if(begin(), end(), [=](TapeFile& tf) { return tf.matchesCopyNb(copyNb); });
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::find() const
//------------------------------------------------------------------------------
ArchiveFile::TapeFilesList::const_iterator ArchiveFile::TapeFilesList::find(uint8_t copyNb) const {
  return std::find_if(cbegin(), cend(), [=](const TapeFile& tf) { return tf.matchesCopyNb(copyNb); });
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::removeAllVidsExcept()
//------------------------------------------------------------------------------
void ArchiveFile::TapeFilesList::removeAllVidsExcept(std::string_view vid) {
  remove_if([&vid](TapeFile& tf) { return tf.vid != vid; });
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const ArchiveFile& obj) {
  os << "{"
        "archiveFileID="
     << obj.archiveFileID
     << ","
        "diskFileID="
     << obj.diskFileId
     << ","
        "diskInstance="
     << obj.diskInstance
     << ","
        "fileSize="
     << obj.fileSize
     << ","
        "checksumBlob="
     << obj.checksumBlob
     << ","
        "storageClass="
     << obj.storageClass
     << ","
        "diskFileInfo="
     << obj.diskFileInfo
     << ","
        "tapeFiles="
     << obj.tapeFiles
     << ","
        "creationTime="
     << obj.creationTime
     << ","
        "reconciliationTime="
     << obj.reconciliationTime << "}";
  return os;
}

}  // namespace cta::common::dataStructures
