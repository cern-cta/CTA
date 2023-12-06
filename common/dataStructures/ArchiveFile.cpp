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

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

#include <algorithm>

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFile::ArchiveFile():
  archiveFileID(0),
  fileSize(0),
  creationTime(0),
  reconciliationTime(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFile::operator==(const ArchiveFile &rhs) const {
  return archiveFileID == rhs.archiveFileID
      && diskFileId    == rhs.diskFileId
      && diskInstance  == rhs.diskInstance
      && fileSize      == rhs.fileSize
      && checksumBlob  == rhs.checksumBlob
      && storageClass  == rhs.storageClass
      && diskFileInfo  == rhs.diskFileInfo
      && tapeFiles    == rhs.tapeFiles;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveFile::operator!=(const ArchiveFile &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::at()
//------------------------------------------------------------------------------
TapeFile& ArchiveFile::TapeFilesList::at(uint8_t copyNb) {
  auto ret = std::find_if(begin(), end(), [=](TapeFile& tf){ return tf.matchesCopyNb(copyNb);});
  if (ret == end()) throw cta::exception::Exception("In ArchiveFile::TapeFilesList::at(): not found.");
  return *ret;
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::at() const
//------------------------------------------------------------------------------
const TapeFile& ArchiveFile::TapeFilesList::at(uint8_t copyNb) const {
  auto ret = std::find_if(cbegin(), cend(), [=](const TapeFile& tf){ return tf.matchesCopyNb(copyNb);});
  if (ret == end()) throw cta::exception::Exception("In ArchiveFile::TapeFilesList::at(): not found.");
  return *ret;
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::find() 
//------------------------------------------------------------------------------
ArchiveFile::TapeFilesList::iterator ArchiveFile::TapeFilesList::find(uint8_t copyNb) {
  return std::find_if(begin(), end(), [=](TapeFile& tf){ return tf.matchesCopyNb(copyNb);});
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::find() const
//------------------------------------------------------------------------------
ArchiveFile::TapeFilesList::const_iterator ArchiveFile::TapeFilesList::find(uint8_t copyNb) const {
  return std::find_if(cbegin(), cend(), [=](const TapeFile& tf){ return tf.matchesCopyNb(copyNb);});
}

//------------------------------------------------------------------------------
// ArchiveFile::TapeFilesList::removeAllVidsExcept()
//------------------------------------------------------------------------------
void ArchiveFile::TapeFilesList::removeAllVidsExcept(const std::string& vid) {
  remove_if([&vid](TapeFile& tf){ return tf.vid != vid; });
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveFile &obj) {
  os <<
    "{"
    "archiveFileID="      << obj.archiveFileID      << ","
    "diskFileID="         << obj.diskFileId         << ","
    "diskInstance="       << obj.diskInstance       << ","
    "fileSize="           << obj.fileSize           << ","
    "checksumBlob="       << obj.checksumBlob       << ","
    "storageClass="       << obj.storageClass       << ","
    "diskFileInfo="       << obj.diskFileInfo       << ","
    "tapeFiles="          << obj.tapeFiles          << ","
    "creationTime="       << obj.creationTime       << ","
    "reconciliationTime=" << obj.reconciliationTime <<
    "}";
  return os;
}

} // namespace cta::common::dataStructures
