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

#include "catalogue/ArchiveFileRow.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFileRow::ArchiveFileRow() :
  archiveFileId(0),
  size(0),
  creationTime(0),
  reconciliationTime(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFileRow::operator==(const ArchiveFileRow &rhs) const {
  return
    archiveFileId == rhs.archiveFileId &&
    diskInstance == rhs.diskInstance &&
    diskFileId == rhs.diskFileId &&
    diskFileOwnerUid == rhs.diskFileOwnerUid &&
    diskFileGid == rhs.diskFileGid &&
    size == rhs.size &&
    checksumBlob == rhs.checksumBlob &&
    storageClassName == rhs.storageClassName;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveFileRow &obj) {
  os <<
  "{"
  "archiveFileId=" << obj.archiveFileId <<
  "diskInstance=" << obj.diskInstance <<
  "diskFileId=" << obj.diskFileId <<
  "diskFileOwnerUid=" << obj.diskFileOwnerUid <<
  "diskFileGid=" << obj.diskFileGid <<
  "size=" << obj.size <<
  "checksumBlob=" << obj.checksumBlob <<
  "storageClassName=" << obj.storageClassName <<
  "}";
  return os;
}

} // namespace catalogue
} // namespace cta
