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

#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveJob::ArchiveJob():
  copyNumber(0),
  archiveFileID(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveJob::operator==(const ArchiveJob &rhs) const {
  return request==rhs.request
      && tapePool==rhs.tapePool
      && instanceName==rhs.instanceName
      && copyNumber==rhs.copyNumber
      && archiveFileID==rhs.archiveFileID;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveJob::operator!=(const ArchiveJob &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveJob &obj) {
  os << "(request=" << obj.request
     << " tapePool=" << obj.tapePool
     << " instanceName=" << obj.instanceName
     << " copyNumber=" << obj.copyNumber
     << " archiveFileID=" << obj.archiveFileID << ")";
  return os;
}

} // namespace cta::common::dataStructures
