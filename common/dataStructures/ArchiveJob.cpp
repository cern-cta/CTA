/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
