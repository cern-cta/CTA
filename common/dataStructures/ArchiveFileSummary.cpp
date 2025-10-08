/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFileSummary::ArchiveFileSummary():
  totalBytes(0),
  totalFiles(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveFileSummary::operator==(const ArchiveFileSummary &rhs) const {
  return totalBytes==rhs.totalBytes
      && totalFiles==rhs.totalFiles;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveFileSummary::operator!=(const ArchiveFileSummary &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveFileSummary &obj) {
  os << "(totalBytes=" << obj.totalBytes
     << " totalFiles=" << obj.totalFiles << ")";
  return os;
}

} // namespace cta::common::dataStructures
