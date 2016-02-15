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

#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary::ArchiveFileSummary() {  
  m_totalBytesSet = false;
  m_totalFilesSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary::~ArchiveFileSummary() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ArchiveFileSummary::allFieldsSet() const {
  return m_totalBytesSet
      && m_totalFilesSet;
}

//------------------------------------------------------------------------------
// setTotalBytes
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFileSummary::setTotalBytes(const uint64_t totalBytes) {
  m_totalBytes = totalBytes;
  m_totalBytesSet = true;
}

//------------------------------------------------------------------------------
// getTotalBytes
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ArchiveFileSummary::getTotalBytes() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFileSummary have been set!");
  }
  return m_totalBytes;
}

//------------------------------------------------------------------------------
// setTotalFiles
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFileSummary::setTotalFiles(const uint64_t totalFiles) {
  m_totalFiles = totalFiles;
  m_totalFilesSet = true;
}

//------------------------------------------------------------------------------
// getTotalFiles
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ArchiveFileSummary::getTotalFiles() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFileSummary have been set!");
  }
  return m_totalFiles;
}
