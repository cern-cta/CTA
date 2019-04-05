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

#include "catalogue/ArchiveFileBuilder.hpp"
#include "common/exception/Exception.hpp"

#include <fstream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFileBuilder::ArchiveFileBuilder(log::Logger &log):
  m_log(log) {
}

//------------------------------------------------------------------------------
// append
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> ArchiveFileBuilder::append(
  const common::dataStructures::ArchiveFile &tapeFile) {

  // If there is currently no ArchiveFile object under construction
  if(nullptr == m_archiveFile.get()) {
    // If the tape file represents an ArchiveFile object with no tape files
    if(tapeFile.tapeFiles.empty()) {
      // Archive file is already complete
      return std::unique_ptr<common::dataStructures::ArchiveFile>(new common::dataStructures::ArchiveFile(tapeFile));
    }

    // If the tape file exists then it must be alone
    if(tapeFile.tapeFiles.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << " failed: Expected exactly one tape file to be appended at a time: actual=" <<
        tapeFile.tapeFiles.size();
      throw ex;
    }

    // Start constructing one
    m_archiveFile.reset(new common::dataStructures::ArchiveFile(tapeFile));

    // There could be more tape files so return incomplete
    return std::unique_ptr<common::dataStructures::ArchiveFile>();
  }

  // If the tape file represents an ArchiveFile object with no tape files
  if(tapeFile.tapeFiles.empty()) {
    // The ArchiveFile object under construction is complete,
    // therefore return it and start the construction of the next
    std::unique_ptr<common::dataStructures::ArchiveFile> tmp;
    tmp = std::move(m_archiveFile);
    m_archiveFile.reset(new common::dataStructures::ArchiveFile(tapeFile));
    return tmp;
  }

  // If the tape file to be appended belongs to the ArchiveFile object
  // currently under construction
  if(tapeFile.archiveFileID == m_archiveFile->archiveFileID) {

    // The tape file must exist and must be alone
    if(tapeFile.tapeFiles.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << " failed: Expected exactly one tape file to be appended at a time: actual=" <<
        tapeFile.tapeFiles.size() << " archiveFileID=" << tapeFile.archiveFileID;
      throw ex;
    }

    // Append the tape file
    m_archiveFile->tapeFiles.push_back(tapeFile.tapeFiles.front());

    // There could be more tape files so return incomplete
    return std::unique_ptr<common::dataStructures::ArchiveFile>();
  }

  // Reaching this point means the tape file to be appended belongs to the next
  // ArchiveFile to be constructed.

  // ArchiveFile object under construction is complete,
  // therefore return it and start the construction of the next
  std::unique_ptr<common::dataStructures::ArchiveFile> tmp;
  tmp = std::move(m_archiveFile);
  m_archiveFile.reset(new common::dataStructures::ArchiveFile(tapeFile));
  return tmp;
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile *ArchiveFileBuilder::getArchiveFile() {
  return m_archiveFile.get();
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void ArchiveFileBuilder::clear() {
  m_archiveFile.reset();
}

} // namespace catalogue
} // namespace cta
