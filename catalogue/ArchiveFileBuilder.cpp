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
    const auto tapeFileMapItor = tapeFile.tapeFiles.begin();
    const auto vid = tapeFileMapItor->second.vid;
    const auto fSeq = tapeFileMapItor->second.fSeq;
    const auto blockId = tapeFileMapItor->second.blockId;
    const auto copyNbOfTapeFileToAppend = tapeFileMapItor->first;
    if(m_archiveFile->tapeFiles.find(copyNbOfTapeFileToAppend) != m_archiveFile->tapeFiles.end()) {
      // Found two tape files for the same archive file with the same copy
      // number

      // Ignore for now any inconsistencies in the copy number of the tape file
      // exception::Exception ex;
      // ex.getMessage() << __FUNCTION__ << " failed: Found two tape files for the same archive file with the same copy"
      //   " numbers: archiveFileID=" << tapeFile.archiveFileID << " copyNb=" << copyNbOfTapeFileToAppend;
      // throw ex;

      // Create a unique copy number to replace the original duplicate
      uint64_t maxCopyNb = 0;
      for(const auto maplet: m_archiveFile->tapeFiles) {
        if(maplet.first > maxCopyNb) {
          maxCopyNb = maplet.first;
        }
      }
      const uint64_t workaroundCopyNb = maxCopyNb + 1;
      {
        std::list<cta::log::Param> params;
        params.push_back(cta::log::Param("archiveFileID", tapeFile.archiveFileID));
        params.push_back(cta::log::Param("duplicateCopyNb", copyNbOfTapeFileToAppend));
        params.push_back(cta::log::Param("workaroundCopyNb", workaroundCopyNb));
        params.push_back(cta::log::Param("tapeVid", vid));
        params.push_back(cta::log::Param("fSeq", fSeq));
        params.push_back(cta::log::Param("blockId", blockId));
        m_log(cta::log::WARNING, "Found a duplicate tape copy number when listing archive files", params);
      }
      m_archiveFile->tapeFiles[workaroundCopyNb] = tapeFileMapItor->second;
    } else {
      m_archiveFile->tapeFiles[copyNbOfTapeFileToAppend] = tapeFileMapItor->second;
    }

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
