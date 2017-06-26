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

#include "xroot_plugins/ListArchiveFilesCmd.hpp"

#include <sstream>
#include <stdint.h>

namespace cta {
namespace xrootPlugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ListArchiveFilesCmd::ListArchiveFilesCmd(
  log::Logger &log,
  XrdOucErrInfo &xrdSfsFileError,
  const bool displayHeader,
  const catalogue::TapeFileSearchCriteria &searchCriteria,
  catalogue::Catalogue &catalogue):
  m_log(log),
  m_xrdSfsFileError(xrdSfsFileError),
  m_displayHeader(displayHeader),
  m_archiveFileItor(catalogue.getArchiveFileItor(searchCriteria)) {
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize ListArchiveFilesCmd::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
  if(State::LISTED_LAST_ARCHIVE_FILE == m_state) {
    return SFS_OK;
  }

  if(State::WAITING_FOR_FIRST_READ == m_state) {
    m_state = State::LISTING_ARCHIVE_FILES;

    // The first character of the reply stream is the return code
    m_readBuffer = "0";

    if(m_displayHeader) {
      m_readBuffer += "id  copy no  vid  fseq  block id  instance  disk id  size  checksum type  checksum value  "
        "storage class  owner  group  path  creation time\n";
    }
  }

  if(offset != m_expectedFileOffset) {
    std::ostringstream errMsg;
    errMsg << "Unexpected read offset: expected=" << m_expectedFileOffset << " actual=" << offset;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  XrdSfsXferSize offSetIntoReadBuffer = offset - m_fileOffsetOfReadBuffer;
  if(0 > offSetIntoReadBuffer) {
    std::ostringstream errMsg;
    errMsg << "offSetIntoReadBuffer must be positive: actual=" << offSetIntoReadBuffer;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  XrdSfsXferSize nbBytesToBeReadFromBuffer = (XrdSfsXferSize)m_readBuffer.size() - offSetIntoReadBuffer;
  if(0 > nbBytesToBeReadFromBuffer) {
    std::ostringstream errMsg;
    errMsg << "nbBytesToBeReadFromBuffer must be positive: actual=" << nbBytesToBeReadFromBuffer;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  if(nbBytesToBeReadFromBuffer == 0) {
    refreshReadBuffer();
    if (m_readBuffer.empty()) {
      m_state = State::LISTED_LAST_ARCHIVE_FILE;
      return SFS_OK;
    }
    m_fileOffsetOfReadBuffer = offset;
    offSetIntoReadBuffer = 0;
    nbBytesToBeReadFromBuffer = m_readBuffer.size();
  }

  const XrdSfsXferSize actualNbBytesToRead = nbBytesToBeReadFromBuffer >= size ? size : nbBytesToBeReadFromBuffer;
  if(0 > actualNbBytesToRead) {
    std::ostringstream errMsg;
    errMsg << "actualNbBytesToRead must be positive: actual=" << actualNbBytesToRead;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  strncpy(buffer, m_readBuffer.c_str() + offSetIntoReadBuffer, actualNbBytesToRead);
  m_expectedFileOffset += actualNbBytesToRead;

  return actualNbBytesToRead;
}

//------------------------------------------------------------------------------
// refreshReadBuffer
//------------------------------------------------------------------------------
void ListArchiveFilesCmd::refreshReadBuffer() {
  m_readBuffer.clear();

  if(m_archiveFileItor->hasMore()) {
    const common::dataStructures::ArchiveFile archiveFile = m_archiveFileItor->next();
    for(auto copyNbToTapeFile: archiveFile.tapeFiles) {
      const auto copyNb = copyNbToTapeFile.first;
      const common::dataStructures::TapeFile &tapeFile = copyNbToTapeFile.second;
      m_readBuffer +=
        std::to_string((unsigned long long) archiveFile.archiveFileID) + " " +
        std::to_string((unsigned long long) copyNb) + " " +
        tapeFile.vid + " " +
        std::to_string((unsigned long long) tapeFile.fSeq) + " " +
        std::to_string((unsigned long long) tapeFile.blockId) + " " +
        archiveFile.diskInstance + " " +
        archiveFile.diskFileId + " " +
        std::to_string((unsigned long long) archiveFile.fileSize) + " " +
        archiveFile.checksumType + " " +
        archiveFile.checksumValue + " " +
        archiveFile.storageClass + " " +
        archiveFile.diskFileInfo.owner + " " +
        archiveFile.diskFileInfo.group + " " +
        archiveFile.diskFileInfo.path + " " +
        std::to_string((unsigned long long) archiveFile.creationTime) + "\n";
    }
  }
}

//------------------------------------------------------------------------------
// stateToStr
//------------------------------------------------------------------------------
std::string ListArchiveFilesCmd::stateToStr(const State state) const {
  switch(state) {
  case WAITING_FOR_FIRST_READ:
    return "WAITING_FOR_FIRST_READ";
  case LISTING_ARCHIVE_FILES:
    return "LISTING_ARCHIVE_FILES";
  case LISTED_LAST_ARCHIVE_FILE:
    return "LISTED_LAST_ARCHIVE_FILE";
  default:
    return "UNKNOWN";
  }
}

} // namespace xrootPlugins
} // namespace cta
