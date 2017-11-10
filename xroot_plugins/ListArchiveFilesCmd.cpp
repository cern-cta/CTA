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

#include <iomanip>
#include <sstream>
#include <stdint.h>

namespace cta {
namespace xrootPlugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ListArchiveFilesCmd::ListArchiveFilesCmd(
  XrdOucErrInfo &xrdSfsFileError,
  const bool displayHeader,
  catalogue::ArchiveFileItor archiveFileItor):
  m_xrdSfsFileError(xrdSfsFileError),
  m_displayHeader(displayHeader),
  m_archiveFileItor(std::move(archiveFileItor)) {
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize ListArchiveFilesCmd::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
  try {
    return exceptionThrowingRead(offset, buffer, size);
  } catch(exception::Exception &ex) {
    std::ostringstream errMsg;
    errMsg << __FUNCTION__ << " failed: " << ex.getMessage().str();
    m_xrdSfsFileError.setErrInfo(ECANCELED, errMsg.str().c_str());
    return SFS_ERROR;
  } catch(std::exception &se) {
    std::ostringstream errMsg;
    errMsg << __FUNCTION__ << " failed: " << se.what();
    m_xrdSfsFileError.setErrInfo(ECANCELED, errMsg.str().c_str());
    return SFS_ERROR;
  } catch(...) {
    std::ostringstream errMsg;
    errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
    m_xrdSfsFileError.setErrInfo(ECANCELED, errMsg.str().c_str());
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// exceptionThrowingRead
//------------------------------------------------------------------------------
XrdSfsXferSize ListArchiveFilesCmd::exceptionThrowingRead(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
  if(State::LISTED_LAST_ARCHIVE_FILE == m_state) {
    return SFS_OK;
  }

  if(State::WAITING_FOR_FIRST_READ == m_state) {
    m_state = State::LISTING_ARCHIVE_FILES;

    // The first character of the reply stream is the return code
    m_readBuffer << "0";

    if(m_displayHeader) {
      m_readBuffer <<
        "\x1b[31;1m" << // Change the colour of the output text to red
        std::setfill(' ') << std::setw(7) << std::right << "id" << " " <<
        std::setfill(' ') << std::setw(7) << std::right << "copy no" << " " <<
        std::setfill(' ') << std::setw(7) << std::right << "vid" << " " <<
        std::setfill(' ') << std::setw(7) << std::right << "fseq" << " " <<
        std::setfill(' ') << std::setw(8) << std::right << "block id" << " " <<
        std::setfill(' ') << std::setw(8) << std::right << "instance" << " " <<
        std::setfill(' ') << std::setw(7) << std::right << "disk id" << " " <<
        std::setfill(' ') << std::setw(12) << std::right << "size" << " " <<
        std::setfill(' ') << std::setw(13) << std::right << "checksum type" << " " <<
        std::setfill(' ') << std::setw(14) << std::right << "checksum value" << " " <<
        std::setfill(' ') << std::setw(13) << std::right << "storage class" << " " <<
        std::setfill(' ') << std::setw(8) << std::right << "owner" << " " <<
        std::setfill(' ') << std::setw(8) << std::right << "group" << " " <<
        std::setfill(' ') << std::setw(13) << std::right << "creation time" << " " <<
        "path" <<
        "\x1b[0m\n"; // Return the colour of the output text
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

  XrdSfsXferSize nbBytesToBeReadFromBuffer = (XrdSfsXferSize)m_readBuffer.str().size() - offSetIntoReadBuffer;
  if(0 > nbBytesToBeReadFromBuffer) {
    std::ostringstream errMsg;
    errMsg << "nbBytesToBeReadFromBuffer must be positive: actual=" << nbBytesToBeReadFromBuffer;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  if(nbBytesToBeReadFromBuffer == 0) {
    refreshReadBuffer();
    if (m_readBuffer.str().empty()) {
      m_state = State::LISTED_LAST_ARCHIVE_FILE;
      return SFS_OK;
    }
    m_fileOffsetOfReadBuffer = offset;
    offSetIntoReadBuffer = 0;
    nbBytesToBeReadFromBuffer = m_readBuffer.str().size();
  }

  const XrdSfsXferSize actualNbBytesToRead = nbBytesToBeReadFromBuffer >= size ? size : nbBytesToBeReadFromBuffer;
  if(0 > actualNbBytesToRead) {
    std::ostringstream errMsg;
    errMsg << "actualNbBytesToRead must be positive: actual=" << actualNbBytesToRead;
    m_xrdSfsFileError.setErrInfo(ESPIPE, errMsg.str().c_str());
    return SFS_ERROR;
  }

  strncpy(buffer, m_readBuffer.str().c_str() + offSetIntoReadBuffer, actualNbBytesToRead);
  m_expectedFileOffset += actualNbBytesToRead;

  return actualNbBytesToRead;
}

//------------------------------------------------------------------------------
// refreshReadBuffer
//------------------------------------------------------------------------------
void ListArchiveFilesCmd::refreshReadBuffer() {
  // Note that one archive file may have more than one tape file
  const uint32_t nbArchiveFilesToFetch = 100;
  uint32_t nbFetchedArchiveFiles = 0;

  // Clear the read buffer
  m_readBuffer.str(std::string());

  while(m_archiveFileItor.hasMore() && nbArchiveFilesToFetch > nbFetchedArchiveFiles) {
    const common::dataStructures::ArchiveFile archiveFile = m_archiveFileItor.next();
    nbFetchedArchiveFiles++;
    for(auto copyNbToTapeFile: archiveFile.tapeFiles) {
      const auto copyNb = copyNbToTapeFile.first;
      const common::dataStructures::TapeFile &tapeFile = copyNbToTapeFile.second;
      m_readBuffer <<
        std::setfill(' ') << std::setw(7) << std::right << archiveFile.archiveFileID << " " <<
        std::setfill(' ') << std::setw(7) << std::right << copyNb << " " <<
        std::setfill(' ') << std::setw(7) << std::right << tapeFile.vid << " " <<
        std::setfill(' ') << std::setw(7) << std::right << tapeFile.fSeq << " " <<
        std::setfill(' ') << std::setw(8) << std::right << tapeFile.blockId << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskInstance << " " <<
        std::setfill(' ') << std::setw(7) << std::right << archiveFile.diskFileId << " " <<
        std::setfill(' ') << std::setw(12) << std::right << archiveFile.fileSize << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.checksumType << " " <<
        std::setfill(' ') << std::setw(14) << std::right << archiveFile.checksumValue << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.storageClass << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskFileInfo.owner << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskFileInfo.group << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.creationTime << " " <<
        archiveFile.diskFileInfo.path << "\n";
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
