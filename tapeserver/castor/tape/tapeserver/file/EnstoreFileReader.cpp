/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/EnstoreFileReader.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor::tape::tapeFile {

EnstoreFileReader::EnstoreFileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall) :
  FileReader(rs, fileToRecall) {
  setPositioningMethod(cta::PositioningMethod::ByFSeq);  // Enstore did not store block IDs
}

void EnstoreFileReader::setPositioningMethod(const cta::PositioningMethod &newMethod) {
  m_positionCommandCode = newMethod;
}

void EnstoreFileReader::positionByFseq(const cta::RetrieveJob &fileToRecall) {
  /* This is a bit tricky since CTA is starts with fSeq=1 before reading the label
     and Enstore store fSeq=1 as the first file AFTER the label
  */

  const auto fSeq = fileToRecall.selectedTapeFile().fSeq;
  if (fSeq < 1) {
    std::stringstream err;
    err << "Unexpected fileId in EnstoreFileReader::positionByFseq fSeq expected >=1, got: "
        << fileToRecall.selectedTapeFile().fSeq << ")";
    throw cta::exception::InvalidArgument(err.str());
  }

  const int64_t fSeq_delta = static_cast<int64_t>(fSeq) - static_cast<int64_t>(m_session.getCurrentFseq());

  if (fSeq == 1) {
    m_session.m_drive.rewind();
    m_session.m_drive.spaceFileMarksForward(1);
  } else if (fSeq_delta == -1) {
      // do nothing we are in the correct place
  } else if (fSeq_delta >= 0) {
    m_session.m_drive.spaceFileMarksForward(static_cast<uint32_t>(fSeq_delta+1));
  } else { //fSeq_delta < 0
    m_session.m_drive.spaceFileMarksBackwards(static_cast<uint32_t>(abs(fSeq_delta)));
    m_session.m_drive.readFileMark("[EnstoreFileReader::position] Reading file mark right before the header of the file we want to read");
  }
  m_session.setCurrentFseq(fSeq);
  setBlockSize(1024*1024);  // Enstore used 1M size blocks for T10K, M8, and LTO-8 tapes
}

void EnstoreFileReader::positionByBlockID(const cta::RetrieveJob &fileToRecall) {
  throw NotImplemented("EnstoreFileReader::positionByBlockID() Cannot be implemented. Enstore did not store block IDs");
}

void EnstoreFileReader::setBlockSize(size_t uiBlockSize) {
  m_currentBlockSize = uiBlockSize;
  if (m_currentBlockSize < 1) {
    std::ostringstream ex_str;
    ex_str << "[EnstoreFileReader::setBlockSize] - Invalid block size detected";
    throw TapeFormatError(ex_str.str());
  }
}

size_t EnstoreFileReader::readNextDataBlock(void *data, const size_t size) {
  if (size != m_currentBlockSize) {
    throw WrongBlockSize();
  }
  size_t bytes_read = 0;
  /*
   * CPIO filter for Enstore/OSM file
   * - caclulate the file size and position of the trailer
   */
  if (size < CPIO::MAXHEADERSIZE) {
    std::ostringstream ex_str;
    ex_str << "Invalid block size: " << size << " - " << "the block size is smaller then max size of a CPIO header: " << CPIO::MAXHEADERSIZE;
    throw TapeFormatError(ex_str.str());
  }
  if (!m_cpioHeader.valid()) {
    size_t uiHeaderSize = 0;
    size_t uiResiduesSize = 0;
    uint8_t* pucTmpData = new uint8_t[size];

    bytes_read = m_session.m_drive.readBlock(pucTmpData, size);
    uiHeaderSize = m_cpioHeader.decode(pucTmpData, size);
    uiResiduesSize = bytes_read - uiHeaderSize;

    // Copy the rest of data to the buffer

    if (uiResiduesSize >= m_cpioHeader.m_ui64FileSize) {
      bytes_read = m_cpioHeader.m_ui64FileSize;
      m_ui64CPIODataSize = bytes_read;
      memcpy(data, pucTmpData + uiHeaderSize, bytes_read);
    } else {
      memcpy(data, pucTmpData + uiHeaderSize, uiResiduesSize);
      bytes_read = uiResiduesSize;
      m_ui64CPIODataSize = bytes_read >= m_cpioHeader.m_ui64FileSize ? m_cpioHeader.m_ui64FileSize : bytes_read;
    }
    delete[] pucTmpData;

  } else {
    bytes_read = m_session.m_drive.readBlock(data, size);
    m_ui64CPIODataSize += bytes_read;

    if (m_ui64CPIODataSize > m_cpioHeader.m_ui64FileSize && bytes_read > 0) {
      // File is ready
      if(bytes_read < (m_ui64CPIODataSize - m_cpioHeader.m_ui64FileSize)) {
	    bytes_read = 0;
      } else {
    	bytes_read = bytes_read - (m_ui64CPIODataSize - m_cpioHeader.m_ui64FileSize);
      }
    }
  }

  // end of file reached!
  if (!bytes_read) {
    m_session.setCurrentFseq(m_session.getCurrentFseq() + 2); // +1 for after the current file, +1 for label being file #1
    m_session.setCurrentFilePart(PartOfFile::Header);
    // the following is a normal day exception: end of files exceptions are thrown at the end of each file being read
    throw EndOfFile();
  }

  return bytes_read;
}

} // namespace castor::tape::tapeFile
