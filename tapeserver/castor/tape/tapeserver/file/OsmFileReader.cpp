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
#include "castor/tape/tapeserver/file/OsmFileReader.hpp"
#include "castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/CRC.hpp"

namespace castor::tape::tapeFile {

void OsmFileReader::positionByFseq(const cta::RetrieveJob &fileToRecall) {
  if (m_session.getCurrentFilePart() != PartOfFile::Header) {
    m_session.setCorrupted();
    throw SessionCorrupted();
  }
  // Make sure the session state is advanced to cover our failures
  // and allow next call to position to discover we failed half way
  m_session.setCurrentFilePart(PartOfFile::HeaderProcessing);

  if (fileToRecall.selectedTapeFile().fSeq < 1) {
    std::ostringstream err;
    err << "Unexpected fileId in FileReader::position with fSeq expected >=1, got: "
        << fileToRecall.selectedTapeFile().fSeq << ")";
    throw cta::exception::InvalidArgument(err.str());
  }

  const int64_t fSeq_delta = static_cast<int64_t>(fileToRecall.selectedTapeFile().fSeq)
                           - static_cast<int64_t>(m_session.getCurrentFseq());
  if(fileToRecall.selectedTapeFile().fSeq == 1) { 
    moveToFirstFile();
  } else {
    moveReaderByFSeqDelta(fSeq_delta);
  }
  m_session.setCurrentFilePart(PartOfFile::Payload);
  setBlockSize(PAYLOAD_BOLCK_SIZE);
}

void OsmFileReader::positionByBlockID(const cta::RetrieveJob &fileToRecall) {
  // Make sure the session state is advanced to cover our failures
  // and allow next call to position to discover we failed half way
  m_session.setCurrentFilePart(PartOfFile::HeaderProcessing);

  if (fileToRecall.selectedTapeFile().blockId
    > std::numeric_limits<decltype(fileToRecall.selectedTapeFile().blockId)>::max()) {
    std::ostringstream ex_str;
    ex_str << "[FileReader::positionByBlockID] - Block id larger than the supported uint32_t limit: "
           << fileToRecall.selectedTapeFile().blockId;
    throw cta::exception::Exception(ex_str.str());
  }
  useBlockID(fileToRecall);
  m_session.setCurrentFilePart(PartOfFile::Payload);
  setBlockSize(PAYLOAD_BOLCK_SIZE);
}

void OsmFileReader::moveToFirstFile() {
  // special case: we can rewind the tape to be faster
  // (TODO: in the future we could also think of a threshold above
  // which we rewind the tape anyway and then space forward)
  m_session.m_drive.rewind();
  osm::LABEL osmLabel;
  m_session.m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()), osm::LIMITS::MAXMRECSIZE, "[FileReader::position] - Reading OSM label - part 1");
  m_session.m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE), osm::LIMITS::MAXMRECSIZE, "[FileReader::position] - Reading OSM label - part 2");
  try {
    osmLabel.decode();
  } catch(const std::exception& osmExc) {
    throw TapeFormatError(osmExc.what());
  }
  m_session.m_drive.readFileMark("[FileReader::position] Reading file mark right before the header of the file we want to read");
}

void OsmFileReader::moveReaderByFSeqDelta(const int64_t fSeq_delta) {
  if (fSeq_delta == 0) {
    // do nothing we are in the correct place
  } else if (fSeq_delta > 0) {
    // we need to skip one file mark per file
    m_session.m_drive.spaceFileMarksForward(static_cast<uint32_t>(fSeq_delta+1));
  } else {  // fSeq_delta < 0
    // we need to skip one file mark per file
    // to go on the BOT (beginning of tape) side
    // of the file mark before the header of the file we want to read
    m_session.m_drive.spaceFileMarksBackwards(static_cast<uint32_t>(abs(fSeq_delta)+1));
    m_session.m_drive.readFileMark(
      "[FileReader::position] Reading file mark right before the header of the file we want to read");
  }
}

void OsmFileReader::useBlockID(const cta::RetrieveJob &fileToRecall) {
  // if we want the first file on tape (fileInfo.blockId < 2) we need to skip 2 blocks of OSM header 
  const uint32_t destination_block = fileToRecall.selectedTapeFile().blockId > 2 ? 
    fileToRecall.selectedTapeFile().blockId : 3;

  /*
  we position using the sg locate because it is supposed to do the
  right thing possibly in a more optimized way (better than st's
  spaceBlocksForward/Backwards)
    */

  // at this point we should be at the beginning of
  // the headers of the desired file, so now let's check the headers...
  m_session.m_drive.positionToLogicalObject(destination_block);
}

void OsmFileReader::setBlockSize(size_t uiBlockSize) {
  m_currentBlockSize = uiBlockSize;
  if (m_currentBlockSize < 1) {
    std::ostringstream ex_str;
    ex_str << "[FileReader::setBlockSize] - Invalid block size detected";
    throw TapeFormatError(ex_str.str());
  }
}

size_t OsmFileReader::readNextDataBlock(void *data, const size_t size) {
  if (size != m_currentBlockSize) {
    throw WrongBlockSize();
  }
  size_t bytes_read = 0;
  /*
   * Cpio filter for OSM file
   * - caclulate the file size and position of the trailer
   */
  if (size < CPIO::MAXHEADERSIZE) {
    std::ostringstream ex_str;
    ex_str << "Invalid block size: " << size << " - "
           << "the block size is smaller then max size of a CPIO header: "
           << CPIO::MAXHEADERSIZE;
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
    //bytes_read = m_session.m_drive.readBlock(data, size);
    // Special case - 64K data format with CRC32
    if (!m_b64KFormat) {
      bytes_read = m_session.m_drive.readBlock(data, size);
    } else {
      bytes_read = m_session.m_drive.readBlock(data, 65536);
    }

//    if (m_session.m_drive.getLbpToUse() == tapeserver::drive::lbpToUse::disabled) {
      if (bytes_read - SCSI::logicBlockProtectionMethod::CRC32CLength > 0 && bytes_read <= PAYLOAD_BOLCK_SIZE_64K_FORMAT
            && !m_b64KFormat) {
        // Checking if the data block is with CRC32
        if (cta::verifyCrc32cForMemoryBlockWithCrc32c(
              SCSI::logicBlockProtectionMethod::CRC32CSeed, bytes_read, static_cast<const uint8_t*>(data))) {
          bytes_read -= SCSI::logicBlockProtectionMethod::CRC32CLength;
          m_b64KFormat = true;
//          m_session.m_drive.enableCRC32CLogicalBlockProtectionReadOnly();
        } else {
          throw TapeFormatError("OSM 64KFormat Error");
        }
      }
    }
    m_ui64CPIODataSize += bytes_read;
    if (m_ui64CPIODataSize > m_cpioHeader.m_ui64FileSize && bytes_read > 0) {
      // File is ready
      if(bytes_read < (m_ui64CPIODataSize - m_cpioHeader.m_ui64FileSize)) {
        bytes_read = 0;
      } else {
        // size_t uiOldSize = bytes_read;
        bytes_read = bytes_read - (m_ui64CPIODataSize - m_cpioHeader.m_ui64FileSize);
        // m_ui64CPIODataSize += bytes_read - uiOldSize;
      }
    }
  }

  // end of file reached! keep reading until the header of the next file
  if (!bytes_read) {
//    if (m_b64KFormat) {
//      m_session.m_drive.disableLogicalBlockProtection();
//    }
    m_session.setCurrentFseq(m_session.getCurrentFseq() + 1); // moving on to the header of the next file
    m_session.setCurrentFilePart(PartOfFile::Header);
    // the following is a normal day exception: end of files exceptions are thrown at the end of each file being read
    throw EndOfFile();
  }

  return bytes_read;
}

} // namespace castor::tape::tapeFile
