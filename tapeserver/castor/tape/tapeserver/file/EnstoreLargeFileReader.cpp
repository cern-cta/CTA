/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/EnstoreLargeFileReader.hpp"

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "scheduler/RetrieveJob.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>

namespace castor::tape::tapeFile {

EnstoreLargeFileReader::EnstoreLargeFileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall)
    : FileReader(rs, fileToRecall) {
  setPositioningMethod(cta::PositioningMethod::ByFSeq);  // Enstore did not store block IDs
}

void EnstoreLargeFileReader::setPositioningMethod(const cta::PositioningMethod& newMethod) {
  m_positionCommandCode = newMethod;
}

void EnstoreLargeFileReader::positionByFseq(const cta::RetrieveJob& fileToRecall) {
  if (m_session.getCurrentFilePart() != PartOfFile::Header) {
    m_session.setCorrupted();
    throw SessionCorrupted();
  }
  // Make sure the session state is advanced to cover our failures
  // and allow next call to position to discover we failed half way
  m_session.setCurrentFilePart(PartOfFile::HeaderProcessing);

  if (fileToRecall.selectedTapeFile().fSeq < 1) {
    std::ostringstream err;
    err << "Unexpected fileId. fSeq expected >=1, got: " << fileToRecall.selectedTapeFile().fSeq << ")";
    throw cta::exception::InvalidArgument(err.str());
  }

  const int64_t fSeq_delta =
    static_cast<int64_t>(fileToRecall.selectedTapeFile().fSeq) - static_cast<int64_t>(m_session.getCurrentFseq());
  moveReaderByFSeqDelta(fSeq_delta);  // Let's just try this
  checkHeaders(fileToRecall);
}

void EnstoreLargeFileReader::positionByBlockID(const cta::RetrieveJob& fileToRecall) {
  throw NotImplemented(
    "EnstoreLargeFileReader::positionByBlockID() Cannot be implemented. Enstore did not store block IDs");
}

void EnstoreLargeFileReader::setBlockSize(const UHL1& uhl1) {  // Could inherit
  m_currentBlockSize = static_cast<size_t>(atol(uhl1.getBlockSize().c_str()));
  if (m_currentBlockSize < 1) {
    std::ostringstream ex_str;
    ex_str << "Invalid block size in uhl1 detected";
    throw TapeFormatError(ex_str.str());
  }
}

void EnstoreLargeFileReader::setTargetFileSize(const UHL2& uhl2) {  // Could inherit
  bytes_to_read = static_cast<size_t>(atol(uhl2.getTargetFileSize().c_str()));
  if (bytes_to_read < 1) {
    std::ostringstream ex_str;
    ex_str << "Invalid file size in uhl2 detected";
    throw TapeFormatError(ex_str.str());
  }
}

size_t EnstoreLargeFileReader::readNextDataBlock(void* data, const size_t size) {
  if (size != m_currentBlockSize) {
    throw WrongBlockSize();
  }
  size_t bytes_read = m_session.m_drive.readBlock(data, size);
  // end of file reached! we will keep on reading until we have read the file mark at the end of the trailers
  if (!bytes_read) {
    checkTrailers();
    // the following is a normal day exception: end of files exceptions are thrown at the end of each file being read
    throw EndOfFile();
  }
  // Make sure we don't read any extra data (EnstoreLarge files are 0 padded)
  if (bytes_read > bytes_to_read) {
    bytes_read = bytes_to_read;
  } else {
    bytes_to_read -= bytes_read;
  }

  return bytes_read;
}

void EnstoreLargeFileReader::moveToFirstHeaderBlock() {
  throw cta::exception::NotImplementedException();
}

void EnstoreLargeFileReader::moveReaderByFSeqDelta(const int64_t fSeq_delta) {
  if (fSeq_delta == 0) {
    // do nothing we are in the correct place
  } else if (fSeq_delta > 0) {
    // we need to skip three file marks per file (header, payload, trailer)
    m_session.m_drive.spaceFileMarksForward(static_cast<uint32_t>(fSeq_delta) * 3);
  } else {  // fSeq_delta < 0
    // we need to skip three file marks per file
    // (trailer, payload, header) + 1 to go on the BOT (beginning of tape) side
    // of the file mark before the header of the file we want to read
    m_session.m_drive.spaceFileMarksBackwards(static_cast<uint32_t>(std::abs(fSeq_delta)) * 3 + 1);
    m_session.m_drive.readFileMark("Reading file mark right before the header of the file we want to read");
  }
}

void EnstoreLargeFileReader::checkTrailers() {
  m_session.setCurrentFilePart(PartOfFile::Trailer);

  // let's read and check the trailers

  size_t blockSize = 256 * 1024;
  auto data = std::make_unique<char[]>(blockSize + 1);
  size_t bytes_read = m_session.m_drive.readBlock(data.get(), blockSize);

  EOF1 eof1;
  EOF2 eof2;
  UTL1 utl1;

  if (bytes_read < sizeof(eof1) + sizeof(eof2) + sizeof(utl1)) {
    throw cta::exception::Exception("Too few bytes read for headers:" + std::to_string(bytes_read));
  }
  memcpy(&eof1, data.get(), sizeof(eof1));
  memcpy(&eof2, data.get() + sizeof(eof1), sizeof(eof2));
  memcpy(&utl1, data.get() + sizeof(eof1) + sizeof(eof2), sizeof(utl1));

  m_session.m_drive.readFileMark("Reading file mark at the end of file header");

  m_session.setCurrentFseq(m_session.getCurrentFseq() + 1);  // moving on to the header of the next file
  m_session.setCurrentFilePart(PartOfFile::Header);

  // the size of the headers is fine, now let's check each header
  try {
    eof1.verify(true);  // Skip certain field where enstore and CTA don't match
    eof2.verify("D");
    utl1.verify();
  } catch (std::exception& e) {
    throw TapeFormatError(e.what());
  }
}

void EnstoreLargeFileReader::checkHeaders(const cta::RetrieveJob& fileToRecall) {
  // save the current fSeq into the read session
  m_session.setCurrentFseq(fileToRecall.selectedTapeFile().fSeq);

  size_t blockSize = 256 * 1024;
  auto data = std::make_unique<char[]>(blockSize + 1);
  size_t bytes_read = m_session.m_drive.readBlock(data.get(), blockSize);

  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  UHL2 uhl2;

  if (bytes_read < sizeof(hdr1) + sizeof(hdr2) + sizeof(uhl1)) {
    throw cta::exception::Exception("Too few bytes read for headers:" + std::to_string(bytes_read));
  }
  memcpy(&hdr1, data.get(), sizeof(hdr1));
  memcpy(&hdr2, data.get() + sizeof(hdr1), sizeof(hdr1));
  memcpy(&uhl1, data.get() + sizeof(hdr1) + sizeof(hdr2), sizeof(uhl1));
  memcpy(&uhl2, data.get() + sizeof(hdr1) + sizeof(hdr2) + sizeof(uhl1), sizeof(uhl2));

  m_session.m_drive.readFileMark("Reading file mark at the end of file header");
  // after this we should be where we want, i.e. at the beginning of the file
  m_session.setCurrentFilePart(PartOfFile::Payload);

  // the size of the headers is fine, now let's check each header
  // Extend these with skipped things for Fermilab tapes
  try {
    hdr1.verify(true);  // Skip certain field where enstore and CTA don't match
    hdr2.verify("D");
    uhl1.verify();
    uhl2.verify();
  } catch (std::exception& e) {
    throw TapeFormatError(e.what());
  }

  // This differs from CTA checkHeaders in that none of the checks in checkHDR1 or checkUHL1
  // works for these Enstore tapes. We skip this entirely but get the block size out of UHL1
  setBlockSize(uhl1);
  setTargetFileSize(uhl2);
}

}  // namespace castor::tape::tapeFile
