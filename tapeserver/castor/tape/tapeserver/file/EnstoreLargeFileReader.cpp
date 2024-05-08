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
#include "castor/tape/tapeserver/file/EnstoreLargeFileReader.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor::tape::tapeFile {

EnstoreLargeFileReader::EnstoreLargeFileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall) :
FileReader(rs, fileToRecall) {
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
    err << std::string(__FUNCTION__) << ": "
        << "Unexpected fileId. fSeq expected >=1, got: " << fileToRecall.selectedTapeFile().fSeq << ")";
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
    ex_str << std::string(__FUNCTION__) << ": "
           << "Invalid block size in uhl1 detected";
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
  return bytes_read;
}

void EnstoreLargeFileReader::moveToFirstHeaderBlock() {
  throw NotImplemented("EnstoreLargeFileReader::moveToFirstHeaderBlock() Not implemented.");
}

void EnstoreLargeFileReader::moveReaderByFSeqDelta(const int64_t fSeq_delta) {
  if (fSeq_delta == 0) {
    // do nothing we are in the correct place
  }
  else if (fSeq_delta > 0) {
    // we need to skip three file marks per file (header, payload, trailer)
    m_session.m_drive.spaceFileMarksForward(static_cast<uint32_t>(fSeq_delta) * 3);
  }
  else {  // fSeq_delta < 0
    // we need to skip three file marks per file
    // (trailer, payload, header) + 1 to go on the BOT (beginning of tape) side
    // of the file mark before the header of the file we want to read
    m_session.m_drive.spaceFileMarksBackwards(static_cast<uint32_t>(std::abs(fSeq_delta)) * 3 + 1);
    m_session.m_drive.readFileMark(std::string(__FUNCTION__) + ": " +
                                   "Reading file mark right before the header of the file we want to read");
  }
}

void EnstoreLargeFileReader::checkTrailers() {
  m_session.setCurrentFilePart(PartOfFile::Trailer);

  // let's read and check the trailers

  size_t blockSize = 256 * 1024;
  char* data = new char[blockSize + 1];
  size_t bytes_read = m_session.m_drive.readBlock(data, blockSize);

  EOF1 eof1;
  EOF2 eof2;
  UTL1 utl1;

  if (bytes_read < sizeof(eof1) + sizeof(eof2) + sizeof(utl1)) {
    throw cta::exception::Exception(std::string(__FUNCTION__) +
                                    " failed: Too few bytes read for headers:" + std::to_string(bytes_read));
  }
  memcpy(&eof1, data, sizeof(eof1));
  memcpy(&eof2, data + sizeof(eof1), sizeof(eof2));
  memcpy(&utl1, data + sizeof(eof1) + sizeof(eof2), sizeof(utl1));
  delete[] data;

  m_session.m_drive.readFileMark(std::string(__FUNCTION__) + ": " + "Reading file mark at the end of file header");

  m_session.setCurrentFseq(m_session.getCurrentFseq() + 1);  // moving on to the header of the next file
  m_session.setCurrentFilePart(PartOfFile::Header);

  // the size of the headers is fine, now let's check each header
  try {
    eof1.verify(true);  // Skip certain field where enstore and CTA don't match
    eof2.verify("D");  
    utl1.verify();
  }
  catch (std::exception& e) {
    throw TapeFormatError(e.what());
  }
}

void EnstoreLargeFileReader::checkHeaders(const cta::RetrieveJob& fileToRecall) {
  // save the current fSeq into the read session
  m_session.setCurrentFseq(fileToRecall.selectedTapeFile().fSeq);

  size_t blockSize = 256 * 1024;
  char* data = new char[blockSize + 1];
  size_t bytes_read = m_session.m_drive.readBlock(data, blockSize);

  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;

  if (bytes_read < sizeof(hdr1) + sizeof(hdr2) + sizeof(uhl1)) {
    throw cta::exception::Exception(std::string(__FUNCTION__) +
                                    " failed: Too few bytes read for headers:" + std::to_string(bytes_read));
  }
  memcpy(&hdr1, data, sizeof(hdr1));
  memcpy(&hdr2, data + sizeof(hdr1), sizeof(hdr1));
  memcpy(&uhl1, data + sizeof(hdr1) + sizeof(hdr2), sizeof(uhl1));
  delete[] data;

  m_session.m_drive.readFileMark(std::string(__FUNCTION__) + ": " + "Reading file mark at the end of file header");
  // after this we should be where we want, i.e. at the beginning of the file
  m_session.setCurrentFilePart(PartOfFile::Payload);

  // the size of the headers is fine, now let's check each header
  // Extend these with skipped things for Fermilab tapes
  try {
    hdr1.verify(true);  // Skip certain field where enstore and CTA don't match
    hdr2.verify("D");  
    uhl1.verify();
  }
  catch (std::exception& e) {
    throw TapeFormatError(e.what());
  }

  // This differs from CTA checkHeaders in that none of the checks in checkHDR1 or checkUHL1
  // works for these Enstore tapes. We skip this entirely but get the block size out of UHL1
  setBlockSize(uhl1);
}

}  // namespace castor::tape::tapeFile
