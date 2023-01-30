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

#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "tapeserver/castor/tape/tapeserver/file/CtaFileReader.hpp"
#include "tapeserver/castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

CtaFileReader::CtaFileReader(const std::unique_ptr<ReadSession> &rs, const cta::RetrieveJob &fileToRecall)
  : FileReader(rs, fileToRecall) {
}

void CtaFileReader::positionByFseq(const cta::RetrieveJob &fileToRecall) {
  if (m_session->getCurrentFilePart() != PartOfFile::Header) {
    m_session->setCorrupted();
    throw SessionCorrupted();
  }
  // Make sure the session state is advanced to cover our failures
  // and allow next call to position to discover we failed half way
  m_session->setCurrentFilePart(PartOfFile::HeaderProcessing);

  if (fileToRecall.selectedTapeFile().fSeq < 1) {
    std::ostringstream err;
    err << "Unexpected fileId in FileReader::position with fSeq expected >=1, got: "
        << fileToRecall.selectedTapeFile().fSeq << ")";
    throw cta::exception::InvalidArgument(err.str());
  }

  const int64_t fSeq_delta = static_cast<int64_t>(fileToRecall.selectedTapeFile().fSeq)
                           - static_cast<int64_t>(m_session->getCurrentFseq());
  if (fileToRecall.selectedTapeFile().fSeq == 1) {
    moveToFirstHeaderBlock();
  } else {
    moveReaderByFSeqDelta(fSeq_delta);
  }
  checkHeaders(fileToRecall);
}

void CtaFileReader::positionByBlockID(const cta::RetrieveJob &fileToRecall) {
  // Make sure the session state is advanced to cover our failures
  // and allow next call to position to discover we failed half way
  m_session->setCurrentFilePart(PartOfFile::HeaderProcessing);

  if (fileToRecall.selectedTapeFile().blockId
    > std::numeric_limits<decltype(fileToRecall.selectedTapeFile().blockId)>::max()) {
    std::ostringstream ex_str;
    ex_str << "[FileReader::positionByBlockID] - Block id larger than the supported uint32_t limit: "
           << fileToRecall.selectedTapeFile().blockId;
    throw cta::exception::Exception(ex_str.str());
  }
  useBlockID(fileToRecall);
  checkHeaders(fileToRecall);
}

void CtaFileReader::moveToFirstHeaderBlock() {
  // special case: we can rewind the tape to be faster
  // (TODO: in the future we could also think of a threshold above
  // which we rewind the tape anyway and then space forward)
  m_session->m_drive.rewind();
  VOL1 vol1;
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
    "[FileReader::position] - Reading VOL1");
  try {
    vol1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }
}

void CtaFileReader::checkTrailers() {
  m_session->setCurrentFilePart(PartOfFile::Trailer);

  // let's read and check the trailers
  EOF1 eof1;
  EOF2 eof2;
  UTL1 utl1;
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&eof1), sizeof(eof1),
    "[FileReader::read] - Reading HDR1");
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&eof2), sizeof(eof2),
    "[FileReader::read] - Reading HDR2");
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&utl1), sizeof(utl1),
    "[FileReader::read] - Reading UTL1");
  m_session->m_drive.readFileMark("[FileReader::read] - Reading file mark at the end of file trailer");

  m_session->setCurrentFseq(m_session->getCurrentFseq() + 1);  // moving on to the header of the next file
  m_session->setCurrentFilePart(PartOfFile::Header);

  // the size of the headers is fine, now let's check each header
  try {
    eof1.verify();
    eof2.verify();
    utl1.verify();
  }
  catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }
}

size_t CtaFileReader::readNextDataBlock(void *data, const size_t size) {
  if (size != m_currentBlockSize) {
    throw WrongBlockSize();
  }
  size_t bytes_read = m_session->m_drive.readBlock(data, size);
  // end of file reached! we will keep on reading until we have read the file mark at the end of the trailers
  if (!bytes_read) {
    checkTrailers();
    // the following is a normal day exception: end of files exceptions are thrown at the end of each file being read
    throw EndOfFile();
  }
  return bytes_read;
}

void CtaFileReader::moveReaderByFSeqDelta(const int64_t fSeq_delta) {
  if (fSeq_delta == 0) {
    // do nothing we are in the correct place
  } else if (fSeq_delta > 0) {
    // we need to skip three file marks per file (header, payload, trailer)
    m_session->m_drive.spaceFileMarksForward(static_cast<uint32_t>(fSeq_delta) * 3);
  } else {  // fSeq_delta < 0
    // we need to skip three file marks per file
    // (trailer, payload, header) + 1 to go on the BOT (beginning of tape) side
    // of the file mark before the header of the file we want to read
    m_session->m_drive.spaceFileMarksBackwards(static_cast<uint32_t>(std::abs(fSeq_delta)) * 3 + 1);
    m_session->m_drive.readFileMark(
      "[FileReader::position] Reading file mark right before the header of the file we want to read");
  }
}

void CtaFileReader::useBlockID(const cta::RetrieveJob &fileToRecall) {
  // if we want the first file on tape (fileInfo.blockId==0) we need to skip the VOL1 header
  const uint32_t destination_block = fileToRecall.selectedTapeFile().blockId ?
    fileToRecall.selectedTapeFile().blockId : 1;
  /*
  we position using the sg locate because it is supposed to do the
  right thing possibly in a more optimized way (better than st's
  spaceBlocksForward/Backwards)
    */

  // at this point we should be at the beginning of
  // the headers of the desired file, so now let's check the headers...
  m_session->m_drive.positionToLogicalObject(destination_block);
}

void CtaFileReader::setBlockSize(const UHL1 &uhl1)  {
  m_currentBlockSize = static_cast<size_t>(atol(uhl1.getBlockSize().c_str()));
  if (m_currentBlockSize < 1) {
    std::ostringstream ex_str;
    ex_str << "[FileReader::setBlockSize] - Invalid block size in uhl1 detected";
    throw TapeFormatError(ex_str.str());
  }
}

void CtaFileReader::checkHeaders(const cta::RetrieveJob &fileToRecall) {
  // save the current fSeq into the read session
  m_session->setCurrentFseq(fileToRecall.selectedTapeFile().fSeq);

  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&hdr1), sizeof(hdr1),
    "[FileReader::position] - Reading HDR1");
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&hdr2), sizeof(hdr2),
    "[FileReader::position] - Reading HDR2");
  m_session->m_drive.readExactBlock(reinterpret_cast<void *>(&uhl1), sizeof(uhl1),
    "[FileReader::position] - Reading UHL1");
  m_session->m_drive.readFileMark("[FileReader::position] - Reading file mark at the end of file header");
  // after this we should be where we want, i.e. at the beginning of the file
  m_session->setCurrentFilePart(PartOfFile::Payload);

  // the size of the headers is fine, now let's check each header
  try {
    hdr1.verify();
    hdr2.verify();
    uhl1.verify();
  }
  catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }

  // headers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
  HeaderChecker::checkHDR1(hdr1, fileToRecall, m_session->getVolumeInfo());
  // we disregard hdr2 on purpose as it contains no useful information, we now check the fSeq in uhl1
  // (hdr1 also contains fSeq info but it is modulo 10000, therefore useless)
  HeaderChecker::checkUHL1(uhl1, fileToRecall);
  // now that we are all happy with the information contained within the
  // headers, we finally get the block size for our file (provided it has a reasonable value)
  setBlockSize(uhl1);
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
