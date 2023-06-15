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

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/file/FileWriter.hpp"
#include "castor/tape/tapeserver/file/WriteSession.hpp"
#include "scheduler/ArchiveJob.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

FileWriter::FileWriter(const std::unique_ptr<WriteSession>& ws,
                       const cta::ArchiveJob& fileToMigrate,
                       const size_t blockSize) :
m_currentBlockSize(blockSize),
m_session(ws),
m_fileToMigrate(fileToMigrate),
m_open(false),
m_nonzeroFileWritten(false),
m_numberOfBlocks(0) {
  // Check the sanity of the parameters. fSeq should be >= 1
  if (0 == m_fileToMigrate.archiveFile.archiveFileID || m_fileToMigrate.tapeFile.fSeq < 1) {
    std::ostringstream err;
    err << "Unexpected fileId in FileWriter::FileWriter (expected != 0, got: "
        << m_fileToMigrate.archiveFile.archiveFileID
        << ") or fSeq (expected >=1, got: " << m_fileToMigrate.tapeFile.fSeq << ")";
    throw cta::exception::InvalidArgument(err.str());
  }
  if (m_session->isCorrupted()) {
    throw SessionCorrupted();
  }
  m_session->lock();
  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  std::stringstream s;
  s << std::hex << m_fileToMigrate.archiveFile.archiveFileID;
  std::string fileId;
  s >> fileId;
  std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
  hdr1.fill(fileId, m_session->m_vid, m_fileToMigrate.tapeFile.fSeq);
  hdr2.fill(m_currentBlockSize, m_session->m_compressionEnabled);
  uhl1.fill(m_fileToMigrate.tapeFile.fSeq, m_currentBlockSize, m_session->getSiteName(), m_session->getHostName(),
            m_session->m_drive.getDeviceInfo());
  /* Before writing anything, we record the blockId of the file */
  if (1 == m_fileToMigrate.tapeFile.fSeq) {
    m_blockId = 0;
  }
  else {
    m_blockId = getPosition();
  }
  m_session->m_drive.writeBlock(&hdr1, sizeof(hdr1));
  m_session->m_drive.writeBlock(&hdr2, sizeof(hdr2));
  m_session->m_drive.writeBlock(&uhl1, sizeof(uhl1));
  m_session->m_drive.writeImmediateFileMarks(1);
  m_open = true;
  m_LBPMode = m_session->getLBPMode();
}

uint32_t FileWriter::getPosition() {
  return m_session->m_drive.getPositionInfo().currentPosition;
}

uint32_t FileWriter::getBlockId() {
  return m_blockId;
}

size_t FileWriter::getBlockSize() {
  return m_currentBlockSize;
}

void FileWriter::write(const void* data, const size_t size) {
  m_session->m_drive.writeBlock(data, size);
  if (size > 0) {
    m_nonzeroFileWritten = true;
    m_numberOfBlocks++;
  }
}

void FileWriter::close() {
  if (!m_open) {
    m_session->setCorrupted();
    throw FileClosedTwice();
  }
  if (!m_nonzeroFileWritten) {
    m_session->setCorrupted();
    throw ZeroFileWritten();
  }
  m_session->m_drive.writeImmediateFileMarks(1);  // filemark at the end the of data file
  EOF1 eof1;
  EOF2 eof2;
  UTL1 utl1;
  std::stringstream s;
  s << std::hex << m_fileToMigrate.archiveFile.archiveFileID;
  std::string fileId;
  s >> fileId;
  std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
  eof1.fill(fileId, m_session->m_vid, m_fileToMigrate.tapeFile.fSeq, m_numberOfBlocks);
  eof2.fill(m_currentBlockSize, m_session->m_compressionEnabled);
  utl1.fill(m_fileToMigrate.tapeFile.fSeq, m_currentBlockSize, m_session->getSiteName(), m_session->getHostName(),
            m_session->m_drive.getDeviceInfo());
  m_session->m_drive.writeBlock(&eof1, sizeof(eof1));
  m_session->m_drive.writeBlock(&eof2, sizeof(eof2));
  m_session->m_drive.writeBlock(&utl1, sizeof(utl1));
  m_session->m_drive.writeImmediateFileMarks(1);  // filemark at the end the of trailers
  m_open = false;
}

FileWriter::~FileWriter() throw() {
  if (m_open) {
    m_session->setCorrupted();
  }
  m_session->release();
}

std::string FileWriter::getLBPMode() {
  return m_LBPMode;
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
