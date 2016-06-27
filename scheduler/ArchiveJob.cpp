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

#include "scheduler/ArchiveJob.hpp"
#include <limits>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveJob::~ArchiveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(ArchiveMount &mount,
  catalogue::Catalogue & catalogue,
  const common::dataStructures::ArchiveFile &archiveFile,
  const RemotePathAndStatus &remotePathAndStatus,
  const common::dataStructures::TapeFile &tapeFile):
  m_mount(mount), m_catalogue(catalogue),
  archiveFile(archiveFile),
  remotePathAndStatus(remotePathAndStatus),
  tapeFile(tapeFile) {}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::ArchiveJob::complete() {
  // First check that the block Id for the file has been set.
  if (tapeFile.blockId ==
      std::numeric_limits<decltype(tapeFile.blockId)>::max())
    throw BlockIdNotSet("In cta::ArchiveJob::complete(): Block ID not set");
  // Also check the checksum has been set
  if (!archiveFile.checksumType.size() || !archiveFile.checksumValue.size())
    throw ChecksumNotSet("In cta::ArchiveJob::complete(): checksum not set");
  // We are good to go to record the data in the persistent storage.
  // First make the file safe on tape.
  m_dbJob->bumpUpTapeFileCount(tapeFile.fSeq);
  // Now record the data in the archiveNS. The checksum will be validated if already
  // present, of inserted if not.
  catalogue::TapeFileWritten fileReport;
  fileReport.archiveFileId = archiveFile.archiveFileID;
  fileReport.blockId = tapeFile.blockId;
  fileReport.checksumType = tapeFile.checksumType;
  fileReport.checksumValue = tapeFile.checksumValue;
  fileReport.compressedSize = tapeFile.compressedSize;
  fileReport.copyNb = tapeFile.copyNb;
  //TODO fileReport.diskFileGroup
  //TODO fileReport.diskFilePath
  //TODO fileReport.diskFileRecoveryBlob
  //TODO fileReport.diskFileUser
  //TODO fileReport.diskInstance
  fileReport.fSeq = tapeFile.fSeq;
  fileReport.size = archiveFile.fileSize;
  //TODO fileReport.storageClassName
  fileReport.vid = tapeFile.vid;
  //TODO fileReport.tapeDrive
  m_catalogue.fileWrittenToTape(fileReport);
  //m_ns.addTapeFile(SecurityIdentity(UserIdentity(std::numeric_limits<uint32_t>::max(), 
  //  std::numeric_limits<uint32_t>::max()), ""), archiveFile.fileId, nameServerTapeFile);
  // We can now record the success for the job in the database
  m_dbJob->succeed();
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::ArchiveJob::failed(const cta::exception::Exception &ex) {
  m_dbJob->fail();
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::ArchiveJob::retry() {
  throw std::runtime_error("cta::ArchiveJob::retry(): not implemented");
}
