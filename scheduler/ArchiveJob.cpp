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
#include "scheduler/ArchiveMount.hpp"
#include "disk/DiskReporterFactory.hpp"
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
  const std::string &srcURL,
  const common::dataStructures::TapeFile &tapeFile):
  m_mount(mount), m_catalogue(catalogue),
  archiveFile(archiveFile),
  srcURL(srcURL),
  tapeFile(tapeFile) {}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
bool cta::ArchiveJob::complete() {
  // First check that the block Id for the file has been set.
  if (tapeFile.blockId ==
      std::numeric_limits<decltype(tapeFile.blockId)>::max())
    throw BlockIdNotSet("In cta::ArchiveJob::complete(): Block ID not set");
  // Also check the checksum has been set
  if (archiveFile.checksumType.empty() || archiveFile.checksumValue.empty() || 
      tapeFile.checksumType.empty() || tapeFile.checksumValue.empty())
    throw ChecksumNotSet("In cta::ArchiveJob::complete(): checksums not set");
  // And matches
  if (archiveFile.checksumType != tapeFile.checksumType || 
      archiveFile.checksumValue != tapeFile.checksumValue)
    throw ChecksumMismatch(std::string("In cta::ArchiveJob::complete(): checksum mismatch!")
            +" Archive file checksum type: "+archiveFile.checksumType
            +" Archive file checksum value: "+archiveFile.checksumValue
            +" Tape file checksum type: "+tapeFile.checksumType
            +" Tape file checksum value: "+tapeFile.checksumValue);
  // We are good to go to record the data in the persistent storage.
  // Record the data in the archiveNS. The checksum will be validated if already
  // present, of inserted if not.
  catalogue::TapeFileWritten fileReport;
  fileReport.archiveFileId = archiveFile.archiveFileID;
  fileReport.blockId = tapeFile.blockId;
  fileReport.checksumType = tapeFile.checksumType;
  fileReport.checksumValue = tapeFile.checksumValue;
  fileReport.compressedSize = tapeFile.compressedSize;
  fileReport.copyNb = tapeFile.copyNb;
  fileReport.diskFileId = archiveFile.diskFileId;
  fileReport.diskFileUser = archiveFile.diskFileInfo.owner;
  fileReport.diskFileGroup = archiveFile.diskFileInfo.group;
  fileReport.diskFilePath = archiveFile.diskFileInfo.path;
  fileReport.diskFileRecoveryBlob = archiveFile.diskFileInfo.recoveryBlob;
  fileReport.diskInstance = archiveFile.diskInstance;
  fileReport.fSeq = tapeFile.fSeq;
  fileReport.size = archiveFile.fileSize;
  fileReport.storageClassName = archiveFile.storageClass;
  fileReport.tapeDrive = m_mount.getDrive();
  fileReport.vid = tapeFile.vid;
  m_catalogue.filesWrittenToTape (std::list<catalogue::TapeFileWritten>{fileReport});
  //m_ns.addTapeFile(SecurityIdentity(UserIdentity(std::numeric_limits<uint32_t>::max(), 
  //  std::numeric_limits<uint32_t>::max()), ""), archiveFile.fileId, nameServerTapeFile);
  // We will now report the successful archival to the EOS instance.
  // if  TODO TODO
  // We can now record the success for the job in the database.
  // If this is the last job of the request, we also report the success to the client.
  if (m_dbJob->succeed()) {
    std::unique_ptr<disk::DiskReporter> reporter(m_mount.createDiskReporter(m_dbJob->archiveReportURL));
    reporter->reportArchiveFullyComplete();
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// ArchiveJob::reportURL
//------------------------------------------------------------------------------
std::string cta::ArchiveJob::reportURL() {
  return m_dbJob->archiveReportURL;
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::ArchiveJob::failed(const cta::exception::Exception &ex) {
  m_dbJob->fail();
}
