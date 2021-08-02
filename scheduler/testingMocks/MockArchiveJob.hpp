/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "common/make_unique.hpp"
#include <memory>

namespace cta {
  class MockArchiveJob: public cta::ArchiveJob {
  public:
    int completes;
    int failures;
    MockArchiveJob(cta::ArchiveMount * am, cta::catalogue::Catalogue &catalogue): cta::ArchiveJob(am, 
        catalogue, cta::common::dataStructures::ArchiveFile(), 
        "", cta::common::dataStructures::TapeFile()),
        completes(0), failures(0) {} 
      
    ~MockArchiveJob() throw() {} 

    void transferFailed(const std::string& failureReason, log::LogContext& lc) override {
      failures++;
    }
    
    virtual void reportJobSucceeded() {
      completes++;
    } 
    
    virtual void validate() override  {}
    virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten() override {
      auto fileReportUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto &  fileReport = *fileReportUP;
      fileReport.archiveFileId = archiveFile.archiveFileID;
      fileReport.blockId = tapeFile.blockId;
      fileReport.checksumBlob = tapeFile.checksumBlob;
      fileReport.copyNb = tapeFile.copyNb;
      fileReport.diskFileId = archiveFile.diskFileId;
      fileReport.diskFileOwnerUid = archiveFile.diskFileInfo.owner_uid;
      fileReport.diskFileGid = archiveFile.diskFileInfo.gid;
      fileReport.diskInstance = archiveFile.diskInstance;
      fileReport.fSeq = tapeFile.fSeq;
      fileReport.size = archiveFile.fileSize;
      fileReport.storageClassName = archiveFile.storageClass;
      fileReport.tapeDrive = "dummy";
      fileReport.vid = tapeFile.vid;
      return cta::catalogue::TapeItemWrittenPointer(fileReportUP.release());
    }
    virtual void failed(const cta::exception::Exception& ex) {
      failures++;
    }
    virtual void retry() {}
  };
}
