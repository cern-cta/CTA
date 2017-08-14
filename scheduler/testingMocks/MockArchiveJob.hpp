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

#pragma once

#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveJob.hpp"
#include <memory>

namespace cta {
  class MockArchiveJob: public cta::ArchiveJob {
  public:
    int completes;
    int failures;
    MockArchiveJob(cta::ArchiveMount & am, cta::catalogue::Catalogue &catalogue): cta::ArchiveJob(am, 
        catalogue, cta::common::dataStructures::ArchiveFile(), 
        "", cta::common::dataStructures::TapeFile()),
        completes(0), failures(0) {} 
      
    ~MockArchiveJob() throw() {} 
    
    void failed(const cta::exception::Exception& ex, log::LogContext & lc) override {
      failures++;
    }
    
    virtual void asyncSetJobSucceed() override {
      completes++;
    }
    virtual bool checkAndAsyncReportComplete() override {
      return false;
    }    
    virtual void validate() override  {}
    virtual void writeToCatalogue() override {}
    virtual catalogue::TapeFileWritten validateAndGetTapeFileWritten() override {
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
      fileReport.tapeDrive = "dummy";
      fileReport.vid = tapeFile.vid;
      return fileReport;
    }
    virtual void failed(const cta::exception::Exception& ex) {
      failures++;
    }
    virtual void retry() {}
  };
}
