/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/TapeFileWritten.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"

namespace cta {
  class MockArchiveJob: public cta::ArchiveJob {
  public:
    int completes;
    int failures;
    MockArchiveJob(cta::ArchiveMount * am, cta::catalogue::Catalogue &catalogue): cta::ArchiveJob(am,
        catalogue, cta::common::dataStructures::ArchiveFile(),
        "", cta::common::dataStructures::TapeFile()),
        completes(0), failures(0) {}

    ~MockArchiveJob() noexcept {}

    void transferFailed(const std::string& failureReason, log::LogContext& lc) override {
      failures++;
    }

    virtual void reportJobSucceeded() {
      completes++;
    }

    virtual void validate() override  {}
    virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten() override {
      auto fileReportUP = std::make_unique<cta::catalogue::TapeFileWritten>();
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
