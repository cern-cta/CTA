/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#pragma once

#include <memory>

#include "catalogue/TapeFileWritten.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"

namespace cta {
class MockArchiveJob : public cta::ArchiveJob {
public:
  int completes;
  int failures;

  MockArchiveJob(cta::ArchiveMount* am, cta::catalogue::Catalogue& catalogue) :
  cta::ArchiveJob(am,
                  catalogue,
                  cta::common::dataStructures::ArchiveFile(),
                  "",
                  cta::common::dataStructures::TapeFile()),
  completes(0),
  failures(0) {}

  ~MockArchiveJob() throw() {}

  void transferFailed(const std::string& failureReason, log::LogContext& lc) override { failures++; }

  virtual void reportJobSucceeded() { completes++; }

  virtual void validate() override {}

  virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten() override {
    auto fileReportUP = std::make_unique<cta::catalogue::TapeFileWritten>();
    auto& fileReport = *fileReportUP;
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

  virtual void failed(const cta::exception::Exception& ex) { failures++; }

  virtual void retry() {}
};
}  // namespace cta
