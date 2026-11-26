/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDBTestFixture.hpp"

namespace unitTests {

cta::common::dataStructures::MountPolicy makeMountPolicy(time_t t) {
  cta::common::dataStructures::MountPolicy mountPolicy;
  mountPolicy.name = "mountPolicy";
  mountPolicy.archivePriority = 1;
  mountPolicy.archiveMinRequestAge = 0;
  mountPolicy.retrievePriority = 1;
  mountPolicy.retrieveMinRequestAge = 0;
  mountPolicy.creationLog = {"u", "h", t};
  mountPolicy.lastModificationLog = {"u", "h", t};
  mountPolicy.comment = "comment";
  return mountPolicy;
}

std::unique_ptr<cta::log::Logger> makeLogger() {
#ifdef STDOUT_LOGGING
  return std::make_unique<cta::log::StdoutLogger>("dummy", "unitTest");
#else
  return std::make_unique<cta::log::DummyLogger>("dummy", "unitTest");
#endif
}

void queueArchiveJob(cta::SchedulerDatabase& db,
                     cta::log::LogContext& lc,
                     uint64_t fileId,
                     const std::string& tapePool,
                     const std::string& suffix,
                     time_t creationTime) {
  cta::common::dataStructures::ArchiveRequest ar;
  cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

  afqc.copyToPoolMap.insert({1, tapePool});
  afqc.fileId = fileId;
  afqc.mountPolicy = makeMountPolicy(creationTime);

  ar.archiveReportURL = "test://archive-report-url";
  ar.archiveErrorReportURL = "test://error-report-url";
  ar.creationLog = {"user", "host", creationTime};
  ar.diskFileID = "diskFile-" + suffix;
  ar.diskFileInfo.path = "/path/" + suffix;
  ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  ar.diskFileInfo.gid = DISK_FILE_GID;
  ar.fileSize = 1000 + fileId;
  ar.requester = {"user", "group"};
  ar.srcURL = "root:/path/" + suffix;
  ar.storageClass = "storageClass";

  db.queueArchive("eosInstance", ar, afqc, lc);
}

void queueRetrieveJob(cta::SchedulerDatabase& db,
                      cta::log::LogContext& lc,
                      uint64_t archiveFileId,
                      const std::string& vid,
                      const std::string& suffix,
                      time_t creationTime) {
  cta::common::dataStructures::RetrieveRequest rr;
  cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;

  rfqc.mountPolicy = makeMountPolicy(creationTime);

  rfqc.archiveFile.archiveFileID = archiveFileId;
  rfqc.archiveFile.fileSize = 4000 + archiveFileId;
  rfqc.archiveFile.storageClass = s_storageClassName;
  rfqc.archiveFile.diskInstance = "diskInstance";
  rfqc.archiveFile.diskFileId = "diskFile-" + suffix;
  rfqc.archiveFile.diskFileInfo.path = "/retrieve/" + suffix;
  rfqc.archiveFile.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  rfqc.archiveFile.diskFileInfo.gid = DISK_FILE_GID;

  rfqc.archiveFile.tapeFiles.emplace_back();
  rfqc.archiveFile.tapeFiles.back().fSeq = archiveFileId;
  rfqc.archiveFile.tapeFiles.back().vid = vid;
  rfqc.archiveFile.tapeFiles.back().copyNb = 1;
  rfqc.archiveFile.tapeFiles.back().fileSize = 4000 + archiveFileId;

  rr.archiveFileID = archiveFileId;
  rr.creationLog = {"user", "host", creationTime};
  rr.diskFileInfo.path = "/retrieve/" + suffix;
  rr.requester = {"user", "group"};
  rr.dstURL = "root://disk/" + suffix;
  rr.errorReportURL = "test://retrieve-error-report-url";

  db.queueRetrieve(rr, rfqc, "ds-A", lc);
}

void queueMultiCopyArchiveJob(cta::SchedulerDatabase& db,
                              cta::log::LogContext& lc,
                              uint64_t fileId,
                              const std::string& tapePool,
                              const std::string& suffix,
                              time_t creationTime) {
  cta::common::dataStructures::ArchiveRequest ar;
  cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

  // Two copies to the same tape pool so both land in the same pending queue
  afqc.copyToPoolMap.insert({1, tapePool});
  afqc.copyToPoolMap.insert({2, tapePool});
  afqc.fileId = fileId;
  afqc.mountPolicy = makeMountPolicy(creationTime);

  ar.archiveReportURL = "test://archive-report-url";
  ar.archiveErrorReportURL = "test://error-report-url";
  ar.creationLog = {"user", "host", creationTime};
  ar.diskFileID = "diskFile-" + suffix;
  ar.diskFileInfo.path = "/path/" + suffix;
  ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  ar.diskFileInfo.gid = DISK_FILE_GID;
  ar.fileSize = 1000 + fileId;
  ar.requester = {"user", "group"};
  ar.srcURL = "root:/path/" + suffix;
  ar.storageClass = "storageClass";

  db.queueArchive("eosInstance", ar, afqc, lc);
}

void queueTripleCopyArchiveJob(cta::SchedulerDatabase& db,
                               cta::log::LogContext& lc,
                               uint64_t fileId,
                               const std::string& tapePool,
                               const std::string& suffix,
                               time_t creationTime) {
  cta::common::dataStructures::ArchiveRequest ar;
  cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

  afqc.copyToPoolMap.insert({1, tapePool});
  afqc.copyToPoolMap.insert({2, tapePool});
  afqc.copyToPoolMap.insert({3, tapePool});
  afqc.fileId = fileId;
  afqc.mountPolicy = makeMountPolicy(creationTime);

  ar.archiveReportURL = "test://archive-report-url";
  ar.archiveErrorReportURL = "test://error-report-url";
  ar.creationLog = {"user", "host", creationTime};
  ar.diskFileID = "diskFile-" + suffix;
  ar.diskFileInfo.path = "/path/" + suffix;
  ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  ar.diskFileInfo.gid = DISK_FILE_GID;
  ar.fileSize = 1000 + fileId;
  ar.requester = {"user", "group"};
  ar.srcURL = "root:/path/" + suffix;
  ar.storageClass = "storageClass";

  db.queueArchive("eosInstance", ar, afqc, lc);
}

}  // namespace unitTests
