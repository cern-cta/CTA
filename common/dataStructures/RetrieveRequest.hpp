/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "LifecycleTimings.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

#include <future>
#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds all the command line parameters of a CTA retrieve command
 */
struct RetrieveRequest {
  RetrieveRequest();

  bool operator==(const RetrieveRequest& rhs) const;

  bool operator!=(const RetrieveRequest& rhs) const;

  /**
  * Idempotently append the fileSize to the dstURL
  * The fileSize will be append only if the dstURL is an XRootD one
  * @param fileSize the file size to append
  */
  void appendFileSizeToDstURL(const uint64_t fileSize);

  RequesterIdentity requester;
  uint64_t archiveFileID = 0;
  std::string dstURL;
  std::string retrieveReportURL;
  std::string errorReportURL;
  DiskFileInfo diskFileInfo;
  EntryLog creationLog;
  bool isVerifyOnly = false;       // request to retrieve file from tape but do not write a disk copy
  std::optional<std::string> vid;  // limit retrieve requests to the specified vid (in the case of dual-copy files)
  std::optional<std::string>
    mountPolicy;  // limit retrieve requests to a specified mount policy (only used for verification requests)
  LifecycleTimings lifecycleTimings;
  std::optional<std::string> activity;

};  // struct RetrieveRequest

std::ostream& operator<<(std::ostream& os, const RetrieveRequest& obj);

/**
 * One request opportunistically batched by Scheduler::queueRetrieve(). instanceName/request are the
 * caller's input; criteria/diskSystemName are resolved per item in stage 1 (catalogue lookup); the
 * copy to read from is only decided during stage 2 (bulk insert), so selectedVid is left empty until
 * then, same as queued below (see ArchiveInsertQueueItem for why copyToPoolMap-style "still empty"
 * checks alone can't tell success from failure).
 */
struct RetrieveInsertQueueItem {
  std::string instanceName;
  cta::common::dataStructures::RetrieveRequest request;
  cta::common::dataStructures::RetrieveFileQueueCriteria criteria;
  std::optional<std::string> diskSystemName;
  std::string selectedVid;

  // Resolves to the request ID (a placeholder "bogus" string, like ArchiveInsertQueueItem's promise
  // — see ArchiveRequest::getIdStr()'s own comment), which is all Scheduler::queueRetrieve() itself
  // returns to its own caller.
  std::promise<std::string> promise;
  bool queued = false;
};

}  // namespace cta::common::dataStructures
