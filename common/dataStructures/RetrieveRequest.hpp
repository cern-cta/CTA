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
 * One retrieve request in flight through the opportunistic batching pipeline.
 *
 * Lifecycle mirrors ArchiveInsertQueueItem.  A caller thread constructs the item,
 * resolves the catalogue criteria (stage 1) on its own thread, then hands it to
 * OpportunisticQueueBatcher via enqueueAndWait().  The leader thread collects a
 * batch and calls FrontendBatchingLayer::resolveRetrieveBatch() (stage 2), which
 * performs a single bulk DB insert — selecting the best tape copy per item — and
 * settles every promise.  Unlike archive requests, a retrieve is always a single
 * job (one tape copy read), so there is no fan-out.
 */
struct RetrieveInsertQueueItem {
  /** Disk-system instance name (e.g. EOS instance) the file is being recalled to. */
  std::string instanceName;

  /** Full set of recall parameters supplied by the client (archive file ID,
   *  destination URL, report URLs, requester identity, optional VID pin, …).
   *  request.appendFileSizeToDstURL() is called inside stage 2; the mutation is
   *  internal and not visible to the caller after enqueueAndWait() returns. */
  cta::common::dataStructures::RetrieveRequest request;

  /** Archive-file metadata and mount policy resolved from the catalogue in stage 1.
   *  criteria.archiveFile carries all tape-copy locations; the DB uses them in
   *  stage 2 to pick the best VID (stored in selectedVid). */
  cta::common::dataStructures::RetrieveFileQueueCriteria criteria;

  /** Disk-system name resolved from the catalogue in stage 1, if the destination
   *  URL maps to a known disk system.  Empty when no disk-system match exists. */
  std::optional<std::string> diskSystemName;

  /** VID of the tape copy selected for reading, written by the DB during stage 2
   *  (RelationalDB::queueRetrieve → selectBestVid4Retrieve).  Empty string until
   *  stage 2 completes; used only for post-batch logging. */
  std::string selectedVid = {};

  /** Settled by the leader thread in stage 2 with the request ID on success, or
   *  with an exception on failure.  The caller thread blocks on the corresponding
   *  future until the batch is committed. */
  std::promise<std::string> promise;

  /** True once promise.set_value() has been called (i.e. the DB insert succeeded).
   *  Left false when promise.set_exception() is used instead.  See
   *  ArchiveInsertQueueItem::queued for why a separate flag is needed. */
  bool queued = false;
};

}  // namespace cta::common::dataStructures
