/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "LifecycleTimings.hpp"

namespace cta::common::dataStructures {

/**
 * This struct holds all the command line parameters of a CTA retrieve command
 */
struct RetrieveRequest {

  RetrieveRequest();

  bool operator==(const RetrieveRequest &rhs) const;

  bool operator!=(const RetrieveRequest &rhs) const;

  /**
  * Idempotently append the fileSize to the dstURL
  * The fileSize will be append only if the dstURL is an XRootD one
  * @param fileSize the file size to append
  */
  void appendFileSizeToDstURL(const uint64_t fileSize);

  RequesterIdentity requester;
  uint64_t archiveFileID;
  std::string dstURL;
  std::string retrieveReportURL;
  std::string errorReportURL;
  DiskFileInfo diskFileInfo;
  EntryLog creationLog;
  bool isVerifyOnly;    // request to retrieve file from tape but do not write a disk copy
  std::optional<std::string> vid;    // limit retrieve requests to the specified vid (in the case of dual-copy files)
  std::optional<std::string> mountPolicy; // limit retrieve requests to a specified mount policy (only used for verification requests)
  LifecycleTimings lifecycleTimings;
  std::optional<std::string> activity;

}; // struct RetrieveRequest

std::ostream &operator<<(std::ostream &os, const RetrieveRequest &obj);

} // namespace cta::common::dataStructures
