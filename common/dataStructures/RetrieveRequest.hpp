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
