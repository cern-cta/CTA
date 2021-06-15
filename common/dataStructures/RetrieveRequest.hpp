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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "LifecycleTimings.hpp"
#include "common/optional.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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
  optional<std::string> vid;    // limit retrieve requests to the specified vid (in the case of dual-copy files)

  LifecycleTimings lifecycleTimings;
  optional<std::string> activity;

}; // struct RetrieveRequest

std::ostream &operator<<(std::ostream &os, const RetrieveRequest &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
