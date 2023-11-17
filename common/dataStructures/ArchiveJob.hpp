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
#include <string>

#include "common/dataStructures/ArchiveRequest.hpp"

namespace cta::common::dataStructures {

/**
 * The archive job contains the original request, and all data needed to queue 
 * the request in the system 
 */
struct ArchiveJob {

  ArchiveJob();

  bool operator==(const ArchiveJob &rhs) const;

  bool operator!=(const ArchiveJob &rhs) const;

  ArchiveRequest request;
  std::string tapePool;
  std::string instanceName;
  uint32_t copyNumber;
  uint64_t archiveFileID;
  std::string objectId; //!< Objectstore address, provided when reporting a failed job
  std::list<std::string> failurelogs;
  std::list<std::string> reportfailurelogs;
  uint64_t totalRetries;
  uint64_t totalReportRetries;

}; // struct ArchiveJob

std::ostream &operator<<(std::ostream &os, const ArchiveJob &obj);

} // namespace cta::common::dataStructures
