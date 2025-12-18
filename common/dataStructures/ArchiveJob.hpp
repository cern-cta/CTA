/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/ArchiveRequest.hpp"

#include <list>
#include <map>
#include <string>

namespace cta::common::dataStructures {

/**
 * The archive job contains the original request, and all data needed to queue
 * the request in the system
 */
struct ArchiveJob {
  ArchiveJob();

  bool operator==(const ArchiveJob& rhs) const;

  bool operator!=(const ArchiveJob& rhs) const;

  ArchiveRequest request;
  std::string tapePool;
  std::string instanceName;
  uint32_t copyNumber = 0;
  uint64_t archiveFileID = 0;
  std::string objectId;  //!< Objectstore address, provided when reporting a failed job
  std::list<std::string> failurelogs;
  std::list<std::string> reportfailurelogs;
  uint64_t totalRetries;
  uint64_t totalReportRetries;

};  // struct ArchiveJob

std::ostream& operator<<(std::ostream& os, const ArchiveJob& obj);

}  // namespace cta::common::dataStructures
