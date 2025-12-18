/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/TapeFile.hpp"

#include <list>
#include <map>
#include <string>

namespace cta::common::dataStructures {

/**
 * The retrieve job contains the original request, and all data needed to queue
 * the request in the system
 */
struct RetrieveJob {
  RetrieveJob() = default;
  bool operator==(const RetrieveJob& rhs) const;
  bool operator!=(const RetrieveJob& rhs) const;

  RetrieveRequest request;
  uint64_t fileSize;
  std::map<std::string, std::pair<uint32_t, TapeFile>> tapeCopies;
  std::string objectId;  //!< Objectstore address, provided when reporting a failed job
  std::list<std::string> failurelogs;
  std::list<std::string> reportfailurelogs;
  // Elements not needed for queueing
  std::string diskInstance;
  std::string storageClass;
  std::string diskFileId;
  uint64_t totalRetries;
  uint64_t totalReportRetries;
};

std::ostream& operator<<(std::ostream& os, const RetrieveJob& obj);

}  // namespace cta::common::dataStructures
