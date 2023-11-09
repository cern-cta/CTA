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

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/TapeFile.hpp"

namespace cta::common::dataStructures {

/**
 * The retrieve job contains the original request, and all data needed to queue 
 * the request in the system 
 */
struct RetrieveJob {

  RetrieveJob();

  bool operator==(const RetrieveJob &rhs) const;

  bool operator!=(const RetrieveJob &rhs) const;

  RetrieveRequest request;
  uint64_t fileSize;
  std::map<std::string,std::pair<uint32_t,TapeFile>> tapeCopies;
  std::string objectId; //!< Objectstore address, provided when reporting a failed job
  std::list<std::string> failurelogs;
  std::list<std::string> reportfailurelogs;
  //Elements not needed for queueing
  std::string diskInstance;
  std::string storageClass;
  std::string diskFileId;
  uint64_t totalRetries;
  uint64_t totalReportRetries;

}; // struct RetrieveJob

std::ostream &operator<<(std::ostream &os, const RetrieveJob &obj);

} // namespace cta::common::dataStructures
