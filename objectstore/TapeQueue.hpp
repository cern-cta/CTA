/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "RetrieveToFileRequest.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"
#include "scheduler/RetrieveRequestDump.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;

class TapeQueue: public ObjectOps<serializers::TapeQueue, serializers::TapeQueue_t> {
public:
  TapeQueue(const std::string & address, Backend & os);
  TapeQueue(GenericObject & go);
  void initialize(const std::string & vid, const std::string &logicalLibrary, 
    const cta::CreationLog & creationLog);
  void garbageCollect();
  bool isEmpty();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void removeIfEmpty();
  std::string dump();
  
  // Retrieve jobs management ==================================================
  void addJob(const RetrieveToFileRequest::JobDump & job,
    const std::string & retrieveToFileAddress, uint64_t size, uint64_t priority,
    time_t startTime);
  struct JobsSummary {
    uint64_t files;
    uint64_t bytes;
    time_t oldestJobStartTime;
    uint64_t priority;
  };
  JobsSummary getJobsSummary();
  std::list<RetrieveRequestDump> dumpAndFetchRetrieveRequests();
  struct JobDump {
    std::string address;
    uint16_t copyNb;
    uint64_t size;
  };
  std::list<JobDump> dumpJobs();
  
  void removeJob(const std::string & retriveToFileAddress);
  // -- Generic parameters
  std::string getVid();
};

}}
