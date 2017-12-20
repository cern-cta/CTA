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
#include "RetrieveRequest.hpp"
#include "scheduler/RetrieveRequestDump.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;

class RetrieveQueue: public ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t> {
public:
  RetrieveQueue(const std::string & address, Backend & os);
  // Undefined object constructor
  RetrieveQueue(Backend & os);
  RetrieveQueue(GenericObject & go);
  void initialize(const std::string & vid);
  void commit();
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  bool isEmpty();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void removeIfEmpty(log::LogContext & lc);
  std::string dump();
  
  // Retrieve jobs management ==================================================
  struct JobToAdd {
    uint64_t copyNb;
    uint64_t fSeq;
    const std::string retrieveRequestAddress;
    uint64_t size;
    const cta::common::dataStructures::MountPolicy policy;
    time_t startTime;
  };
  void addJobs(std::list<JobToAdd> & jobsToAdd);
  /// This version will check for existence of the job in the queue before
  // returns true if a new job was actually inserted.
  bool addJobIfNecessary(uint64_t copyNb, uint64_t fSeq,
    const std::string & retrieveRequestAddress, uint64_t size,
    const cta::common::dataStructures::MountPolicy & policy, time_t startTime);
  struct JobsSummary {
    uint64_t files;
    uint64_t bytes;
    time_t oldestJobStartTime;
    uint64_t priority;
    uint64_t minArchiveRequestAge;
    uint64_t maxDrivesAllowed;
  };
  JobsSummary getJobsSummary();
  std::list<RetrieveRequestDump> dumpAndFetchRetrieveRequests();
  struct JobDump {
    std::string address;
    uint16_t copyNb;
    uint64_t size;
  };
  std::list<JobDump> dumpJobs();
  
  void removeJob(const std::string & retrieveToFileAddress);
  // -- Generic parameters
  std::string getVid();
};

}}
