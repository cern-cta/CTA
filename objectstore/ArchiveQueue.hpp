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

#include "Backend.hpp"
#include "ObjectOps.hpp"
#include <string>
#include "objectstore/cta.pb.h"
#include "common/CreationLog.hpp"
#include "common/MountControl.hpp"
#include "ArchiveRequest.hpp"
#include "ArchiveRequest.hpp"
#include "EntryLogSerDeser.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {
  
class GenericObject;

class ArchiveQueue: public ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t> {
  // TODO: rename tapepoolname field to tapepool (including in probuf)
  
public:
  // Constructor
  ArchiveQueue(const std::string & address, Backend & os);
  
  // Undefined object constructor
  ArchiveQueue(Backend & os);
  
  // Upgrader form generic object
  ArchiveQueue(GenericObject & go);

  // In memory initialiser
  void initialize(const std::string & name);
  
  // Set/get tape pool
  void setTapePool(const std::string & name);
  std::string getTapePool();
  
  // Archive jobs management ===================================================  
  void addJob(const ArchiveRequest::JobDump & job,
    const std::string & archiveRequestAddress, uint64_t archiveFileId,
    uint64_t fileSize, const cta::common::dataStructures::MountPolicy & priority, time_t startTime);
  /// This version will check for existence of the job in the queue before
  // returns true if a new job was actually inserted.
  bool addJobIfNecessary(const ArchiveRequest::JobDump & job,
    const std::string & archiveRequestAddress, uint64_t archiveFileId,
    uint64_t fileSize, const cta::common::dataStructures::MountPolicy & priority, time_t startTime);
  /// This version will check for existence of the job in the queue before
  // returns true if a new job was actually inserted.
  bool addOrphanedJobPendingNsCreation(const ArchiveRequest::JobDump& job,
    const std::string& archiveToFileAddress, uint64_t fileid,
    uint64_t size);
  /// This version will check for existence of the job in the queue before
  // returns true if a new job was actually inserted.
  bool addOrphanedJobPendingNsDeletion(const ArchiveRequest::JobDump& job,
    const std::string& archiveToFileAddress,
    uint64_t fileid, uint64_t size);
  
  struct JobsSummary {
    uint64_t files;
    uint64_t bytes;
    time_t oldestJobStartTime;
    uint64_t priority;
    uint64_t minArchiveRequestAge;
    uint64_t maxDrivesAllowed;
  };
  JobsSummary getJobsSummary();
  
  void removeJob(const std::string &archiveToFileAddress);
  class JobDump {
  public:
    uint64_t size;
    std::string address;
    uint16_t copyNb;
  };
  std::list<JobDump> dumpJobs();
  
  // Check that the tape pool is empty (of both tapes and jobs)
  bool isEmpty();
 
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  // Garbage collection
  void garbageCollect(const std::string &presumedOwner) override;
  
  std::string dump();
};
  
}}
