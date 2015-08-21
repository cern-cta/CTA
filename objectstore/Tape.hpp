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

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;

class Tape: public ObjectOps<serializers::Tape> {
public:
  Tape(const std::string & address, Backend & os);
  Tape(GenericObject & go);
  void initialize(const std::string & vid, const std::string &logicalLibrary);
  void garbageCollect();
  bool isEmpty();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void removeIfEmpty();
  std::string dump();
  
  // Tape location management ==================================================
  std::string getLogicalLibrary();
  void setLogicalLibrary(const std::string & library);
  
  // Tape status management ====================================================
  void setFull(bool full);
  bool isFull();
  void setArchived(bool archived);
  bool isArchived();
  void setReadOnly(bool readOnly);
  bool isReadOnly();
  void setDisabled(bool disabled);
  bool isDisabled();
  
  enum class MountType {
    Archive, Retrieve
  };
  void setBusy(const std::string & drive, MountType mountType, 
    const std::string & host, time_t time, const std::string &agentAddress);
  void releaseBusy();
  bool isBusy();
  
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
  
  // -- Stored data counting ---------------------------------------------------
  uint64_t getStoredData();
  std::string getVid();
  void setStoredData(uint64_t bytes);
  void addStoredData(uint64_t bytes);
};

}}