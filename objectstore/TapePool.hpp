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
#include "scheduler/CreationLog.hpp"
#include "ArchiveToFileRequest.hpp"
#include "CreationLog.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {
  
class GenericObject;

class TapePool: public ObjectOps<serializers::TapePool> {
public:
  // Constructor
  TapePool(const std::string & address, Backend & os);
  
  // Upgrader form generic object
  TapePool(GenericObject & go);

  // In memory initialiser
  void initialize(const std::string & name);
  
  // Set/get name
  void setName(const std::string & name);
  std::string getName();
  
  // Tapes management ==========================================================
  std::string addOrGetTapeAndCommit(const std::string &vid, 
    const std::string &logicalLibraryName, const uint64_t capacityInBytes, 
    Agent & agent, const cta::CreationLog & CreationLog);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchTape);
  CTA_GENERATE_EXCEPTION_CLASS(WrongTape);
  void removeTapeAndCommit(const std::string &vid);
  std::string getTapeAddress(const std::string &vid);
  class TapeDump {
  public:
    std::string vid;
    std::string address;
    std::string logicalLibraryName;
    uint64_t capacityInBytes;
    objectstore::CreationLog log;
  };
  std::list<TapeDump> dumpTapes();
  
  // Archival jobs management ==================================================
  void addJob(const ArchiveToFileRequest::JobDump & job,
    const std::string & archiveToFileAddress, uint64_t size);
  
  // Check that the tape pool is empty (of both tapes and jobs)
  bool isEmpty();
 
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  // Garbage collection
  void garbageCollect();
};
  
}}