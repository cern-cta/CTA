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
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class CreationLog;

class ArchiveToFileRequest: public ObjectOps<serializers::ArchiveToFileRequest> {
public:
  ArchiveToFileRequest(const std::string & address, Backend & os);
  ArchiveToFileRequest(GenericObject & go);
  void initialize();
  void addJob(uint16_t copyNumber, const std::string & tapepool,
    const std::string & tapepooladdress);
  void setArchiveFile(const std::string & archiveFile);
  std::string getArchiveFile();
  void setRemoteFile (const std::string & remoteFile);
  std::string getRemoteFile();
  void setPriority (uint64_t priority);
  void setLog (const objectstore::CreationLog& creationLog);
  void setArchiveToDirRequestAddress(const std::string & dirRequestAddress);
  void setSize(uint64_t size);
  uint64_t getSize();
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tapePool;
    std::string tapePoolAddress;
  };
  std::list<JobDump> dumpJobs();
};

}}
