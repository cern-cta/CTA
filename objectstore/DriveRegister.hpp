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
#include <limits>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class CreationLog;

class DriveRegister: public ObjectOps<serializers::DriveRegister> {
  CTA_GENERATE_EXCEPTION_CLASS(DuplicateEntry);
public:
  DriveRegister(const std::string & address, Backend & os);
  DriveRegister(GenericObject & go);
  void initialize();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string &presumedOwner);
  bool isEmpty();
  
  // Drives management =========================================================
  void addDrive (const std::string & driveName, const std::string & logicalLibrary,
    const CreationLog & creationLog);
  void removeDrive (const std::string  & name);
  enum class MountType {
    NoMount,
    Archive,
    Retrieve
  };
  enum class DriveStatus {
    Down,
    Idle,
    Starting,
    Mounting,
    Transfering,
    Unloading,
    Unmounting,
    DrainingToDisk,
    CleaningUp
  };
  struct DriveState {
    std::string name;
    std::string logicalLibrary;
    uint64_t sessionId;
    uint64_t bytesTransferedInSession;
    uint64_t filesTransferedInSession;
    double latestBandwidth; /** < Byte per seconds */
    time_t sessionStartTime;
    time_t mountStartTime;
    time_t transferStartTime;
    time_t unloadStartTime;
    time_t unmountStartTime;
    time_t drainingStartTime;
    time_t downOrIdleStartTime;
    time_t cleanupStartTime;
    time_t lastUpdateTime;
    MountType mountType;
    DriveStatus status;
    std::string currentVid;
    std::string currentTapePool;
  };
private:
  MountType deserializeMountType(serializers::MountType);
  serializers::MountType serializeMountType(MountType);
  DriveStatus deserializeDriveStatus(serializers::DriveStatus);
  serializers::DriveStatus serializeDriveStatus(DriveStatus);
public:
  CTA_GENERATE_EXCEPTION_CLASS(MissingStatistics);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);
  void reportDriveStatus (const std::string & drive, const std::string & logicalLibary,
    DriveStatus status, time_t reportTime, 
    uint64_t mountSessionId = std::numeric_limits<uint64_t>::max(),
    uint64_t byteTransfered = std::numeric_limits<uint64_t>::max(), 
    uint64_t filesTransfered = std::numeric_limits<uint64_t>::max(),
    double latestBandwidth = std::numeric_limits<double>::max());
  std::list<DriveState> dumpDrives();
  std::string dump();
};

}}