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
    Up,
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
    time_t downOrUpStartTime;
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
  CTA_GENERATE_EXCEPTION_CLASS(MissingTapeInfo);
  CTA_GENERATE_EXCEPTION_CLASS(MissingSessionInfo);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);
  CTA_GENERATE_EXCEPTION_CLASS(WrongStateTransition);
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  /**
   * Report the status of the drive to the DB.
   */
  void reportDriveStatus (const std::string & drive, const std::string & logicalLibary,
    DriveStatus status, time_t reportTime, 
    MountType mountType = MountType::NoMount,
    uint64_t mountSessionId = std::numeric_limits<uint64_t>::max(),
    uint64_t byteTransfered = std::numeric_limits<uint64_t>::max(), 
    uint64_t filesTransfered = std::numeric_limits<uint64_t>::max(),
    double latestBandwidth = std::numeric_limits<double>::max(),
    const std::string & vid = "", 
    const std::string & tapepool = "");
private:
  /* Collection of smaller scale parts of reportDriveStatus */
  struct ReportDriveStatusInputs {
    const std::string & drive;
    const std::string & logicalLibary;
    DriveStatus status;
    MountType mountType;
    time_t reportTime; 
    uint64_t mountSessionId;
    uint64_t byteTransfered;
    uint64_t filesTransfered;
    double latestBandwidth;
    std::string vid;
    std::string tapepool;
    ReportDriveStatusInputs(const std::string & d, const std::string & ll):
      drive(d), logicalLibary(ll) {}
  };
  void checkReportDriveStatusInputs(ReportDriveStatusInputs & inputs);
  void setDriveDown(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveUp(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveMounting(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveTransfering(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveUnloading(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveUnmounting(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveDrainingToDisk(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveCleaningUp(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
public:
  std::list<DriveState> dumpDrives();
  std::string dump();
};

}}