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
#include <list>
#include <limits>
#include "common/DriveState.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class DriveRegister: public ObjectOps<serializers::DriveRegister, serializers::DriveRegister_t> {
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
    const EntryLogSerDeser & creationLog);
  void removeDrive (const std::string  & name);
private:
  cta::MountType::Enum deserializeMountType(serializers::MountType);
  serializers::MountType serializeMountType(cta::MountType::Enum);
  common::DriveStatus deserializeDriveStatus(serializers::DriveStatus);
  serializers::DriveStatus serializeDriveStatus(common::DriveStatus);
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
    common::DriveStatus status, time_t reportTime, 
    cta::MountType::Enum mountType = cta::MountType::NONE,
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
    common::DriveStatus status;
    cta::MountType::Enum mountType;
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
  void setDriveStarting(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveMounting(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveTransfering(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveUnloading(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveUnmounting(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveDrainingToDisk(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
  void setDriveCleaningUp(ReportDriveStatusInputs & inputs, serializers::DriveState * drive);
public:
  std::list<common::DriveState> dumpDrives();
  std::string dump();
};

}}