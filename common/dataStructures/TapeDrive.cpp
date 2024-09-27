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

#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeDrive::TapeDrive():
  driveStatus(DriveStatus::Unknown),
  desiredUp(false),
  desiredForceDown(false),
  nextMountType(MountType::NoMount) {}

const std::map<DriveStatus, std::string> TapeDrive::STATE_TO_STRING_MAP = {
  {DriveStatus::Unknown, "UNKNOWN"},
  {DriveStatus::Down, "DOWN"},
  {DriveStatus::Up, "UP"},
  {DriveStatus::Probing, "PROBING"},
  {DriveStatus::Starting, "STARTING"},
  {DriveStatus::Mounting, "MOUNTING"},
  {DriveStatus::Transferring, "TRANSFERING"},
  {DriveStatus::Unloading, "UNLOADING"},
  {DriveStatus::Unmounting, "UNMOUNTING"},
  {DriveStatus::DrainingToDisk, "DRAININGTODISK"},
  {DriveStatus::CleaningUp, "CLEANINGUP"},
  {DriveStatus::Shutdown, "SHUTDOWN"}
};

const std::map<std::string, DriveStatus> TapeDrive::STRING_TO_STATE_MAP = {
  {"UNKNOWN", DriveStatus::Unknown},
  {"DOWN", DriveStatus::Down},
  {"UP", DriveStatus::Up},
  {"PROBING", DriveStatus::Probing},
  {"STARTING", DriveStatus::Starting},
  {"MOUNTING", DriveStatus::Mounting},
  {"TRANSFERING", DriveStatus::Transferring},
  {"UNLOADING", DriveStatus::Unloading},
  {"UNMOUNTING", DriveStatus::Unmounting},
  {"DRAININGTODISK", DriveStatus::DrainingToDisk},
  {"CLEANINGUP", DriveStatus::CleaningUp},
  {"SHUTDOWN", DriveStatus::Shutdown}
};

std::string TapeDrive::getAllPossibleStates(){
  std::string ret;
  for(auto &kv: STRING_TO_STATE_MAP){
    ret += kv.first + " ";
  }
  if(ret.size())
    ret.pop_back();
  return ret;
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeDrive::operator==(const TapeDrive &rhs) const {
  auto checkOptionalString = [](const std::optional<std::string> &str) -> std::string {
    if (!str) return "";
    return str.value();
  };
  return driveName == rhs.driveName
      && host == rhs.host
      && logicalLibrary == rhs.logicalLibrary
      && mountType == rhs.mountType
      && driveStatus == rhs.driveStatus
      && desiredUp == rhs.desiredUp
      && desiredForceDown == rhs.desiredForceDown

      && sessionId == rhs.sessionId
      && bytesTransferedInSession == rhs.bytesTransferedInSession
      && filesTransferedInSession == rhs.filesTransferedInSession
      && sessionStartTime==rhs.sessionStartTime
      && sessionElapsedTime==rhs.sessionElapsedTime
      && mountStartTime == rhs.mountStartTime
      && transferStartTime == rhs.transferStartTime
      && unloadStartTime == rhs.unloadStartTime
      && unmountStartTime == rhs.unmountStartTime
      && drainingStartTime == rhs.drainingStartTime
      && downOrUpStartTime == rhs.downOrUpStartTime
      && probeStartTime == rhs.probeStartTime
      && cleanupStartTime == rhs.cleanupStartTime
      && startStartTime == rhs.startStartTime
      && shutdownTime == rhs.shutdownTime

      && checkOptionalString(reasonUpDown) == checkOptionalString(rhs.reasonUpDown)
      && checkOptionalString(currentVid) == checkOptionalString(rhs.currentVid)
      && checkOptionalString(ctaVersion) == checkOptionalString(rhs.ctaVersion)
      && currentPriority == rhs.currentPriority
      && checkOptionalString(currentActivity) == checkOptionalString(rhs.currentActivity)
      && checkOptionalString(currentTapePool) == checkOptionalString(rhs.currentTapePool)
      && nextMountType == rhs.nextMountType
      && checkOptionalString(nextVid) == checkOptionalString(rhs.nextVid)
      && checkOptionalString(nextTapePool) == checkOptionalString(rhs.nextTapePool)
      && nextPriority == rhs.nextPriority
      && checkOptionalString(nextActivity) == checkOptionalString(rhs.nextActivity)
      && checkOptionalString(diskSystemName) == checkOptionalString(rhs.diskSystemName)
      && checkOptionalString(physicalLibraryName) == checkOptionalString(rhs.physicalLibraryName)
      && reservedBytes == rhs.reservedBytes
      && reservationSessionId == rhs.reservationSessionId

      //
      // && devFileName==rhs.devFileName
      // && rawLibrarySlot==rhs.rawLibrarySlot
      //
      // && currentVo==rhs.currentVo
      // && nextVo==rhs.nextVo
      //
      // && userComment==rhs.userComment
      // && creationLog==rhs.creationLog
      // && lastModificationLog==rhs.lastModificationLog
      ;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapeDrive::operator!=(const TapeDrive &rhs) const {
  return !operator==(rhs);
}

void TapeDrive::setState(const std::string& state) {
  this->driveStatus = TapeDrive::stringToState(state);
}

std::string TapeDrive::getStateStr() const {
  return TapeDrive::stateToString(driveStatus);
}

std::string TapeDrive::stateToString(const DriveStatus & state) {
  try {
    return TapeDrive::STATE_TO_STRING_MAP.at(state);
  } catch (std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The state given (") + std::to_string(state) +
      ") does not exist.");
  }
}

DriveStatus TapeDrive::stringToState(const std::string& state) {
  std::string stateUpperCase = state;
  cta::utils::toUpper(stateUpperCase);
  try {
    return TapeDrive::STRING_TO_STATE_MAP.at(stateUpperCase);
  } catch(std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The state given (") + stateUpperCase +
      ") does not exist. Possible values are " + TapeDrive::getAllPossibleStates());
  }
}

void TapeDrive::convertToLogParams(log::ScopedParamContainer& params, const std::string& prefix) const {

  params.add(prefix + "driveName", driveName);
  params.add(prefix + "host", host);
  params.add(prefix + "logicalLibrary", logicalLibrary);
  params.add(prefix + "logicalLibraryDisabled", logicalLibraryDisabled);
  params.add(prefix + "sessionId", sessionId);

  params.add(prefix + "bytesTransferedInSession", bytesTransferedInSession);
  params.add(prefix + "filesTransferedInSession", filesTransferedInSession);

  params.add(prefix + "sessionStartTime", sessionStartTime);
  params.add(prefix + "sessionElapsedTime", sessionElapsedTime);
  params.add(prefix + "mountStartTime", mountStartTime);
  params.add(prefix + "transferStartTime", transferStartTime);
  params.add(prefix + "unloadStartTime", unloadStartTime);
  params.add(prefix + "unmountStartTime", unmountStartTime);
  params.add(prefix + "drainingStartTime", drainingStartTime);
  params.add(prefix + "downOrUpStartTime", downOrUpStartTime);
  params.add(prefix + "probeStartTime", probeStartTime);
  params.add(prefix + "cleanupStartTime", cleanupStartTime);
  params.add(prefix + "startStartTime", startStartTime);
  params.add(prefix + "shutdownTime", shutdownTime);

  params.add(prefix + "mountType", mountType);
  params.add(prefix + "driveStatus", driveStatus);
  params.add(prefix + "desiredUp", desiredUp);
  params.add(prefix + "desiredForceDown", desiredForceDown);
  params.add(prefix + "reasonUpDown", reasonUpDown);

  params.add(prefix + "currentVid", currentVid);
  params.add(prefix + "ctaVersion", ctaVersion);
  params.add(prefix + "currentPriority", currentPriority);
  params.add(prefix + "currentActivity", currentActivity);
  params.add(prefix + "currentTapePool", currentTapePool);
  params.add(prefix + "nextMountType", nextMountType);
  params.add(prefix + "nextVid", nextVid);
  params.add(prefix + "nextTapePool", nextTapePool);
  params.add(prefix + "nextPriority", nextPriority);
  params.add(prefix + "nextActivity", nextActivity);

  params.add(prefix + "devFileName", devFileName);
  params.add(prefix + "rawLibrarySlot", rawLibrarySlot);

  params.add(prefix + "currentVo", currentVo);
  params.add(prefix + "nextVo", nextVo);

  params.add(prefix + "diskSystemName", diskSystemName);
  params.add(prefix + "reservedBytes", reservedBytes);
  params.add(prefix + "reservationSessionId", reservationSessionId);

  params.add(prefix + "physicalLibraryName", physicalLibraryName);

  params.add(prefix + "userComment", userComment);
  params.add(prefix + "creationLog", creationLog);
  params.add(prefix + "lastModificationLog", lastModificationLog);
  params.add(prefix + "physicalLibraryDisabled", physicalLibraryDisabled);
}

} // namespace cta::common::dataStructures
