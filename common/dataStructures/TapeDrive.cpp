/*
 * @project        The CERN TapeDrive Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeDrive::TapeDrive():
  driveStatus(DriveStatus::Unknown),
  desiredUp(false),
  desiredForceDown(false),
  nextMountType(MountType::NoMount),
  reservedBytes(0) {}

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
    ret = ret.substr(0,ret.size() - 1);
  return ret;
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeDrive::operator==(const TapeDrive &rhs) const {
  auto checkOptionalString = [](const optional<std::string> &str) -> std::string {
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
      && diskSystemName == rhs.diskSystemName
      && reservedBytes == rhs.reservedBytes

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

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeDrive &obj) {
  std::string stateStr = "UNKNOWN";
  try {
    stateStr = TapeDrive::stateToString(obj.driveStatus);
  } catch(const cta::exception::Exception &ex){
    //Do nothing
  }
  os << "(driveName=" << obj.driveName
     << " host=" << obj.host
     << " logicalLibrary=" << obj.logicalLibrary
     << " mountType=" << obj.mountType
     << " driveStatus=" << stateStr
     << " desiredUp=" << obj.desiredUp
     << " desiredForceDown=" << obj.desiredForceDown
     << " diskSystemName=" << obj.diskSystemName
     << " reservedBytes=" << obj.reservedBytes << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
