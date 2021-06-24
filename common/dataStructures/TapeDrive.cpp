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
  driveStatus(TapeDrive::State::UNKNOWN),
  desiredUp(false),
  desiredForceDown(false)
  {}

const std::map<TapeDrive::State,std::string> TapeDrive::STATE_TO_STRING_MAP = {
  {TapeDrive::State::UNKNOWN,"UNKNOWN"},
  {TapeDrive::State::DOWN,"DOWN"},
  {TapeDrive::State::UP,"UP"},
  {TapeDrive::State::PROBING,"PROBING"},
  {TapeDrive::State::STARTING,"STARTING"},
  {TapeDrive::State::MOUNTING,"MOUNTING"},
  {TapeDrive::State::TRANSFERING,"TRANSFERING"},
  {TapeDrive::State::UNLOADING,"UNLOADING"},
  {TapeDrive::State::UNMOUNTING,"UNMOUNTING"},
  {TapeDrive::State::DRAININGTODISK,"DRAININGTODISK"},
  {TapeDrive::State::CLEANINGUP,"CLEANINGUP"},
  {TapeDrive::State::SHUTDOWN,"SHUTDOWN"}
};

const std::map<std::string,TapeDrive::State> TapeDrive::STRING_TO_STATE_MAP = {
  {"UNKNOWN",TapeDrive::State::UNKNOWN},
  {"DOWN",TapeDrive::State::DOWN},
  {"UP",TapeDrive::State::UP},
  {"PROBING",TapeDrive::State::PROBING},
  {"STARTING",TapeDrive::State::STARTING},
  {"MOUNTING",TapeDrive::State::MOUNTING},
  {"TRANSFERING",TapeDrive::State::TRANSFERING},
  {"UNLOADING",TapeDrive::State::UNLOADING},
  {"UNMOUNTING",TapeDrive::State::UNMOUNTING},
  {"DRAININGTODISK",TapeDrive::State::DRAININGTODISK},
  {"CLEANINGUP",TapeDrive::State::CLEANINGUP},
  {"SHUTDOWN",TapeDrive::State::SHUTDOWN}
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
  return driveName==rhs.driveName
      && host==rhs.host
      && logicalLibrary==rhs.logicalLibrary
      && mountType==rhs.mountType
      && driveStatus==rhs.driveStatus
      && desiredUp==rhs.desiredUp
      && desiredForceDown==rhs.desiredForceDown
      && diskSystemName==rhs.diskSystemName
      && reservedBytes==rhs.reservedBytes

      && sessionId==rhs.sessionId
      && bytesTransferedInSession==rhs.bytesTransferedInSession
      && filesTransferedInSession==rhs.filesTransferedInSession
      && latestBandwidth==rhs.latestBandwidth
      && sessionStartTime==rhs.sessionStartTime
      && mountStartTime==rhs.mountStartTime
      && transferStartTime==rhs.transferStartTime
      && unloadStartTime==rhs.unloadStartTime
      && unmountStartTime==rhs.unmountStartTime
      && drainingStartTime==rhs.drainingStartTime
      && downOrUpStartTime==rhs.downOrUpStartTime
      && probeStartTime==rhs.probeStartTime
      && cleanupStartTime==rhs.cleanupStartTime
      && startStartTime==rhs.startStartTime
      && shutdownTime==rhs.shutdownTime

      && reasonUpDown==rhs.reasonUpDown
      && currentVid==rhs.currentVid
      && ctaVersion==rhs.ctaVersion
      && currentPriority==rhs.currentPriority
      && currentActivity==rhs.currentActivity
      && currentActivityWeight==rhs.currentActivityWeight
      && currentTapePool==rhs.currentTapePool
      && nextMountType==rhs.nextMountType
      && nextVid==rhs.nextVid
      && nextTapePool==rhs.nextTapePool
      && nextPriority==rhs.nextPriority
      && nextActivity==rhs.nextActivity
      && nextActivityWeight==rhs.nextActivityWeight
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

std::string TapeDrive::stateToString(const TapeDrive::State & state) {
  try {
    return TapeDrive::STATE_TO_STRING_MAP.at(state);
  } catch (std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The state given (") + std::to_string(state) +
      ") does not exist.");
  }
}

TapeDrive::State TapeDrive::stringToState(const std::string& state) {
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
