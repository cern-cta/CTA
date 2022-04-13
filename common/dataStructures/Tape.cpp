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

#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Tape::Tape():
  lastFSeq(0),
  capacityInBytes(0), 
  dataOnTapeInBytes(0),
  nbMasterFiles(0),
  masterDataInBytes(0),
  full(false),
  state(Tape::State::ACTIVE)
  {}

const std::map<Tape::State,std::string> Tape::STATE_TO_STRING_MAP = {
  {Tape::State::ACTIVE,"ACTIVE"},
  {Tape::State::BROKEN,"BROKEN"},
  {Tape::State::BROKEN_PENDING,"BROKEN_PENDING"},
  {Tape::State::DISABLED,"DISABLED"},
  {Tape::State::REPACKING,"REPACKING"},
  {Tape::State::REPACKING_PENDING,"REPACKING_PENDING"},
};

const std::map<std::string,Tape::State> Tape::STRING_TO_STATE_MAP = {
  {"ACTIVE",Tape::State::ACTIVE},
  {"BROKEN",Tape::State::BROKEN},
  {"BROKEN_PENDING",Tape::State::BROKEN_PENDING},
  {"DISABLED",Tape::State::DISABLED},
  {"REPACKING",Tape::State::REPACKING},
  {"REPACKING_PENDING",Tape::State::REPACKING_PENDING}
};

std::string Tape::getAllPossibleStates(){
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
bool Tape::operator==(const Tape &rhs) const {
  return vid==rhs.vid
      && lastFSeq==rhs.lastFSeq
      && logicalLibraryName==rhs.logicalLibraryName
      && tapePoolName==rhs.tapePoolName
      && capacityInBytes==rhs.capacityInBytes
      && dataOnTapeInBytes==rhs.dataOnTapeInBytes
      && encryptionKeyName==rhs.encryptionKeyName
      && full==rhs.full
      && state==rhs.state
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment
      && labelLog==rhs.labelLog
      && lastWriteLog==rhs.lastWriteLog
      && lastReadLog==rhs.lastReadLog;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool Tape::operator!=(const Tape &rhs) const {
  return !operator==(rhs);
}

void Tape::setState(const std::string& state) {
  this->state = Tape::stringToState(state);
}

std::string Tape::getStateStr() const {
  return Tape::stateToString(state);
}

std::string Tape::stateToString(const Tape::State & state) {
  try {
    return Tape::STATE_TO_STRING_MAP.at(state);
  } catch (std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The state given (") + std::to_string(state) + ") does not exist.");
  }
}

Tape::State Tape::stringToState(const std::string& state) {
  std::string stateUpperCase = state;
  cta::utils::toUpper(stateUpperCase);
  try {
    return Tape::STRING_TO_STATE_MAP.at(stateUpperCase);
  } catch(std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The state given (") + stateUpperCase + ") does not exist. Possible values are " + Tape::getAllPossibleStates());
  }
}

bool Tape::isDisabled() const {
  return state == Tape::State::DISABLED;
}

bool Tape::isRepacking() const {
  return state == Tape::State::REPACKING;
}

bool Tape::isBroken() const {
  return state == Tape::State::BROKEN;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const Tape &obj) {
  std::string stateStr = "UNKNOWN";
  try {
    stateStr = Tape::stateToString(obj.state);
  } catch(const cta::exception::Exception &ex){
    //Do nothing
  }
  os << "(vid=" << obj.vid
     << " lastFSeq=" << obj.lastFSeq
     << " logicalLibraryName=" << obj.logicalLibraryName
     << " tapePoolName=" << obj.tapePoolName
     << " capacityInBytes=" << obj.capacityInBytes
     << " dataOnTapeInBytes=" << obj.dataOnTapeInBytes
     << " encryptionKeyName=" << (obj.encryptionKeyName ? obj.encryptionKeyName.value() : "null")
     << " full=" << obj.full 
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment
     << " state=" << stateStr
     << " stateReason=" << (obj.stateReason ? obj.stateReason.value() : "null")
     << " stateUpdateTime=" << obj.stateUpdateTime
     << " stateModifiedBy=" << obj.stateModifiedBy
     << " labelLog=" << obj.labelLog
     << " lastWriteLog=" << obj.lastWriteLog
     << " lastReadLog=" << obj.lastReadLog << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
