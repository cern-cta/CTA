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
  disabled(false),
  readOnly(false)
  {}

const std::map<Tape::State,std::string> Tape::STATE_TO_STRING_MAP = {
  {Tape::State::ACTIVE,"ACTIVE"},
  {Tape::State::BROKEN,"BROKEN"},
  {Tape::State::DISABLED,"DISABLED"}
};

const std::map<std::string,Tape::State> Tape::STRING_TO_STATE_MAP = {
  {"ACTIVE",Tape::State::ACTIVE},
  {"BROKEN",Tape::State::BROKEN},
  {"DISABLED",Tape::State::DISABLED}
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
      && disabled==rhs.disabled
      && readOnly==rhs.readOnly
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


//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const Tape &obj) {
  os << "(vid=" << obj.vid
     << " lastFSeq=" << obj.lastFSeq
     << " logicalLibraryName=" << obj.logicalLibraryName
     << " tapePoolName=" << obj.tapePoolName
     << " capacityInBytes=" << obj.capacityInBytes
     << " dataOnTapeInBytes=" << obj.dataOnTapeInBytes
     << " encryptionKeyName=" << (obj.encryptionKeyName ? obj.encryptionKeyName.value() : "null")
     << " full=" << obj.full
     << " disabled=" << obj.disabled
     << " readOnly=" << obj.readOnly    
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment
     << " labelLog=" << obj.labelLog
     << " lastWriteLog=" << obj.lastWriteLog
     << " lastReadLog=" << obj.lastReadLog << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
