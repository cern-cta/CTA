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

#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveRoute::ArchiveRoute():
  copyNb(0), type(ArchiveRoute::Type::DEFAULT) {}


const std::map<ArchiveRoute::Type,std::string> ArchiveRoute::TYPE_TO_STRING_MAP = {
        {ArchiveRoute::Type::DEFAULT,"DEFAULT"},
        {ArchiveRoute::Type::REPACK,"REPACK"},
};

const std::map<std::string,ArchiveRoute::Type> ArchiveRoute::STRING_TO_TYPE_MAP = {
        {"DEFAULT",ArchiveRoute::Type::DEFAULT},
        {"REPACK",ArchiveRoute::Type::REPACK},
};

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool ArchiveRoute::operator==(const ArchiveRoute &rhs) const {
  return storageClassName==rhs.storageClassName
      && copyNb==rhs.copyNb
      && tapePoolName==rhs.tapePoolName
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool ArchiveRoute::operator!=(const ArchiveRoute &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const ArchiveRoute &obj) {
  os << "(storageClassName=" << obj.storageClassName
     << " copyNb=" << obj.copyNb
     << " tapePoolName=" << obj.tapePoolName
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

std::string ArchiveRoute::typeToString(const ArchiveRoute::Type & type) {
  try {
    return TYPE_TO_STRING_MAP.at(type);
  } catch (std::out_of_range &ex){
    throw cta::exception::Exception(std::string("The type given (") + std::to_string(type) + ") does not exist.");
  }
}

ArchiveRoute::Type ArchiveRoute::stringToType(const std::string& typeStr) {
  std::string typeUpperCase = typeStr;
  cta::utils::toUpper(typeUpperCase);
  try {
    return STRING_TO_TYPE_MAP.at(typeUpperCase);
  } catch(std::out_of_range &ex){
    throw cta::exception::UserError(std::string("The type given (") + typeUpperCase + ") does not exist. Possible values are " + ArchiveRoute::getAllPossibleTypes());
  }
}

std::string ArchiveRoute::getAllPossibleTypes(){
  std::string ret;
  for(auto &kv: STRING_TO_TYPE_MAP){
    ret += kv.first + " ";
  }
  if(ret.size())
    ret.pop_back();
  return ret;
}

} // namespace cta::common::dataStructures
