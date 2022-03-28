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

#include "common/dataStructures/RepackInfo.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

std::string toString(RepackInfo::Type type) {
  switch(type) {
    case RepackInfo::Type::MoveAndAddCopies:
      return "move and add copies";
    case RepackInfo::Type::AddCopiesOnly:
      return "add copies only";
    case RepackInfo::Type::MoveOnly:
      return "move only";
    default:
      return "UNKNOWN";
  }
}

std::string toString(RepackInfo::Status status) {
  switch(status) {
    case RepackInfo::Status::Complete:
      return "Complete";
    case RepackInfo::Status::Failed:
      return "Failed";
     case RepackInfo::Status::Pending:
      return "Pending";
    case RepackInfo::Status::Running:
      return "Running";
    case RepackInfo::Status::Starting:
      return "Starting";
    case RepackInfo::Status::ToExpand:
      return "ToExpand";
  default:
      return "UNKNOWN";
  }
}

RepackQueueType RepackInfo::getQueueType(){
  switch(status){
    case RepackInfo::Status::Pending:
      return RepackQueueType::Pending;
    case RepackInfo::Status::ToExpand:
      return RepackQueueType::ToExpand;
    case RepackInfo::Status::Running:
    case RepackInfo::Status::Starting:
      if(!isExpandFinished) return RepackQueueType::ToExpand;
      goto explicit_default;
    default:
    explicit_default:
      throw cta::exception::Exception("The status "+toString(status)+" has no corresponding queue type.");
  }
}

} // namespace dataStructures
} // namespace common
} // namespace cta
