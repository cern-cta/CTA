/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/RepackInfo.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

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
      throw cta::exception::Exception("The status "+toString(status)+" has no corresponding queue type.");
    default:
      throw cta::exception::Exception("The status "+toString(status)+" has no corresponding queue type.");
  }
}

} // namespace cta::common::dataStructures
