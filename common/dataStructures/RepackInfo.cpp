/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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
