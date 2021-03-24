/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2021 CERN
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

#include "RepackQueueType.hpp"
#include "common/exception/Exception.hpp"

namespace cta { namespace common { namespace dataStructures {

std::string toString(RepackQueueType queueType) {
  switch(queueType) {
  case RepackQueueType::Pending:
    return "Pending";
  case RepackQueueType::ToExpand:
    return "ToExpand";
  }
  throw exception::Exception("In toString(RepackQueueType): unexpected queue type.");
}

}}} // namespace cta::common::dataStructures
