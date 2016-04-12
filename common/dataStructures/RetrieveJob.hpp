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

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/*
 * The retrieve job contains the original request, and all data needed to queue the request in the system
 */
struct RetrieveJob {

  RetrieveJob();

  bool operator==(const RetrieveJob &rhs) const;

  bool operator!=(const RetrieveJob &rhs) const;

  cta::common::dataStructures::RetrieveRequest request;
  std::map<std::string,std::pair<uint64_t,cta::common::dataStructures::TapeFileLocation>> tapeCopies;

}; // struct RetrieveJob

} // namespace dataStructures
} // namespace common
} // namespace cta

std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::RetrieveJob &obj);
