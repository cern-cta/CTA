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
#include <string>

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/TapeFile.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * The retrieve job contains the original request, and all data needed to queue 
 * the request in the system 
 */
struct RetrieveJob {

  RetrieveJob();

  bool operator==(const RetrieveJob &rhs) const;

  bool operator!=(const RetrieveJob &rhs) const;

  RetrieveRequest request;
  uint64_t fileSize;
  std::map<std::string,std::pair<uint32_t,TapeFile>> tapeCopies;
  std::string objectId; //!< Objectstore address, provided when reporting a failed job
  std::list<std::string> failurelogs;

}; // struct RetrieveJob

std::ostream &operator<<(std::ostream &os, const RetrieveJob &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
