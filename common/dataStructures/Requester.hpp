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

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This is the disk system user that triggered the request (any of: archival, 
 * retrieval, deletion of an archive file, cancel of an ongoing retrieval, 
 * update of a file metadata, listing of storage classes). It may or may not be 
 * the owner of the file (basically there's no relationship between the two) 
 */
struct Requester {

  Requester();

  bool operator==(const Requester &rhs) const;

  bool operator!=(const Requester &rhs) const;

  std::string name;
  std::string group;
  std::string mountPolicy;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

}; // struct Requester

std::ostream &operator<<(std::ostream &os, const Requester &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
