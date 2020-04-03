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

#include "common/optional.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This is a request to delete an existing archive file or to cancel and ongoing 
 * archival 
 */
struct DeleteArchiveRequest {

  DeleteArchiveRequest();

  bool operator==(const DeleteArchiveRequest &rhs) const;

  bool operator!=(const DeleteArchiveRequest &rhs) const;

  RequesterIdentity requester;
  uint64_t archiveFileID;
  cta::optional<std::string> address;

}; // struct DeleteArchiveRequest

std::ostream &operator<<(std::ostream &os, const DeleteArchiveRequest &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
