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

#include "common/archiveNS/ArchiveFile.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"
#include "common/CreationLog.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to retrieve a single archive file to a 
 * single remote file.
 */
struct RetrieveRequestDump {
  uint64_t priority; /**< The priority of the request. */
  common::dataStructures::EntryLog entryLog; /**< The time at which the request was created. */
  cta::common::archiveNS::ArchiveFile archiveFile; /**< he full path of the source archive file. */
  uint64_t activeCopyNb; /**< The tape copy number currenty considered for retrieve. */
  std::list<TapeFileLocation> tapeCopies; /**<The location of the copies on tape. */
  std::string remoteFile; /**< The URL of the destination remote file. */
}; // struct RetrieveFromTapeCopyRequest

} // namespace cta
