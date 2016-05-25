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

#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "scheduler/UserArchiveRequest.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * A user request to archive a remote file to a copy on tape.
 */
struct ArchiveToTapeCopyRequest: public UserArchiveRequest {

  /**
   * Constructor.
   */
  ArchiveToTapeCopyRequest();

  /**
   * Destructor.
   */
  ~ArchiveToTapeCopyRequest() throw();

  /**
   * Constructor.
   *
   * @param diskFileID The ID of the remote file to be archived.
   * @param archiveFileID The ID of the archive file
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param priority The priority of the request.
   * @param requester The identity of the user who made the request.
   * @param entryLog log for the creation of the request.
   */
  ArchiveToTapeCopyRequest(
    const std::string & diskFileID,
    const uint64_t archiveFileID,
    const uint16_t copyNb,
    const std::string tapePoolName,
    const uint64_t priority, 
    const common::dataStructures::EntryLog & entryLog);

  /**
   * The ID of the remote file to be archived.
   */
  std::string diskFileID;

  /**
   * The full path of the source archive file.
   */
  uint64_t archiveFileID;

  /**
   * The tape copy number.
   */
  uint16_t copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string tapePoolName;

}; // class ArchiveToTapeCopyRequest

} // namespace cta
