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

#include "scheduler/ArchiveRequest.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * A user request to archive a remote file to a copy on tape.
 */
class ArchiveToTapeCopyRequest: public ArchiveRequest {
public:

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
   * @param remoteFile The URL of the source remote file to be archived.
   * @param archiveFile The full path of the destination archive file.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param priority The priority of the request.
   * @param requester The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  ArchiveToTapeCopyRequest(
    const std::string &remoteFile,
    const std::string &archiveFile,
    const uint16_t copyNb,
    const std::string tapePoolName,
    const uint64_t priority, 
    const CreationLog & creationLog);

  /**
   * Returns the URL of the source remote file to be archived.
   *
   * @return The URL of the source remote file to be archived.
   */
  const std::string &getRemoteFile() const throw();

  /**
   * Returns the full path of the destination archive file.
   *
   * @return The full path of the destination archive file.
   */
  const std::string &getArchiveFile() const throw();

  /**
   * Returns the tape copy number.
   *
   * @return The tape copy number.
   */
  uint16_t getCopyNb() const throw();

  /**
   * Returns the name of the destination tape pool.
   *
   * @return the name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

private:

  /**
   * The URL of the destination remote file to be archived.
   */
  std::string m_remoteFile;

  /**
   * The full path of the source archive file.
   */
  std::string m_archiveFile;

  /**
   * The tape copy number.
   */
  uint16_t m_copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

}; // class ArchiveToTapeCopyRequest

} // namespace cta
