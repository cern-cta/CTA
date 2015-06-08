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

#include "scheduler/ArchivalRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to archive one or more remote files to an
 * archive directory.
 */
class ArchiveToDirRequest: public ArchivalRequest {
public:

  /**
   * Constructor.
   */
  ArchiveToDirRequest();

  /**
   * Destructor.
   */
  ~ArchiveToDirRequest() throw();

  /**
   * Constructor.
   *
   * @param archiveDir The full path of the destination archive directory.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  ArchiveToDirRequest(
    const std::string &archiveDir,
    const uint64_t priority,
    const SecurityIdentity &user,
    const time_t creationTime = time(NULL));

  /**
   * Returns the full path of the destination archive directory.
   *
   * @return The full path of the destination archive directory.
   */
  const std::string &getArchiveDir() const throw();

  /**
   * Returns the list of the individual archive to individual file requests
   * that make up this archive to directory request.
   *
   * @return The list of the individual archive to file requests that make up
   * this archive to directory request.
   */
  const std::list<ArchiveToFileRequest> &getArchiveToFileRequests() const
    throw();

  /**
   * Returns the list of the individual archive to file requests that make up
   * this archive to directory request.
   *
   * @return The list of the individual archive to file requests that make up
   * this archive to directory request.
   */
  std::list<ArchiveToFileRequest> &getArchiveToFileRequests() throw();

private:

  /**
   * The full path of the destination archive directory.
   */
  std::string m_archiveDir;

  /**
   * The list of the individual archive to file requests that make up this
   * archive to directory request.
   */
  std::list<ArchiveToFileRequest> m_archiveToFileRequests;

}; // class ArchiveToDirRequest

} // namespace cta
