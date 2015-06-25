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

#include "scheduler/RetrieveRequest.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to retrieve one or more archived files to a
 * remote directory.
 */
class RetrieveToDirRequest: public RetrieveRequest {
public:

  /**
   * Constructor.
   */
  RetrieveToDirRequest();

  /**
   * Destructor.
   */
  ~RetrieveToDirRequest() throw();

  /**
   * Constructor.
   *
   * @param remoteDir The URL of the destination remote directory.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  RetrieveToDirRequest(
    const std::string &remoteDir,
    const uint64_t priority,
    const CreationLog & creationLog);

  /**
   * Returns the URL of the destination remote directory.
   *
   * @return The URL of the destination remote directory.
   */
  const std::string &getRemoteDir() const throw();

  /**
   * Returns the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   *
   * @return the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   */
  const std::list<RetrieveToFileRequest> &getRetrieveToFileRequests() const
    throw();

  /**
   * Returns the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   *
   * @return the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   */
  std::list<RetrieveToFileRequest> &getRetrieveToFileRequests() throw();

private:

  /**
   * The URL of the destination remote directory.
   */
  std::string m_remoteDir;

  /**
   * The list of the individual retrieve to file requests that make up this
   * retrieve to directory request.
   */
  std::list<RetrieveToFileRequest> m_retrieveToFileRequests;

}; // class RetrieveToDirRequest

} // namespace cta
