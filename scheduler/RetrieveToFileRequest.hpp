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

#include <list>
#include <string>

namespace cta {

// Forward declarations
class TapeCopyInfo;

/**
 * Class representing a user request to retrieve a single archived file to a
 * single remote file.
 */
class RetrieveToFileRequest: public RetrieveRequest {
public:

  /**
   * Constructor.
   */
  RetrieveToFileRequest();

  /**
   * Destructor.
   */
  ~RetrieveToFileRequest() throw();

  /**
   * Constructor.
   *
   * @param archiveFile The full path of the source archive file.
   * @param tapeCopies The physical location(s) of the archive file on tape.
   * @param remoteFile The URL of the destination remote file.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  RetrieveToFileRequest(
    const std::string &archiveFile,
    const uint64_t size,
    const std::list<TapeCopyInfo> &tapeCopies,
    const std::string &remoteFile,
    const uint64_t priority,
    const CreationLog & creationLog);

  /**
   * Returns the full path of the source archive file.
   *
   * @return The full path of the source archive file.
   */
  const std::string &getArchiveFile() const throw();

  /**
   * Returns the size of the source archive file.
   *
   * @return The size of the source archive file.
   */
  uint64_t getSize() const throw();
  
  /**
   * Returns the physical location(s) of the archive file on tape.
   *
   * @return The physical location(s) of the archive file on tape.
   */
  const std::list<TapeCopyInfo> &getTapeCopies() const throw();

  /**
   * Returns the URL of the destination remote file.
   *
   * @return The URL of the destination remote file.
   */
  const std::string &getRemoteFile() const throw();

private:

  /**
   * The full path of the source archive file.
   */
  std::string m_archiveFile;

  /**
   * The file size
   */
  uint64_t m_size;
  /**
   * The physical location(s) of the archive file on tape.
   */
  std::list<TapeCopyInfo> m_tapeCopies;

  /**
   * The URL of the destination remote file.
   */
  std::string m_remoteFile;

}; // class RetrieveToFileRequest

} // namespace cta
