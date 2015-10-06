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
#include "scheduler/RetrieveRequest.hpp"

#include <list>
#include <string>

namespace cta {

// Forward declarations
class TapeFileLocation;

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
   * @param archiveFile The source archive file.
   * @param tapeCopies The physical location(s) of the archive file on tape.
   * @param remoteFile The URL of the destination remote file.
   * @param priority The priority of the request.
   * @param creationLog The creation log parameters
   */
  RetrieveToFileRequest(
    const cta::ArchiveFile &archiveFile,
    const std::list<TapeFileLocation> &tapeCopies,
    const std::string &remoteFile,
    const uint64_t priority,
    const CreationLog & creationLog);

  /**
   * Returns the source archive file.
   *
   * @return The source archive file.
   */
  const cta::ArchiveFile &getArchiveFile() const throw();
  
  /**
   * Returns the physical location(s) of the archive file on tape.
   *
   * @return The physical location(s) of the archive file on tape.
   */
  const std::list<TapeFileLocation> &getTapeCopies() const throw();

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
  cta::ArchiveFile m_archiveFile;
  
  /**
   * The physical location(s) of the archive file on tape.
   */
  std::list<TapeFileLocation> m_tapeCopies;

  /**
   * The URL of the destination remote file.
   */
  std::string m_remoteFile;

}; // class RetrieveToFileRequest

} // namespace cta
