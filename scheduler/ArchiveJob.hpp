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

#include "scheduler/TapeJob.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing the transfer of a single copy of a remote file to tape.
 */
class ArchiveJob: public TapeJob {
public:

  /**
   * Constructor.
   */
  ArchiveJob();

  /**
   * Destructor.
   */
  ~ArchiveJob() throw();

  /**
   * Constructor.
   *
   * @param tapePoolName The name of the destination tape pool.
   * @param id The identifier of the tape job.
   * @param userRequestId The identifier of the associated user request.
   * @param copyNb The copy number.
   * @param remoteFile The URL of the remote source file.
   */
  ArchiveJob(
    const std::string &tapePoolName,
    const std::string &id,
    const std::string &userRequestId,
    const uint32_t copyNb,
    const std::string &remoteFile);

  /**
   * Returns the name of the destination tape pool.
   *
   * @return the name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

private:

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

}; // class ArchiveJob

} // namespace cta
