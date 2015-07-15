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

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class representing the transfer of a single copy of file.
 */
class TapeJob {
public:

  /**
   * Constructor.
   */
  TapeJob();

  /**
   * Destructor.
   */
  virtual ~TapeJob() throw() = 0;

  /**
   * Constructor.
   *
   * @param id The identifier of the tape job.
   * @param userRequestId The identifier of the associated user request.
   * @param copyNb The copy number.
   * @param remoteFile The URL of the remote file that depending on the
   * direction of the data transfer may be either the source or the
   * destination of the tape job.
   */
  TapeJob(
    const std::string &id,
    const std::string &userRequestId,
    const uint32_t copyNb,
    const std::string &remoteFile);

  /**
   * Returns the identifier of the tape job.
   *
   * @return The identifier of the tape job.
   */
  const std::string &getId() const throw();

  /**
   * Returns the identifier of the associated user request.
   *
   * @return The identifier of the associated user request.
   */
  const std::string &getUserRequestId() const throw();

  /**
   * Returns the copy number.
   *
   * @return The copy number.
   */
  uint32_t getCopyNb() const throw();

  /**
   * Returns the URL of the remote file that depending on the direction of the
   * data transfer may be either the source or the destination of the file
   * transfer.
   *
   * @return The URL of the remote file that depending on the direction of the
   * data transfer may be either the source or the destination of the file
   * transfer.
   */
  const std::string &getRemoteFile() const throw();

private:

  /**
   * The identifier of the tape job.
   */
  std::string m_id;

  /**
   * The identifier of the associated user request.
   */
  std::string m_userRequestId;

  /**
   * The copy number.
   */
  uint32_t m_copyNb;

  /**
   * The URL of the remote file that depending on the direction of the data
   * transfer may be either the source or the destination of the tape job.
   */
  std::string m_remoteFile;

}; // class TapeJob

} // namespace cta
