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

#include "cta/RetrievalJobState.hpp"
#include "cta/UserIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing the job of retrieving a file from tape.
 */
class RetrievalJob {
public:

  /**
   * Constructor.
   */
  RetrievalJob();

  /**
   * Constructor.
   *
   * @param state The state of the retrieval job.
   * @param srcPath The full path of the source file within the archive.
   * @param dstUrl The URL of the destination file.
   * @param creator The identity of the user that created the job.
   * @param creationTime The absolute time when the retrieval job was created.
   */
  RetrievalJob(
    const RetrievalJobState::Enum state,
    const std::string &srcPath,
    const std::string &dstUrl,
    const UserIdentity &creator,
    const time_t creationTime);

  /**
   * Sets the state of the retrieval job.
   */
  void setState(const RetrievalJobState::Enum state);

  /**
   * Return sthe state of the retrieval job.
   *
   * @return sthe state of the retrieval job.
   */
  RetrievalJobState::Enum getState() const throw();

  /**
   * Thread safe method that returns the string representation of the state of
   * the retrieval job.
   *
   * @return The string representation of the state of the retrieval job.
   */
  const char *getStateStr() const throw();

  /**
   * Returns the full path of the source file within the archive.
   *
   * @return The full path of the source file within the archive.
   */
  const std::string &getSrcPath() const throw();

  /**
   * Returns the URL of the destination file.
   *
   * @return The URL of the destination file.
   */
  const std::string &getDstUrl() const throw();

  /**
   * Returns the time when the job was created.
   *
   * @return The time when the job was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the job.
   *
   * @return The identity of the user that created the job.
   */
  const UserIdentity &getCreator() const throw();

private:

  /**
   * The state of the retrieval job.
   */
  RetrievalJobState::Enum m_state;

  /**
   * The full path of the source file within the archive.
   */
  std::string m_srcPath;

  /**
   * The URL of the destination file.
   */
  std::string m_dstUrl;

  /**
   * The time when the job was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the job.
   */
  UserIdentity m_creator;

}; // class FileRetrieval

} // namespace cta
