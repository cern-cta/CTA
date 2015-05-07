/**
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

#include <string>

namespace cta {

/**
 * Abstract class specifying the interface to a remote storage system.
 */
class RemoteStorage {
public:

  /**
   * Destructor.
   */
  virtual ~RemoteStorage() throw() = 0;

  /**
   * Returns true if the specified remote file exists or false if not.
   *
   * @param remoteFile The URL of the remote file.
   * @return true if the specified remote file exists or false if not.
   */
  virtual bool fileExists(const std::string &remoteFile) = 0;

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @param remoteFile The current URL of the remote file.
   * @param newRemoteFile The new URL of the remote file.
   */
  virtual void rename(const std::string &remoteFile,
    const std::string &newRemoteFile) = 0;

}; // class RemoteStorage

} // namespace cta
