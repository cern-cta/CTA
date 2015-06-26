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

#include "common/RemotePath.hpp"
#include "remotens/RemoteFileStatus.hpp"

namespace cta {

/**
 * An abstract proxy class specifying the interface to the name space of a
 * remote storage system.
 */
class RemoteNS {
public:

  /**
   * Destructor.
   */
  virtual ~RemoteNS() throw() = 0;

  /**
   * Returns the status of the specified file or directory within the remote
   * storage system.
   *
   * @param path The absolute path of the file or directory.
   * @return The status of the file or directory.
   */
  virtual RemoteFileStatus statFile(const RemotePath &path) const = 0;

  /**
   * Returns true if the specified regular file exists.
   *
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  virtual bool regularFileExists(const RemotePath &path) const = 0;

  /**
   * Returns true if the specified directory exists.
   *
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  virtual bool dirExists(const RemotePath &path) const = 0;

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @param remoteFile The current path.
   * @param newRemoteFile The new path.
   */
  virtual void rename(const RemotePath &remoteFile,
    const RemotePath &newRemoteFile) = 0;

}; // class RemoteNS

} // namespace cta
