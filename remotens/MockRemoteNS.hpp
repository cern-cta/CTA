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

#include "common/RemoteFileStatus.hpp"
#include "remotens/RemoteNS.hpp"

#include <map>

namespace cta {

/**
 * A mock proxy class for the namespace of a remote storage system.
 */
class MockRemoteNS: public RemoteNS {
public:

  /**
   * Destructor.
   */
  ~MockRemoteNS() throw();
  
  /**
   * Constructor.
   */
  MockRemoteNS();
  
  /**
   * Constructor.
   */
  MockRemoteNS(std::map<RemotePath, RemoteFileStatus> &entries);

  /**
   * Returns the status of the specified file or directory within the remote
   * storage system or NULL if the fil eor directory does not exist.
   *
   * @param path The absolute path of the file or directory.
   * @return The status of the file or directory or NULL if the file or
   * directory does not exist.
   */
  std::unique_ptr<RemoteFileStatus> statFile(const RemotePath &path) const;

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @param remoteFile The current path.
   * @param newRemoteFile The new path.
   */
  void rename(const RemotePath &remoteFile,
    const RemotePath &newRemoteFile);

  /**
   * Parses the specified remote file and returns the file name.
   *
   * @param path The absolute path of the file.
   * @return The file name.
   */
  std::string getFilename(const RemotePath &remoteFile) const;
  
  /**
   * Adds a directory or file to the remote NS with the desired status
   * 
   * @param status The desired status of the entry to be created
   * @param path The absolute path of the file or directory
   */
  void createEntry(const RemotePath &path, const RemoteFileStatus &status);
  
private:
  
  /**
   * The fake filesystem entries (dirs and files)
   */
  std::map<RemotePath, RemoteFileStatus> m_entries;

}; // class MockRemoteNS

} // namespace cta
