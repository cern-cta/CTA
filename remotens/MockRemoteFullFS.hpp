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

#include "common/remoteFS/RemoteFileStatus.hpp"
#include "remotens/RemoteNS.hpp"
#include "tapeserver/castor/tape/tapeserver/utils/Regex.hpp"
#include <sys/stat.h>
#include <ftw.h>

namespace cta {

/**
 * A mock proxy class for the namespace of a remote storage system, backed
 * by a real local temporary file system. This mock namespace also generates
 * valid URLs in order for the tape server to be able to actually transfer
 * data.
 */
class MockRemoteFullFS: public RemoteNS {
public:

  /**
   * Destructor.
   */
  ~MockRemoteFullFS() throw();
  
  /**
   * Constructor.
   */
  MockRemoteFullFS();

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
   * Get a real URL that will be compatible with both remoteNS interface for
   * metadata access and tape server's file access (via a file:// URL).
   */
  std::string createFullURL(const std::string & relativePath);
  
  /**
   * Create a file with random content
   */
  void createFile(const std::string & URL, size_t size);
  
private:
  std::string m_basePath; ///< The path to the directory within which all the URLs will be created
  std::unique_ptr<castor::tape::utils::Regex> m_pathRegex; ///< A regex alloging validation of the path in calls
  /** Utility function to delete a directory in depth */
  static int deleteFileOrDirectory(const char* fpath,
        const struct ::stat* sb, int tflag, struct ::FTW * ftwbuf);
}; // class MockRemoteNS

} // namespace cta
