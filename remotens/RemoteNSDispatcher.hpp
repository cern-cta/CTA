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

#include "remotens/RemoteNS.hpp"

#include <map>
#include <memory>
#include <string>

namespace cta {

/**
 * Dispatches remote namespace requests to the appropriate proxy objects based
 * on the of the protocol of the request.
 */
class RemoteNSDispatcher: public RemoteNS {
public:

  /**
   * Destructor.
   */
  ~RemoteNSDispatcher() throw();

  /**
   * Registers the specified proxy object with the specified protocol.
   *
   * @param protocol The name of the protocol as it appears URLS.  For example
   * the protocol name of the URL "xroot://my_dir/m_file" would be "xroot".
   * @param handler The proxy object to the namespace of teh remote storage
   * system.
   */
  void registerProtocolHandler(const std::string &protocol,
    std::unique_ptr<RemoteNS> handler);

  /**
   * Returns the status of the specified file or directory within the remote
   * storage system.
   *
   * @param path The absolute path of the file or directory.
   * @return The status of the file or directory.
   */
  RemoteFileStatus statFile(const RemotePath &path) const;

  /**
   * Returns true if the specified regular file exists.
   *
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  bool regularFileExists(const RemotePath &path) const;

  /**
   * Returns true if the specified directory exists.
   *
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  bool dirExists(const RemotePath &path) const;

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

private:

  /**
   * Mapping from protocol name to protocol handler.
   */
  std::map<std::string, RemoteNS*> m_handlers;

  /**
   * Returns the proxy object to be used to handle the specified protocol.
   *
   * @param protocol The name of the protocol.
   */
  RemoteNS &getHandler(const std::string &protocol);

  /**
   * Returns the proxy object to be used to handle the specified protocol.
   *
   * @param protocol The name of the protocol.
   */
  const RemoteNS &getHandler(const std::string &protocol) const;

}; // class RemoteNSDispatcher

} // namespace cta
