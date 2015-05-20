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

#include "scheduler/RemoteStorage.hpp"

namespace cta {

/**
 * Proxy class for a remote EOS storage system.
 */
class EosRemoteStorage: public RemoteStorage {
public:

  /**
   * Destructor.
   */
  ~EosRemoteStorage() throw();

  /**
   * Returns true if the specified remote file exists or false if not.
   *
   * @param remoteFile The URL of the remote file.
   * @return true if the specified remote file exists or false if not.
   */
  bool fileExists(const std::string &remoteFile);

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @param remoteFile The current URL of the remote file.
   * @param newRemoteFile The new URL of the remote file.
   */
  void rename(const std::string &remoteFile,
    const std::string &newRemoteFile);

}; // class EosRemoteStorage

} // namespace cta
