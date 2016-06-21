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

#include "common/UserIdentity.hpp"
#include "nameserver/NameServer.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

// The header file for atomic was is actually called cstdatomic in gcc 4.4
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
  #include <atomic>
#endif

#include <list>
#include <mutex>
#include <string>

namespace cta {

/**
 * Local file system implementation of a name server to contain the archive
 * namespace.
 */
class MockNameServer: public NameServer {

public:

  /**
   * Destructor.
   */
  ~MockNameServer();

  /**
   * Add the specified tape file entry to the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @param nameServerTapeFile The tape file entry.
   */
  void addTapeFile(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &path,
    const NameServerTapeFile &tapeFile);

}; // class MockNameServer

} // namespace cta
