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

#include "common/archiveNS/ArchiveFileStatus.hpp"
#include "common/SecurityIdentity.hpp"
#include "nameserver/NameServerTapeFile.hpp"

#include <list>
#include <memory>
#include <string>

namespace cta {

/**
 * Abstract class specifying the interface to the name server containing the
 * archive namespace.
 */
class NameServer {
public:

  /**
   * Destructor.
   */
  virtual ~NameServer() throw() = 0;

  /**
   * Add the specified tape file entry to the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @param nameServerTapeFile The tape file entry.
   */
  virtual void addTapeFile(
    const SecurityIdentity &cliIdentity,
    const std::string &path,
    const NameServerTapeFile &tapeFile) = 0;
  
}; // class NameServer

} // namespace 
