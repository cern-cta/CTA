/******************************************************************************
 *                castor/tape/tapebridge/RequestToMigrateFile.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_REQUESTTOMIGRATEFILE_HPP
#define CASTOR_TAPE_TAPEBRIDGE_REQUESTTOMIGRATEFILE_HPP 1

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * A request to migrate a file to tape that was recieved from the client
 * (either tapegatewayd or writetp).
 */
struct RequestToMigrateFile {
  uint64_t    fileTransactionId;
  std::string nsHost;
  uint64_t    nsFileId;
  int32_t     positionCommandCode;
  int32_t     tapeFSeq;
  uint64_t    fileSize;
  std::string lastKnownFilename;
  uint64_t    lastModificationTime;
  std::string path;
  int         umask;

  /**
   * Constructor.
   */
  RequestToMigrateFile():
    fileTransactionId(0),
    nsHost(""),
    nsFileId(0),
    positionCommandCode(0),
    tapeFSeq(0),
    fileSize(0),
    lastKnownFilename(""),
    lastModificationTime(0),
    path(""),
    umask(0) {
    // Do nothing
  }
}; // struct RequestToMigrateFile

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_REQUESTTOMIGRATEFILE_HPP
