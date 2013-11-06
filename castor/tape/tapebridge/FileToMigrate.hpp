/******************************************************************************
 *                castor/tape/tapebridge/FileToMigrate.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_FILETOMIGRATE_HPP
#define CASTOR_TAPE_TAPEBRIDGE_FILETOMIGRATE_HPP 1

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * A file to be recalled from tape.
 */
struct FileToMigrate {

  /**
   * Constructor that initialises all integer member-values to 0 and string
   * member-values to empty strings.
   */
  FileToMigrate();

  uint64_t fileTransactionId;

  std::string nsHost;

  uint64_t fileId;

  int32_t fseq;

  int32_t positionMethod;

  uint64_t fileSize;

  std::string lastKnownFilename;

  uint64_t lastModificationTime;

  std::string path;

  int32_t umask;
}; // struct FileToMigrate

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_FILETOMIGRATE_HPP
