/******************************************************************************
 *                castor/tape/tapebridge/FileToRecall.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_FILETORECALL_HPP
#define CASTOR_TAPE_TAPEBRIDGE_FILETORECALL_HPP 1

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * A file to be recalled from tape.
 */
struct FileToRecall {

  /**
   * Constructor that initialises all integer member-values to 0 and string
   * member-values to empty strings.
   */
  FileToRecall();

  uint64_t fileTransactionId;

  std::string nsHost;

  uint64_t fileId;

  int32_t fseq;

  int32_t positionMethod;

  std::string path;

  unsigned char blockId0;

  unsigned char blockId1;

  unsigned char blockId2;

  unsigned char blockId3;

  int32_t umask;

}; // struct FileToRecall

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_FILETORECALL_HPP
