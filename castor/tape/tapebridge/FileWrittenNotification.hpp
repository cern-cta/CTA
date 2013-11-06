/******************************************************************************
 *                castor/tape/tapebridge/FileWrittenNotification.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_FILEWRITTENNOTIFICATION_HPP
#define CASTOR_TAPE_TAPEBRIDGE_FILEWRITTENNOTIFICATION_HPP 1

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * A notification that a file has been written to tape without a flush.
 */
struct FileWrittenNotification {
  uint64_t    fileTransactionId;
  std::string nsHost;
  uint64_t    nsFileId;
  int32_t     positionCommandCode;
  int32_t     tapeFSeq;
  uint64_t    fileSize;
  std::string checksumName;
  uint64_t    checksum;
  uint64_t    compressedFileSize;
  uint8_t     blockId0;
  uint8_t     blockId1;
  uint8_t     blockId2;
  uint8_t     blockId3;

  /**
   * Constructor.
   */
  FileWrittenNotification():
    fileTransactionId(0),
    nsHost(""),
    nsFileId(0),
    positionCommandCode(0),
    tapeFSeq(0),
    fileSize(0),
    checksumName(""),
    checksum(0),
    compressedFileSize(0),
    blockId0(0),
    blockId1(0),
    blockId2(0),
    blockId3(0) {
    // Do nothing
  }
}; // struct FileWrittenNotification

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_FILEWRITTENNOTIFICATION_HPP
