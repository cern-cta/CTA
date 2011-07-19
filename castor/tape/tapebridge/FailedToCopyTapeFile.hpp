/******************************************************************************
 *                 castor/tape/tapebridge/FailedToCopyTapeFile.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_FAILEDTOCOPYTAPEFILE_HPP 
#define CASTOR_TAPE_TAPEBRIDGE_FAILEDTOCOPYTAPEFILE_HPP 1

#include "castor/exception/Exception.hpp"

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Failed to copy a tape file either from tape or to tape.
 */
class FailedToCopyTapeFile : public castor::exception::Exception {

public:

  /**
   * Structure used to describe the failed file.
   */
  struct FailedFile {
    uint64_t      fileTransactionId;
    std::string   nsHost;
    uint64_t      fileId;
    int32_t       fSeq;
    unsigned char blockId0;
    unsigned char blockId1;
    unsigned char blockId2;
    unsigned char blockId3;
    std::string   path;
    int32_t       cprc;
  };
      
  /**
   * Constructor
   *
   * @param se         The serrno code of the corresponding C error.
   * @param failedFile Information about the failed file.
   */
  FailedToCopyTapeFile(const int se, const FailedFile &failedFile) throw();

  /**
   * Returns a constant reference to the information about the failed file.
   *
   * @return A constant reference to the information about the failed file.
   */
  const FailedFile &failedFile();

private:

  /**
   * Information about the failed file.
   */
  FailedFile m_failedFile;

}; // class FailedToCopyTapeFile
      
} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_FAILEDTOCOPYTAPEFILE_HPP
