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
   * Inner class used to describe the failed file.
   */
  class FailedFile {
  private:
    uint64_t    m_fileTransactionId;
    std::string m_nsHost;
    uint64_t    m_nsFileId;
    int32_t     m_tapeFSeq;

  public:
    FailedFile():
      m_fileTransactionId(0),
      m_nsHost(""),
      m_nsFileId(0),
      m_tapeFSeq(0) {
      // Do nothing
    }

    void setFileTransactionId(const uint64_t value) {
      m_fileTransactionId = value;
    }

    uint64_t getFileTransactionId() const {
      return m_fileTransactionId;
    }

    void setNsHost(const std::string &value) {
      m_nsHost = value;
    }

    const std::string &getNsHost() const {
      return m_nsHost;
    }

    void setNsFileId(const uint64_t value) {
      m_nsFileId = value;
    }

    uint64_t getNsFileId() const {
      return m_nsFileId;
    }

    void setTapeFSeq(const int32_t value) {
      m_tapeFSeq = value;
    }

    int32_t getTapeFSeq() const {
      return m_tapeFSeq;
    }
  }; // class FailedFile
      
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
  const FailedFile &getFailedFile() const;

  /**
   * Pure virtual destructor to make this class abstract.
   */
  virtual ~FailedToCopyTapeFile() = 0;

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
