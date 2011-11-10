/******************************************************************************
 *                      castor/tape/tapebridge/RtcpdError.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_RTCPDERROR
#define CASTOR_TAPE_TAPEBRIDGE_RTCPDERROR

#include "castor/exception/InvalidArgument.hpp"

#include <string>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * An error that has been received from the rtcpd daemon.
 */
class RtcpdError {
public:
  enum ErrorScope {
    FILE_SCOPE,
    SESSION_SCOPE,
    UNKNOWN_SCOPE,
    RTCPDERRORSCOPE_MAX = UNKNOWN_SCOPE};

private:
  ErrorScope  m_errorScope;
  int         m_errorCode;
  std::string m_errorMessage;
  uint64_t    m_fileTransactionId;
  std::string m_nsHost;
  uint64_t    m_nsFileId;
  int32_t     m_tapeFSeq;

public:
  RtcpdError();

  void setErrorScope(const ErrorScope value)
    throw(castor::exception::InvalidArgument);

  ErrorScope getErrorScope() const;

  void setErrorCode(const int value);

  int getErrorCode() const;

  void setErrorMessage(const std::string &value);

  const std::string &getErrorMessage() const;

  void setFileTransactionId(const uint64_t value);

  uint64_t getFileTransactionId() const;

  void setNsHost(const std::string &value);

  const std::string &getNsHost() const;

  void setNsFileId(const uint64_t value);

  uint64_t getNsFileId() const;

  void setTapeFSeq(const int32_t value);

  int32_t getTapeFSeq() const;
}; // class RtcpdError

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_RTCPDERROR
