/******************************************************************************
 *                      castor/tape/tapebridge/SessionError.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_SESSIONERROR_HPP
#define CASTOR_TAPE_TAPEBRIDGE_SESSIONERROR_HPP 1

#include "castor/exception/InvalidArgument.hpp"

#include <string>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * An error that occurred during the tape session daemon that could not be
 * sent immediately to the tapegatewayd daemon, but instead had to be held back
 * until the lats message to the tapegatewayd daemon at the end of the tape
 * session.
 */
class SessionError {
public:

  /**
   * Enumeration of the possible scopes a session-error can have.
   */
  enum ErrorScope {
    FILE_SCOPE,
    SESSION_SCOPE,
    UNKNOWN_SCOPE,
    SESSIONERRORSCOPE_MAX = UNKNOWN_SCOPE};

  /**
   * Returns the string represenation of the specified error-scope.
   *
   * @param scope The error-scope.
   */
  static const char *errorScopeToString(const ErrorScope scope) throw();

private:

  /**
   * The scope of the session-error.
   */
  ErrorScope  m_errorScope;

  /**
   * The error code of the session-error.
   */
  int m_errorCode;

  /**
   * The error message of the session-error.
   */
  std::string m_errorMessage;

  /**
   * If the scope of the session-error is ErrorScope::FILE_SCOPE, then the
   * value of this member variable is the file transaction-id.
   *
   * If the scope of the session-error is not ErrorScope::FILE_SCOPE, then the
   * value of this member variable is undefined.
   */
  uint64_t m_fileTransactionId;

  /**
   * If the scope of the session-error is ErrorScope::FILE_SCOPE, then the
   * value of this member variable is the hostname of the name-server of the
   * file.
   *
   * If the scope of the session-error is not ErrorScope::FILE_SCOPE, then the
   * value of this member variable is undefined.
   */
  std::string m_nsHost;

  /**
   * If the scope of the session-error is ErrorScope::FILE_SCOPE, then the
   * value of this member variable is the name-server file-id of the file.
   *
   * If the scope of the session-error is not ErrorScope::FILE_SCOPE, then the
   * value of this member variable is undefined.
   */
  uint64_t m_nsFileId;

  /**
   * If the scope of the session-error is ErrorScope::FILE_SCOPE, then the
   * value of this member variable is the tape-file sequence-number of the file.
   *
   * If the scope of the session-error is not ErrorScope::FILE_SCOPE, then the
   * value of this member variable is undefined.
   */
  int32_t m_tapeFSeq;

public:

  /**
   * Constructor.
   */
  SessionError();

  /**
   * Sets the scope of the session-error.
   */
  void setErrorScope(const ErrorScope value)
    throw(castor::exception::InvalidArgument);

  /**
   * Gets the scope of the session-error.
   */
  ErrorScope getErrorScope() const;

  /**
   * Sets the error code of the session-error.
   */
  void setErrorCode(const int value);

  /**
   * Gets the error code of the session-error.
   */
  int getErrorCode() const;

  /**
   * Sets the error message of the session-error.
   */
  void setErrorMessage(const std::string &value);

  /**
   * Gets the error message of the session-error.
   */
  const std::string &getErrorMessage() const;

  /**
   * Sets the file transaction-id of the session-error.
   */
  void setFileTransactionId(const uint64_t value);

  /**
   * Gets the file transaction-id of the session-error.
   */
  uint64_t getFileTransactionId() const;

  /**
   * Sets the name-server hostname of the file associated with the
   * session-error.
   */
  void setNsHost(const std::string &value);

  /**
   * Gets the name-server hostname of the file associated with the
   * session-error.
   */
  const std::string &getNsHost() const;

  /**
   * Sets the name-server file-id of the file associated with the
   * session-error.
   */
  void setNsFileId(const uint64_t value);

  /**
   * Gets the name-server file-id of the file associated with the
   * session-error.
   */
  uint64_t getNsFileId() const;

  /**
   * Sets the tape-file sequence-number of the file associated with the
   * session-error.
   */
  void setTapeFSeq(const int32_t value);

  /**
   * Gets the tape-file sequence-number of the file associated with the
   * session-error.
   */
  int32_t getTapeFSeq() const;

}; // class SessionError

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_SESSIONERROR_HPP
