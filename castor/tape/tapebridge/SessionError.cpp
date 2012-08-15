/******************************************************************************
 *                      castor/tape/tapebridge/SessionError.cpp
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

#include "castor/tape/tapebridge/SessionError.hpp"
#include "h/serrno.h"

//----------------------------------------------------------------------------
// errorScopeToString
//----------------------------------------------------------------------------
const char *castor::tape::tapebridge::SessionError::errorScopeToString(
  const ErrorScope scope) throw() {
  switch(scope) {
  case FILE_SCOPE   : return "file scope";
  case SESSION_SCOPE: return "session scope";
  case UNKNOWN_SCOPE: return "unknown scope";
  default           : return "INVALID SCOPE";
  }
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::SessionError::SessionError() throw() :
  m_errorScope(UNKNOWN_SCOPE),
  m_errorCode(SEINTERNAL),
  m_errorMessage("UNKNOWN"),
  m_fileTransactionId(0),
  m_nsHost("UNKNOWN"),
  m_nsFileId(0),
  m_tapeFSeq(0) {
  // Do nothing
}

//-----------------------------------------------------------------------------
// setErrorScope
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setErrorScope(
  const ErrorScope value) throw() {
  m_errorScope = value;
}

//-----------------------------------------------------------------------------
// getErrorScope
//-----------------------------------------------------------------------------
castor::tape::tapebridge::SessionError::ErrorScope
  castor::tape::tapebridge::SessionError::getErrorScope() const throw() {
  return m_errorScope;
}

//-----------------------------------------------------------------------------
// setErrorCode
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setErrorCode(const int value)
  throw() {
  m_errorCode = value;
}

//-----------------------------------------------------------------------------
// getErrorCode
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::SessionError::getErrorCode() const throw() {
  return m_errorCode;
}

//-----------------------------------------------------------------------------
// setErrorMessage
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setErrorMessage(
  const std::string &value) throw() {
  m_errorMessage = value;
}

//-----------------------------------------------------------------------------
// getErrorMessage
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapebridge::SessionError::getErrorMessage()
  const throw() {
  return m_errorMessage;
}

//-----------------------------------------------------------------------------
// setFileTransactionId
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setFileTransactionId(
  const uint64_t value) throw() {
  m_fileTransactionId = value;
}

//-----------------------------------------------------------------------------
// getFileTransactionId
//-----------------------------------------------------------------------------
uint64_t castor::tape::tapebridge::SessionError::getFileTransactionId()
  const throw() {
  return m_fileTransactionId;
}

//-----------------------------------------------------------------------------
// setNsHost
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setNsHost(
  const std::string &value) throw() {
  m_nsHost = value;
}

//-----------------------------------------------------------------------------
// getNsHost
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapebridge::SessionError::getNsHost() const
  throw() {
  return m_nsHost;
}

//-----------------------------------------------------------------------------
// setNsFileId
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setNsFileId(const uint64_t value)
  throw() {
  m_nsFileId = value;
}

//-----------------------------------------------------------------------------
// getNsFileId
//-----------------------------------------------------------------------------
uint64_t castor::tape::tapebridge::SessionError::getNsFileId() const
  throw() {
  return m_nsFileId;
}

//-----------------------------------------------------------------------------
// setTapeFSeq
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SessionError::setTapeFSeq(const int32_t value)
  throw() {
  m_tapeFSeq = value;
}

//-----------------------------------------------------------------------------
// getTapeFSeq
//-----------------------------------------------------------------------------
int32_t castor::tape::tapebridge::SessionError::getTapeFSeq() const
  throw() {
  return m_tapeFSeq;
}
