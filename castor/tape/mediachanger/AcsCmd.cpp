/******************************************************************************
 *                 castor/tape/mediachanger/AcsCmd.cpp
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

#include "castor/tape/mediachanger/AcsCmd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <stdlib.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::AcsCmd::AcsCmd(std::istream &inStream,
  std::ostream &outStream, std::ostream &errStream, Acs &acs) throw():
  m_in(inStream), m_out(outStream), m_err(errStream), m_acs(acs),
  m_debugBuf(outStream), m_dbg(&m_debugBuf) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::AcsCmd::~AcsCmd() throw() {
}

//------------------------------------------------------------------------------
// bool2Str
//------------------------------------------------------------------------------
std::string castor::tape::mediachanger::AcsCmd::bool2Str(bool &value) const
  throw() {
  if(value) {
    return "TRUE";
  } else {
    return "FALSE";
  }
}

//------------------------------------------------------------------------------
// bool2Str
//------------------------------------------------------------------------------
std::string castor::tape::mediachanger::AcsCmd::bool2Str(BOOLEAN &value) const
  throw() {
  if(value) {
    return "TRUE";
  } else {
    return "FALSE";
  }
}

//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void castor::tape::mediachanger::AcsCmd::requestResponsesUntilFinal(
  const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)],
  const int queryInterval, const int timeout)
  throw (castor::exception::RequestFailed) {
  ACS_RESPONSE_TYPE responseType = RT_NONE;
  int elapsedTime = 0;
  do {
    const int remainingTime = timeout - elapsedTime;
    const int responseTimeout = remainingTime > queryInterval ?
      queryInterval : remainingTime;

    const time_t startTime = time(NULL);
    responseType = requestResponse(responseTimeout, requestSeqNumber, buf);
    elapsedTime += time(NULL) - startTime;

    if(RT_ACKNOWLEDGE == responseType) {
      m_dbg << "Received RT_ACKNOWLEDGE" << std::endl;
    }

    if(elapsedTime >= timeout) {
      castor::exception::RequestFailed ex;
      ex.getMessage() << "Timed out after " << timeout << " seconds";
      throw(ex);
    }
  } while(RT_FINAL != responseType);

  m_dbg << "Received RT_FINAL" << std::endl;
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE castor::tape::mediachanger::AcsCmd::requestResponse(
  const int timeout, const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::RequestFailed) {
  SEQ_NO responseSeqNumber = 0;
  REQ_ID reqId = (REQ_ID)0;
  ACS_RESPONSE_TYPE responseType = RT_NONE;

  m_dbg << "Calling Acs::response()" << std::endl;
  const STATUS s = m_acs.response(timeout, responseSeqNumber, reqId,
    responseType, buf);
  m_dbg << "Acs::response() returned " << acs_status(s) << std::endl;

  switch(s) {
  case STATUS_SUCCESS:
    checkResponseSeqNumber(requestSeqNumber, responseSeqNumber);
    return responseType;
  case STATUS_PENDING:
    return RT_NONE;
  default:
    castor::exception::RequestFailed ex;
    ex.getMessage() << "Failed to request response: " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkSeqNumber
//------------------------------------------------------------------------------
void castor::tape::mediachanger::AcsCmd::checkResponseSeqNumber(
  const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber)
  throw(castor::exception::Mismatch) {
  if(requestSeqNumber != responseSeqNumber) {
    castor::exception::Mismatch ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw(ex);
  }
}
