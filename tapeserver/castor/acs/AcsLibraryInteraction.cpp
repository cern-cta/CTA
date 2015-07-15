/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/acs/AcsLibraryInteraction.hpp"

#include <stdlib.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsLibraryInteraction::AcsLibraryInteraction(
  Acs &acs, log::Logger &log) throw(): m_acs(acs), m_log(log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsLibraryInteraction::~AcsLibraryInteraction() throw() {
}


//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void castor::acs::AcsLibraryInteraction::requestResponsesUntilFinal(
  const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)],
  const int queryInterval, const int timeout) const
  {
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
      m_log(LOG_DEBUG,"Received RT_ACKNOWLEDGE");
    }

    if(elapsedTime >= timeout) {
      castor::exception::RequestFailed ex;
      ex.getMessage() << "Timed out after " << timeout << " seconds";
      throw ex;
    }
  } while(RT_FINAL != responseType);

  m_log(LOG_DEBUG,"Received RT_FINAL");
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE castor::acs::AcsLibraryInteraction::requestResponse(
  const int timeout, const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
   {
  SEQ_NO responseSeqNumber = 0;
  REQ_ID reqId = (REQ_ID)0;
  ACS_RESPONSE_TYPE responseType = RT_NONE;

  std::stringstream dbgMsg;
  dbgMsg << "Calling Acs::response() for requestSeqNumber=" << requestSeqNumber;
  m_log(LOG_DEBUG, dbgMsg.str());
  
  const STATUS s = m_acs.response(timeout, responseSeqNumber, reqId,
    responseType, buf);
  
  dbgMsg.str("");
  dbgMsg << "Acs::response() for requestSeqNumber=" << requestSeqNumber <<
    " returned responseSeqNumber=" << responseSeqNumber << " and status " << 
    acs_status(s);          
  m_log(LOG_DEBUG,dbgMsg.str());
  
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
void castor::acs::AcsLibraryInteraction::checkResponseSeqNumber(
  const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber) const
   {
  if(requestSeqNumber != responseSeqNumber) {
    castor::exception::Mismatch ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw ex;
  }
}
