/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsCmd.hpp"
#include "common/exception/Exception.hpp"
#include <stdlib.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::acs::AcsCmd::AcsCmd(std::istream &inStream,
  std::ostream &outStream, std::ostream &errStream, Acs &acs) throw():
  CmdLineTool(inStream, outStream, errStream), m_acs(acs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::acs::AcsCmd::~AcsCmd() throw() {
}

//------------------------------------------------------------------------------
// bool2Str
//------------------------------------------------------------------------------
std::string cta::acs::AcsCmd::bool2Str(const BOOLEAN value) const
  throw() {
  return value ? "TRUE" : "FALSE";
}

//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void cta::acs::AcsCmd::requestResponsesUntilFinal(
  const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)],
  const int queryInterval, const int timeout) {
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
      cta::exception::RequestFailed ex;
      ex.getMessage() << "Timed out after " << timeout << " seconds";
      throw ex;
    }
  } while(RT_FINAL != responseType);

  m_dbg << "Received RT_FINAL" << std::endl;
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE cta::acs::AcsCmd::requestResponse(
  const int timeout, const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) {
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
    cta::exception::RequestFailed ex;
    ex.getMessage() << "Failed to request response: " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkSeqNumber
//------------------------------------------------------------------------------
void cta::acs::AcsCmd::checkResponseSeqNumber( const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber) {
  if(requestSeqNumber != responseSeqNumber) {
    cta::exception::Mismatch ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw ex;
  }
}
