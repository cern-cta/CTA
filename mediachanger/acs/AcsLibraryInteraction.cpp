
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

#include "AcsLibraryInteraction.hpp"
#include "common/log/log.hpp"

#include <stdlib.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsLibraryInteraction::AcsLibraryInteraction(
  Acs &acs, cta::log::Logger& log): m_acs(acs), m_log(log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsLibraryInteraction::~AcsLibraryInteraction() {
}


//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsLibraryInteraction::requestResponsesUntilFinal(
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
      cta::exception::RequestFailed ex;
      ex.getMessage() << "Timed out after " << timeout << " seconds";
      throw ex;
    }
  } while(RT_FINAL != responseType);

  m_log(LOG_DEBUG,"Received RT_FINAL");
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE cta::mediachanger::acs::AcsLibraryInteraction::requestResponse(
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
    cta::exception::RequestFailed ex;
    ex.getMessage() << "Failed to request response: " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkSeqNumber
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsLibraryInteraction::checkResponseSeqNumber(
  const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber) const
   {
  if(requestSeqNumber != responseSeqNumber) {
    cta::exception::Mismatch ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// volumeStatusAsString
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::AcsLibraryInteraction::volumeStatusAsString(
  const QU_VOL_STATUS &s) const {
  
  std::ostringstream os;
  os << "vid=" << s.vol_id.external_label << " ";
  os << "media_types.dat=" << (int)s.media_type << " ";

  switch(s.location_type) {
  case LOCATION_CELL: {
    os << "location=cell" << " ";
    const CELLID &cellId = s.location.cell_id;
    os << "acs=" << (int)cellId.panel_id.lsm_id.acs << " ";
    os << "lsm=" << (int)cellId.panel_id.lsm_id.lsm << " ";
    os << "panel=" << (int)cellId.panel_id.panel << " ";
    os << "raw=" << (int)cellId.row << " ";
    os << "column=" << (int)cellId.col << " ";
    break;
  }
  case LOCATION_DRIVE: {
    os << "location=drive" << " ";
    const DRIVEID &driveId = s.location.drive_id;
    os << "acs=" << (int)driveId.panel_id.lsm_id.acs << " ";
    os << "lsm=" << (int)driveId.panel_id.lsm_id.lsm << " ";
    os << "panel=" << (int)driveId.panel_id.panel << " ";
    os << "column=" << (int)driveId.drive << " ";
    break;
  }
  default:
    os << "location=UNKNOWN" << " ";
    break;
  }

  os << "status=" << acs_status(s.status);
  return os.str();
}
