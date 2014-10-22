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

#include "castor/acs/AcsPendingRequests.hpp"
#include "castor/acs/AcsRequestDismountTape.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/messages/AcsDismountTape.pb.h"
#include "castor/acs/Constants.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::acs::AcsPendingRequests::AcsPendingRequests(
  log::Logger &log, const CastorConf &castorConf):
  m_log(log),
  m_castorConf(castorConf),
  m_lastTimeResponseUsed(0) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::acs::AcsPendingRequests::~AcsPendingRequests() throw() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();  itor++) {
    AcsRequest *const acsRequest = *itor;
    delete acsRequest;
  }
}

//-----------------------------------------------------------------------------
// tick
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::tick() {
  bool haveRunningRequests = false;
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    acsRequest->tick();
    if(acsRequest->isRunning()) {
      haveRunningRequests = true;
    }
  }
  
  if (haveRunningRequests) {
    const time_t now = time(0);
    
    const time_t secsSinceLastResponse = now -  m_lastTimeResponseUsed;
    const bool responseTimeExceeded = secsSinceLastResponse > 
      m_castorConf.acsResponseTimeout;
    
    if (responseTimeExceeded) {
      const int responseTimeout = 0 ; // 0 - means pool for 
                                      // the existence of a response.

      SEQ_NO responseSeqNumber = 0;
      REQ_ID reqId = (REQ_ID)0;
      ACS_RESPONSE_TYPE responseType = RT_NONE;
      ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

      m_log(LOG_DEBUG,
        "AcsPendingRequests::tick() Calling Acs::response()");

      const STATUS responseStatus = m_acs.response(responseTimeout,
        responseSeqNumber, reqId, responseType, buf);

      if (STATUS_SUCCESS == responseStatus) {
        setRequestResponse(responseSeqNumber,responseType, buf);
      }
      m_lastTimeResponseUsed = time(0);
    }
  }    
}

//-----------------------------------------------------------------------------
// setRequestResponse
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::setRequestResponse(
  const SEQ_NO responseSeqNumber, const ACS_RESPONSE_TYPE responseType,
  const ALIGNED_BYTES *const responseMsg) {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if ( responseSeqNumber == acsRequest->getSeqNo()) {
      std::stringstream dbgMsg;
      dbgMsg << "AcsPendingRequests::setRequestResponse responseType=" <<
        responseType << " " << acsRequest->str();
      m_log(LOG_DEBUG, dbgMsg.str());      
      acsRequest->setResponse(responseType, responseMsg);      
    }
  }
}

//-----------------------------------------------------------------------------
// handleCompletedRequests
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::handleCompletedRequests() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if (acsRequest->isCompleted()) {
      log::Param params[] = {log::Param("vid", acsRequest->getVid()),
        log::Param("acs", acsRequest->getAcs()),
        log::Param("lsm", acsRequest->getLsm()),
        log::Param("panel", acsRequest->getPanel()),
        log::Param("drive", acsRequest->getDrive()),
        log::Param("sender identity", acsRequest->getIdentity())
      };
      m_log(LOG_INFO,"Tape successfully dismounted",params);
      acsRequest->sendReplayToClientOnce();
      acsRequest->setStateToDelete();
    }
  }
}

//-----------------------------------------------------------------------------
// handleFailedRequests
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::handleFailedRequests() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if (acsRequest->isFailed()) {
      log::Param params[] = {log::Param("vid", acsRequest->getVid()),
        log::Param("acs", acsRequest->getAcs()),
        log::Param("lsm", acsRequest->getLsm()),
        log::Param("panel", acsRequest->getPanel()),
        log::Param("drive", acsRequest->getDrive()),
        log::Param("sender identity", acsRequest->getIdentity())
      };    
      m_log(LOG_INFO,"Dismount tape failed", params);
      acsRequest->sendReplayToClientOnce();
      acsRequest->setStateToDelete();
    }
  }
}

//-----------------------------------------------------------------------------
// handleToDeleteRequests
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::handleToDeleteRequests() {
  for(RequestList::iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if (acsRequest->isToDelete()) {
      m_log(LOG_DEBUG,"AcsPendingRequests::handleToDeleteRequests " +
        acsRequest->str());     
      delete acsRequest;
      itor=m_acsRequestList.erase(itor);      
    }    
  }
}

//-----------------------------------------------------------------------------
// checkAndAddRequest
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::checkAndAddRequest(
 messages::ZmqMsg &address, messages::ZmqMsg &empty,
 const messages::Frame &rqst, messages::ZmqSocketST &socket) {
  log::Param params[] = {
    log::Param("sender identity", 
      tape::utils::toHexString(address.getData(), address.size()))
  };
  m_log(LOG_DEBUG, "AcsPendingRequests::checkAndAddRequest", params);
  switch(rqst.header.msgtype()) {
    case messages::MSG_TYPE_ACSMOUNTTAPEREADONLY:
    case messages::MSG_TYPE_ACSMOUNTTAPEREADWRITE:
    {  
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to check request"
        ": Handling of this message type is not implemented: msgtype=" <<
        rqst.header.msgtype();
      throw ex;      
    }
    case messages::MSG_TYPE_ACSDISMOUNTTAPE:
      checkAndAddRequestDismountTape(address, empty, rqst, socket);
      break;
    default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to check request"
        ": Unknown request type: msgtype=" << rqst.header.msgtype();
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// checkAndAddRequestDismountTape
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::checkAndAddRequestDismountTape(
  messages::ZmqMsg &address,  messages::ZmqMsg &empty,
  const messages::Frame &rqst, messages::ZmqSocketST &socket) {
  m_log(LOG_DEBUG, 
    "AcsPendingRequests::checkAndAddRequestDismountTape");
    
  messages::AcsDismountTape rqstBody;
  rqst.parseBodyIntoProtocolBuffer(rqstBody);

  const std::string vid = rqstBody.vid();
  const uint32_t acs    = rqstBody.acs();
  const uint32_t lsm    = rqstBody.lsm();
  const uint32_t panel  = rqstBody.panel();
  const uint32_t drive  = rqstBody.drive();

  checkRequest(vid, acs, lsm, panel, drive);
  
  log::Param params[] = {log::Param("vid", vid),
    log::Param("acs", acs),
    log::Param("lsm", lsm),
    log::Param("panel", panel),
    log::Param("drive", drive),
    log::Param("sender identity", 
      tape::utils::toHexString(address.getData(), address.size()))
  };
  m_log(LOG_INFO, "Dismount tape", params);
   
  const SEQ_NO seqNo = getSequenceNumber();
  log::Param seqParam[] = {log::Param("seqNumber", seqNo)};
  m_log(LOG_DEBUG, "ACS sequence number", seqParam);  

  try {
    AcsRequest * acsRequestDismountTape = 
      new AcsRequestDismountTape(m_log, vid, acs, lsm, panel, drive, 
      m_castorConf, socket, address, empty, seqNo);
    
    acsRequestDismountTape->setStateToExecute(); 
    m_acsRequestList.push_back(acsRequestDismountTape); 
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to add dismount request: "
      << ne.getMessage().str();
    m_log(LOG_ERR, ex.getMessage().str());  
    throw ex;  
  }
}

//-----------------------------------------------------------------------------
// checkRequest
//-----------------------------------------------------------------------------
void castor::acs::AcsPendingRequests::checkRequest(const std::string &vid, 
  const uint32_t acs, const uint32_t lsm, const uint32_t panel,
  const uint32_t drive) const {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if (acs == acsRequest->getAcs() && lsm == acsRequest->getLsm() &&
      panel == acsRequest->getPanel() && drive == acsRequest->getDrive()) {   
      castor::exception::Exception ex;
      ex.getMessage() << "Check request failed: "
        "acs, lsm, panel, drive already are used by another request: "<<
         acsRequest->str();
      throw ex; 
    }
    if (std::string::npos !=  vid.find(acsRequest->getVid())) {
      castor::exception::Exception ex;
      ex.getMessage() << "Check request failed: "
        "vid already is used by another request: "<<
         acsRequest->str();
      throw ex; 
    }
  }
}

//-----------------------------------------------------------------------------
// getSequenceNumber
//-----------------------------------------------------------------------------
SEQ_NO castor::acs::AcsPendingRequests::getSequenceNumber() const {
  unsigned short maxSeqNo = 0;
  unsigned short minSeqNo = ACS_MAXIMUM_SEQUENCE_NUMBER;
  
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    AcsRequest *const acsRequest = *itor;
    if (maxSeqNo < acsRequest->getSeqNo()) {
      maxSeqNo = acsRequest->getSeqNo();
    }
    if (minSeqNo > acsRequest->getSeqNo()) {
      minSeqNo = acsRequest->getSeqNo();
    }
  }
  
  // first request
  if(ACS_MAXIMUM_SEQUENCE_NUMBER == minSeqNo && 0 == maxSeqNo) {
    return 1;
  }
    
  // try to get number from 1 to minSeqNo
  if(1 != minSeqNo ) {
      return minSeqNo-1;
    }
  
  // try to get number from maxSeqNo to maximum allowed
  if (ACS_MAXIMUM_SEQUENCE_NUMBER != maxSeqNo) {
    return maxSeqNo+1;
  }
  
  castor::exception::Exception ex;
  ex.getMessage() << "Failed to get sequence number for ACS"
    ": allocated minimum seqNo=\""<<minSeqNo<<"\""<<
    " allocated maximum seqNo=\""<<maxSeqNo<<"\"";
  m_log(LOG_ERR, ex.getMessage().str());  
  throw ex;   
}
