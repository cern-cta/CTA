/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsPendingRequests.hpp"
#include "mediachanger/acs/AcsImpl.hpp"
#include "mediachanger/acs/daemon/AcsRequest.hpp"
#include "mediachanger/acs/daemon/AcsDismountTape.hpp"
#include "AcsRequestDismountTape.hpp"
#include "common/Constants.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/acs/daemon/Constants.hpp"
#include "common/log/log.hpp"
#include "mediachanger/messages.hpp"
#include "mediachanger/Frame.hpp"
#include "mediachanger/AcsDismountTape.pb.h"
//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsPendingRequests::AcsPendingRequests(
  const AcsdConfiguration &ctaConf, cta::log::Logger &l):
  m_ctaConf(ctaConf),
  m_log(l),
  m_lastTimeResponseUsed(0) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsPendingRequests::~AcsPendingRequests() throw() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();  itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    delete acsRequest;
  }
}

//-----------------------------------------------------------------------------
// tick
//-----------------------------------------------------------------------------

  void cta::mediachanger::acs::daemon::AcsPendingRequests::tick() {
  bool haveRunningRequests = false;
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    acsRequest->tick();
    if(acsRequest->isRunning()) {
      haveRunningRequests = true;
    }
  }
  
  if (haveRunningRequests) {
    const time_t now = time(0);
    
    const time_t secsSinceLastResponse = now -  m_lastTimeResponseUsed;
    const bool responseTimeExceeded = secsSinceLastResponse >
      ACS_RESPONSE_TIMEOUT;
    
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
void cta::mediachanger::acs::daemon::AcsPendingRequests::setRequestResponse(
  const SEQ_NO responseSeqNumber, const ACS_RESPONSE_TYPE responseType,
  const ALIGNED_BYTES *const responseMsg) {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
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
void cta::mediachanger::acs::daemon::AcsPendingRequests::handleCompletedRequests() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    if (acsRequest->isCompleted()) {
      std::list<log::Param> params = {log::Param("TPVID", acsRequest->getVid()),
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
void cta::mediachanger::acs::daemon::AcsPendingRequests::handleFailedRequests() {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    if (acsRequest->isFailed()) {
      std::list<log::Param> params = {log::Param("TPVID", acsRequest->getVid()),
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
void cta::mediachanger::acs::daemon::AcsPendingRequests::handleToDeleteRequests() {
  for(RequestList::iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
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
void cta::mediachanger::acs::daemon::AcsPendingRequests::checkAndAddRequest(
 mediachanger::ZmqMsg &address, mediachanger::ZmqMsg &empty,
 const cta::mediachanger::Frame &rqst, cta::mediachanger::ZmqSocketST &socket) {
  std::list<log::Param> params = {
    log::Param("sender identity", 
      utils::hexDump(address.getData(), address.size()))
  };
  m_log(LOG_DEBUG, "AcsPendingRequests::checkAndAddRequest", params);

  const cta::mediachanger::acs::daemon::MsgType msgType = (cta::mediachanger::acs::daemon::MsgType)rqst.header.msgtype();
  switch(msgType) {
  case mediachanger::MSG_TYPE_ACSMOUNTTAPEREADONLY:
  case mediachanger::MSG_TYPE_ACSMOUNTTAPEREADWRITE:
    {  
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to check request"
        ": Handling of this message type is not implemented: msgtype=" <<
        rqst.header.msgtype();
      throw ex;      
    }
  case mediachanger::MSG_TYPE_ACSDISMOUNTTAPE:
    checkAndAddRequestDismountTape(address, empty, rqst, socket);
    break;
  default:
    {
      const std::string msgTypeStr = cta::mediachanger::acs::daemon::msgTypeToString(msgType);
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to check request"
        ": Unexpected request type: msgType=" << msgType << " msgTypeStr=" <<
        msgTypeStr;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// checkAndAddRequestDismountTape
//-----------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsPendingRequests::checkAndAddRequestDismountTape(
  mediachanger::ZmqMsg &address,  mediachanger::ZmqMsg &empty,
  const mediachanger::Frame &rqst, mediachanger::ZmqSocketST &socket) {
  m_log(LOG_DEBUG, 
    "AcsPendingRequests::checkAndAddRequestDismountTape");
    
  mediachanger::AcsDismountTape rqstBody;
  rqst.parseBodyIntoProtocolBuffer(rqstBody);

  const std::string vid = rqstBody.vid();
  const uint32_t acs    = rqstBody.acs();
  const uint32_t lsm    = rqstBody.lsm();
  const uint32_t panel  = rqstBody.panel();
  const uint32_t drive  = rqstBody.drive();

  checkRequest(vid, acs, lsm, panel, drive);
  
  std::list<log::Param> params = {log::Param("TPVID", vid),
    log::Param("acs", acs),
    log::Param("lsm", lsm),
    log::Param("panel", panel),
    log::Param("drive", drive),
    log::Param("sender identity", 
      utils::hexDump(address.getData(), address.size()))
  };
  m_log(LOG_INFO, "Dismount tape", params);
   
  const SEQ_NO seqNo = getSequenceNumber();
  std::list<log::Param> seqParam = {log::Param("seqNumber", seqNo)};
  m_log(LOG_DEBUG, "ACS sequence number", seqParam);  

  try {
    cta::mediachanger::acs::daemon::AcsRequest * acsRequestDismountTape = 
      new AcsRequestDismountTape(vid, acs, lsm, panel, drive, 
      //new AcsRequestDismountTape AcsRequestDismountTape(vid, acs, lsm, panel, drive, 
        m_ctaConf, socket, address, empty, m_log, seqNo);
    
    acsRequestDismountTape->setStateToExecute(); 
    m_acsRequestList.push_back(acsRequestDismountTape); 
  } catch (cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to add dismount request: "
      << ne.getMessage().str();
    m_log(LOG_ERR, ex.getMessage().str());  
    throw ex;  
  }
}

//-----------------------------------------------------------------------------
// checkRequest
//-----------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsPendingRequests::checkRequest(const std::string &vid, 
  const uint32_t acs, const uint32_t lsm, const uint32_t panel,
  const uint32_t drive) const {
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    if (acs == acsRequest->getAcs() && lsm == acsRequest->getLsm() &&
      panel == acsRequest->getPanel() && drive == acsRequest->getDrive()) {   
      cta::exception::Exception ex;
      ex.getMessage() << "Check request failed: "
        "acs, lsm, panel, drive already are used by another request: "<<
         acsRequest->str();
      throw ex; 
    }
    if (std::string::npos !=  vid.find(acsRequest->getVid())) {
      cta::exception::Exception ex;
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
SEQ_NO cta::mediachanger::acs::daemon::AcsPendingRequests::getSequenceNumber() const {
  unsigned short maxSeqNo = 0;
  unsigned short minSeqNo = ACS_MAX_SEQ;
  
  for(RequestList::const_iterator itor = m_acsRequestList.begin(); 
    itor != m_acsRequestList.end();itor++) {
    cta::mediachanger::acs::daemon::AcsRequest *const acsRequest = *itor;
    if (maxSeqNo < acsRequest->getSeqNo()) {
      maxSeqNo = acsRequest->getSeqNo();
    }
    if (minSeqNo > acsRequest->getSeqNo()) {
      minSeqNo = acsRequest->getSeqNo();
    }
  }
  
  // first request
  if(ACS_MAX_SEQ == minSeqNo && 0 == maxSeqNo) {
    return 1;
  }
    
  // try to get number from 1 to minSeqNo
  if(1 != minSeqNo ) {
      return minSeqNo-1;
    }
  
  // try to get number from maxSeqNo to maximum allowed
  if (ACS_MAX_SEQ != maxSeqNo) {
    return maxSeqNo+1;
  }
  
  cta::exception::Exception ex;
  ex.getMessage() << "Failed to get sequence number for ACS"
    ": allocated minimum seqNo=\""<<minSeqNo<<"\""<<
    " allocated maximum seqNo=\""<<maxSeqNo<<"\"";
  m_log(LOG_ERR, ex.getMessage().str());  
  throw ex;   
}
