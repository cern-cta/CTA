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

#include "castor/acs/AcsRequestDismountTape.hpp"
#include "castor/exception/DismountFailed.hpp"
#include "h/serrno.h"

//-----------------------------------------------------------------------------
// constructor 
//-----------------------------------------------------------------------------
castor::acs::AcsRequestDismountTape::AcsRequestDismountTape(log::Logger &log,
  const std::string &vid, const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive, 
  const AcsDaemonConfig &castorConf,
  messages::ZmqSocketST &socket,
  messages::ZmqMsg &address,
  messages::ZmqMsg &empty,
  const SEQ_NO seqNo): 
  AcsRequest(socket, address, empty, seqNo, vid, acs, lsm, panel, drive),
  m_log(log),
  m_castorConf(castorConf),
  m_acsDismountTape(vid, acs, lsm, panel, drive, m_acs, m_log, castorConf),
  m_lastTimeLibraryQueried(0),
  m_timeAcsCommandStarted(0){  
}

//-----------------------------------------------------------------------------
// tick 
//-----------------------------------------------------------------------------
void castor::acs::AcsRequestDismountTape::tick() {
  try {
    if (isToExecute()) {
      m_log(LOG_DEBUG,"AcsRequestDismountTape::tick isToExecute");
      m_acsDismountTape.asyncExecute(getSeqNo());
      setStateIsRunning();  
      m_timeAcsCommandStarted = time(0);
    }  

    const time_t now = time(0);
    
    const time_t secsSinceLastQuery = now -  m_lastTimeLibraryQueried;
    const bool firstQueryOrTimeExceeded = secsSinceLastQuery > 
      m_castorConf.queryInterval;
    
    if(isRunning() && firstQueryOrTimeExceeded) {      
      if(isResponseFinalAndSuccess()) {
        setStateCompleted();
        m_log(LOG_DEBUG,"AcsRequestDismountTape::tick ACS_REQUEST_COMPLETED");
      } else {
        m_log(LOG_DEBUG,"AcsRequestDismountTape::tick "
          "firstQueryOrTimeExceeded()");
        m_lastTimeLibraryQueried = time(0);
      }        
    }
    
    const time_t secsSinceCommandStarted = now -  m_timeAcsCommandStarted;
    const bool acsCommandTimeExceeded = secsSinceCommandStarted >
      m_castorConf.cmdTimeout;
    
    if(isRunning() && acsCommandTimeExceeded) {
      castor::exception::RequestFailed ex;
      ex.getMessage() << "ACS command timed out after " << 
        secsSinceCommandStarted << " seconds";
      throw ex;
    }
  } catch (castor::exception::Exception &ex) {
    setStateFailed(ex.code(), ex.getMessage().str());
    m_log(LOG_ERR,"Failed to handle the ACS dismount tape request: "
      + ex.getMessage().str());
  } catch(std::exception &se) {
    setStateFailed(SEINTERNAL, se.what());
    m_log(LOG_ERR, se.what());
  } catch(...) {
    setStateFailed(SEINTERNAL, "Caught an unknown exception");
    m_log(LOG_ERR, "Caught an unknown exception");
  }    
}

//------------------------------------------------------------------------------
// isResponseFinalAndSuccess
//------------------------------------------------------------------------------
bool castor::acs::AcsRequestDismountTape::isResponseFinalAndSuccess() const  {
  if (RT_FINAL == m_responseType ) {
    const ACS_DISMOUNT_RESPONSE *const msg = 
      (ACS_DISMOUNT_RESPONSE *)m_responseMsg;
    if(STATUS_SUCCESS != msg->dismount_status) {
      castor::exception::DismountFailed ex;
      ex.getMessage() << "Status of dismount response is not success: " <<
        acs_status(msg->dismount_status);
      throw ex;
    }    
    return true;
  }
  return false;  
}
