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

#include "AcsRequestDismountTape.hpp"
#include "common/exception/DismountFailed.hpp"
#include "common/log/log.hpp"

//-----------------------------------------------------------------------------
// constructor 
//-----------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsRequestDismountTape::AcsRequestDismountTape(
  const std::string &vid, const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive, 
  const AcsdConfiguration &ctaConf,
  mediachanger::ZmqSocketST &socket,
  mediachanger::ZmqMsg &address,
  mediachanger::ZmqMsg &empty,
  log::Logger &log,
  const SEQ_NO seqNo): 
  AcsRequest(socket, address, empty, seqNo, vid, acs, lsm, panel, drive),
  m_ctaConf(ctaConf),
  m_acsDismountTape(vid, acs, lsm, panel, drive, m_acs, log, ctaConf),
  m_lastTimeLibraryQueried(0),
  m_log(log),
  m_timeAcsCommandStarted(0) {  
}

//-----------------------------------------------------------------------------
// tick 
//-----------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequestDismountTape::tick() {
  try {
    if (isToExecute()) {
      m_log(LOG_DEBUG,"AcsRequestDismountTape::tick isToExecute");
      m_acsDismountTape.asyncExecute(getSeqNo());
      setStateIsRunning();  
      m_timeAcsCommandStarted = time(0);
    }  

    const time_t now = time(0);
    
    const time_t secsSinceLastQuery = now -  m_lastTimeLibraryQueried;
    const bool firstQueryOrTimeExceeded = (unsigned int)secsSinceLastQuery > 
      m_ctaConf.QueryInterval.value();
    
    if(isRunning() && firstQueryOrTimeExceeded) {      
      if(isResponseFinalAndSuccess()) {
        setStateCompleted();
        m_log(LOG_DEBUG,
         "AcsRequestDismountTape::tick ACS_REQUEST_COMPLETED");
      } else {
        m_log(LOG_DEBUG,"AcsRequestDismountTape::tick "
          "firstQueryOrTimeExceeded()");
        m_lastTimeLibraryQueried = time(0);
      }        
    }
    
    const time_t secsSinceCommandStarted = now -  m_timeAcsCommandStarted;
    const bool acsCommandTimeExceeded = (unsigned int)secsSinceCommandStarted >
      m_ctaConf.CmdTimeout.value();
    
    if(isRunning() && acsCommandTimeExceeded) {
      cta::exception::RequestFailed ex;
      ex.getMessage() << "ACS command timed out after " << 
        secsSinceCommandStarted << " seconds";
      throw ex;
    }
  } catch (cta::exception::Exception &ex) {
   setStateFailed(ECANCELED, ex.getMessage().str());
    m_log(LOG_ERR,"Failed to handle the ACS dismount tape request: "
      + ex.getMessage().str());
  } catch(std::exception &se) {
    setStateFailed(ECANCELED, se.what());
    m_log(LOG_ERR, se.what());
  } catch(...) {
    setStateFailed(ECANCELED, "Caught an unknown exception");
    m_log(LOG_ERR, "Caught an unknown exception");
  }    
}

//------------------------------------------------------------------------------
// isResponseFinalAndSuccess
//------------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequestDismountTape::isResponseFinalAndSuccess() const  {
  if (RT_FINAL == m_responseType ) {
    const ACS_DISMOUNT_RESPONSE *const msg = 
      (ACS_DISMOUNT_RESPONSE *)m_responseMsg;
    if(STATUS_SUCCESS != msg->dismount_status) {
      cta::exception::DismountFailed ex;
      ex.getMessage() << "Status of dismount response is not success: " <<
        acs_status(msg->dismount_status);
      throw ex;
    }    
    return true;
  }
  return false;  
}
