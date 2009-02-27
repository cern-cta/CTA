/******************************************************************************
 *                      DriveAllocationProtocolEngine.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/aggregator/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/fsm/Callback.hpp"

#include <iostream>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::DriveAllocationProtocolEngine::
  DriveAllocationProtocolEngine(const Cuuid_t &cuuid,
  const int rtcpdCallbackSocketFd, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) : m_cuuid(cuuid),
  m_rtcpdCallbackSocketFd(rtcpdCallbackSocketFd),
  m_rtcpdInitialSocketFd(rtcpdInitialSocketFd) {
}


//-----------------------------------------------------------------------------
// testFsm
//-----------------------------------------------------------------------------
void castor::tape::aggregator::DriveAllocationProtocolEngine::testFsm() {
  std::cout << std::endl;                         
  std::cout << "=========================================" << std::endl;
  std::cout << "castor::tape::aggregator::FsmTest::doit()" << std::endl;
  std::cout << "=========================================" << std::endl;
  std::cout << std::endl;                                               


  //---------------------------------------------
  // Define and set the state machine transitions
  //---------------------------------------------

  typedef fsm::Callback<DriveAllocationProtocolEngine> Callback;
  Callback getReqFromRtcpd(*this,
    &DriveAllocationProtocolEngine::getReqFromRtcpd);
  Callback getVolFromRtcpd(*this,
    &DriveAllocationProtocolEngine::getVolFromRtcpd);
  Callback error(*this, &DriveAllocationProtocolEngine::error);
  Callback closeRtcpdConAndError(*this,
    &DriveAllocationProtocolEngine::closeRtcpdConAndError);

  fsm::Transition transitions[] = {
  // from state       , to state        , event , action
    {"INIT"           , "WAIT_RTCPD_REQ", "INIT", &getReqFromRtcpd      },
    {"WAIT_RTCPD_REQ" , "WAIT_TGATE_VOL", "REQ" , &getVolFromRtcpd      },
    {"WAIT_TGATE_VOL" , "END"           , "VOL" , NULL                  },
    {"RTCPD_CONNECTED", "FAILED"        , "ERR" , &closeRtcpdConAndError},
    {NULL             , NULL            , NULL  , NULL                  }};

  m_fsm.setTransitions(transitions);


  //------------------------
  // Set the state hierarchy
  //------------------------

  fsm::ChildToParentState hierarchy[] = {
  // child           , parent            
    {"WAIT_RTCPD_REQ", "RTCPD_CONNECTED"},
    {"WAIT_TGATE_VOL", "RTCPD_CONNECTED"},
    {NULL            , NULL             }
  };

  m_fsm.setStateHierarchy(hierarchy);


  //-------------------------------------------
  // Set the initial state of the state machine
  //-------------------------------------------

  m_fsm.setState("INIT");


  //----------------------------------------------------------
  // Execute the state machine with debugging print statements
  //----------------------------------------------------------

  const char *event = "INIT";

  // While no more automatic state transitions
  while(event != NULL) {

    std::cout << "From state        = " << m_fsm.getState() << std::endl;
    std::cout << "Dispatching event = " << event            << std::endl;

    event = m_fsm.dispatch(event);

    if(event != NULL) {
      std::cout << "Internally generated event for an automatic state "
        "transition" << std::endl;
    }
  }

  std::cout << std::endl;
}


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  getVolFromRtcpd() {
  std::cout << "getVolFromRtcpd()" << std::endl;
  sleep(1);                                     
  return "VOL";                                 
}                                               


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  getReqFromRtcpd() {
  std::cout << "getReqFromRtcpd()" << std::endl;
  sleep(1);                                     
  return "REQ";                                 

}                                               


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  error() {
  std::cout << "error()" << std::endl;
  sleep(1);                           
  return NULL;                        

}                                     


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  closeRtcpdConAndError() {
  std::cout << "closeRtcpConAndError()" << std::endl;
  sleep(1);                                          
  return NULL;                                       
}                                                    
