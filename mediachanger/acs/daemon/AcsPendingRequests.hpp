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

#pragma once

#include "AcsdConfiguration.hpp"
#include "common/log/log.hpp"
#include "common/log/SyslogLogger.hpp"
#include "mediachanger/acs/AcsRequest.hpp"
#include "mediachanger/acs/AcsImpl.hpp"
#include "mediachanger/messages.hpp"
#include "mediachanger/Frame.hpp"
#include "mediachanger/ZmqMsg.hpp"
#include "mediachanger/ZmqSocket.hpp"
#include "mediachanger/ZmqSocketST.hpp"
#include <list>
#include <time.h>

namespace cta     {
namespace mediachanger        {
namespace acs        {
namespace daemon        {
   
/**
 * Class responsible for keeping track of the Acs requests  controlled by
 * the CASTOR ACS daemon.
 */
class AcsPendingRequests {
public:

  /**
   * Constructor.
   *
   * @param castorConf The configuration for the CASTOR ACS daemon.
   */
  AcsPendingRequests(const AcsdConfiguration &ctaConf, cta::log::Logger &);
  
  /**
   * Destructor.
   */
  ~AcsPendingRequests() throw();

  /**
   * Notifies the AcsPendingRequests that it should perform any time related 
   * actions.
   *
   * This method does not have to be called at any time precise interval.
   */
  void tick();

  /**
   * Requests to the CASTOR ACS daemon might have several states.
   * 
   *  ACS_REQUEST_TO_EXECUTE - is initial state. When request arrives from a 
   *    client it is set to be asynchronous executed to ACS Library.
   *  ACS_REQUEST_IS_RUNNING - the state in which we periodically query ACS 
   *    Library to check the status of the ongoing request.
   *  ACS_REQUEST_COMPLETED  - indicates that the request is completed 
   *    successfully.
   *  ACS_REQUEST_FAILED     - indicates that the request is completed 
   *    unsuccessfully.
   *  ACS_REQUEST_TO_DELETE  - indicates that the request is handled and might 
   *    be deleted.
   * 
   *                             /- COMPLETED -\ 
   * TO_EXECUTE -> IS_RUNNING ->|               |-> TO_DELETE
   *                             \- FAILED    -/
   */
  
  /**
   * Handles successfully completed requests.
   */
  void handleCompletedRequests();
  
  /**
   * Handles failed requests.
   */
  void handleFailedRequests();
  
  /**
   * Performs cleanup for deleted requests.
   */
  void handleToDeleteRequests();
  
  /**
   * Performs general checks for the incoming requests and calls next checker 
   * for the message. Throws exceptions if checks are not passed.
   * 
   * @param address ZMQ message with client address.
   * @param empty   ZMQ empty message.
   * @param rqst    ZMQ message with CASTOR frame.
   * @param socket  ZMQ socket to use.
   */

   void checkAndAddRequest(mediachanger::ZmqMsg &address,
    mediachanger::ZmqMsg &empty,
    const mediachanger:: Frame &rqst,
    mediachanger::ZmqSocketST &socket);
  
/**
   * Performs dismount specific checks for the incoming request and add it to 
   * the list of the request to be handled.
   * 
   * @param address ZMQ message with client address.
   * @param empty   ZMQ empty message.
   * @param rqst    ZMQ message with CASTOR frame.
   * @param socket  ZMQ socket to use.
   */
  void checkAndAddRequestDismountTape(mediachanger::ZmqMsg &address,
    mediachanger::ZmqMsg &empty,
    const mediachanger::Frame &rqst,
    mediachanger::ZmqSocketST &socket);
  
  /**
   * Find and return free sequence number for the ACS request.
   * 
   * @return The value of free sequence number for the ACS request. Throws
   * exception if the is no free sequence number.
   */
  SEQ_NO getSequenceNumber() const;

  /**
   * Sets the type of the response and the response message in the ACS request
   * with the sequence number equal response sequence number.
   * 
   * @param responseSeqNumber The sequence number to find ongoing ACS request.
   * @param responseType The type of the response message.
   * @param responseMsg  The response message
   */ 
   void setRequestResponse(const SEQ_NO responseSeqNumber,
   const ACS_RESPONSE_TYPE responseType, 
   const  ALIGNED_BYTES *const responseMsg);
    
  /**
   * Performs checks for the request before adding it to the ACS requests list.
   * Throws exceptions if there are any problems.
   * 
   * @param vid     The vid of the ACS request.
   * @param acs     The acs value of the ACS request.
   * @param lsm     The lsm value of the ACS request.
   * @param panel   The panel value of the ACS request.
   * @param drive   The drive value of the ACS request.
   */ 
  void checkRequest(const std::string &vid, const uint32_t acs, 
    const uint32_t lsm, const uint32_t panel, const uint32_t drive) const ;
  
private:

  /**
   * The object representing castor configuration parameters for 
   * the CASTOR ACS daemon.
   */
  const AcsdConfiguration m_ctaConf;
  
  log::Logger &m_log;
  /**
   * Type for the list of the ACS requests.
   */
  typedef std::list<AcsRequest *> RequestList;
  
  /**
   * The list for the ACS requests.
   */
  RequestList m_acsRequestList;
  
  /**
   * The ACLS C-API wrapper.
   */  
  AcsImpl m_acs;
  
  /**
   * The time when the last ACS response command was used.
   */ 
  time_t m_lastTimeResponseUsed;
  
}; // class AcsPendingRequests

}
}
} // namespace acs
} // namespace cta
