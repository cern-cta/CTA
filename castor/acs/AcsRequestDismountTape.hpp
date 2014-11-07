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

#include "castor/acs/AcsDaemonConfig.hpp"
#include "castor/acs/AcsDismountTape.hpp"
#include "castor/acs/AcsImpl.hpp"
#include "castor/acs/AcsRequest.hpp"
#include "castor/log/Logger.hpp"

namespace castor     {
namespace acs        {
  
  
/**
 * Concrete class providing a dismount tape implementation of an AcsRequest 
 * abstract class.
 */
class AcsRequestDismountTape: public AcsRequest {
public:

  /**
   * Constructor.
   * 
   * @param log     The object representing the API of the CASTOR logging system
   * @param vid     The vid of the ACS request.
   * @param acs     The acs value of the ACS request.
   * @param lsm     The lsm value of the ACS request.
   * @param panel   The panel value of the ACS request.
   * @param drive   The drive value of the ACS request.
   * @param castorConf The configuration for the CASTOR ACS daemon.
   * @param socket  ZMQ socket to use.
   * @param address ZMQ message with client address.
   * @param empty   ZMQ empty message.
   * @param seqNo   Sequence number for the ACS request.
   */
  AcsRequestDismountTape(log::Logger &log, const std::string &vid, 
    const uint32_t acs, const uint32_t lsm, const uint32_t panel, 
    const uint32_t drive, const AcsDaemonConfig &castorConf,
    messages::ZmqSocketST &socket,  messages::ZmqMsg &address,
     messages::ZmqMsg &empty, const SEQ_NO seqNo);
  
  /**
   * Perform any time related actions with the request to CASTOR ACS daemon.
   *
   * This method does not have to be called at any time precise interval.
   */
  void tick();
 
  /**
   * Checks the status of the response from the dismount response message buffer
   * and the type of the response. Throws exception if the type of the response 
   * is RT_FINAL but the status is not success.
   * 
   * @return true if the type of response RT_FINAL and the response status 
   *         is STATUS_SUCCESS.
   */
  bool isResponseFinalAndSuccess() const;
 
private:
  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  /**
   * The CASTOR configuration parameters for the CASTOR ACS daemon.
   */
  const AcsDaemonConfig m_castorConf;
  
  /**
   * The object representing the class for tape dismount through ACS API.
   */
  AcsDismountTape m_acsDismountTape;
  
  /**
   * The ACLS C-API wrapper.
   */  
  AcsImpl m_acs;
  
  /**
   * The time when the ACS library queried last time.
   */  
  time_t m_lastTimeLibraryQueried;
  
  /**
   * The time at which ACS command was started.
   */
  time_t m_timeAcsCommandStarted;
  
}; // class AcsRequestDismountTape

} // namespace acs
} // namespace castor
