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

#include "castor/acs/Acs.hpp"
#include "castor/acs/AcsDaemonConfig.hpp"
#include "castor/acs/AcsLibraryInteraction.hpp"
#include "castor/log/Logger.hpp"

namespace castor     {
namespace acs        {

/**
 * Class responsible for dismounting tapes through ACS API .
 */
class AcsForceDismountTape: public AcsLibraryInteraction {

public:

  /**
   * Constructor.
   */
  AcsForceDismountTape(
    const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive,
    Acs &acsWrapper, log::Logger &log,
    const AcsDaemonConfig &castorConf);

  /**
   * Destructor.
   */
  ~AcsForceDismountTape() throw();

  /**
   * Execute force dismount request through ACS API.
   */
  void execute() const;
  
  /**
   * Execute asynchronous force dismount request through ACS API.
   * 
   * @param The value of sequence number for ACS API.
   */
  void asyncExecute(const SEQ_NO seqNo) const;
  
protected:

  /**
   * Force dismounts the tape with the specified m_volId from the drive with the
   * specified m_driveId.
   *
   * This method does not return until the dismount has either succeeded, failed
   * or the specified timeout has been reached.
   */
  void syncForceDismount() const;
  
  
  /**
   * Force dismounts the tape with the specified m_volId from the drive with the
   * specified m_driveId.
   * This method sends a dismount request to ACSLS and returns.
   * 
   * @param The value of sequence number for ACS API.
   */
  void asyncForceDismount(const SEQ_NO seqNo) const;
  
  /**
   * Sends the force dismount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendForceDismountRequest(const SEQ_NO seqNumber) const;
 
  /**
   * Throws castor::exception::DismountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processForceDismountResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const;
  
  /**
   * VOLID
   */  
  VOLID m_volId;
  
  /**
   * DRIVEID
   */  
  DRIVEID m_driveId;  
  
  /**
   * Object providing c wrapper for ACS commands.
   */
  Acs &m_acsWrapper;
  
  /**
   * Logger.
   */
  log::Logger &m_log;
  
  /**
   * The configuration parameters for the CASTOR ACS daemon.
   */
  const AcsDaemonConfig m_castorConf;

}; // class AcsForceDismountTape

} // namespace acs
} // namespace castor
