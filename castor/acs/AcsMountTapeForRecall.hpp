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

#include "castor/tape/rmc/Acs.hpp"
#include "castor/acs/AcsLibraryInteraction.hpp"
#include "castor/acs/AcsDaemon.hpp"
#include "castor/log/Logger.hpp"

#include <stdint.h>
#include <string>

namespace castor     {
namespace acs        {

/**
 * Class responsible for mounting tapes for recall through ACS API .
 */
class AcsMountTapeForRecall: public AcsLibraryInteraction {

public:

  /**
   * Constructor.
   */
  AcsMountTapeForRecall(
    const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive,
    castor::tape::rmc::Acs &acsWrapper,log::Logger &log,
    const AcsDaemon::CastorConf &castorConf);

  /**
   * Destructor.
   */
  ~AcsMountTapeForRecall() throw();

  /**
   * Execute mount request through ACS API.
   */
  void execute() const ;
  
protected:

  /**
   * mounts the tape with the specified VID into the drive with the specified
   * drive ID.
   *
   * This method does not return until the mount has either succeeded, failed or
   * the specified timeout has been reached.
   */
  void syncMountTapeForRecall() const;
  
  /**
   * Sends the mount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendMountForRecallRequest(const SEQ_NO seqNumber) const;
 
  /**
   * Throws castor::exception::MountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processMountForRecallResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const;
  
  /**
   * ACS VOLID
   */  
  VOLID m_volId;
  
  /**
   * ACS DRIVEID
   */  
  DRIVEID m_driveId;  
  
  /**
   * Object providing c wrapper for ACS commands.
   */
  tape::rmc::Acs &m_acsWrapper;
  
  /**
   * Logger.
   */
  log::Logger &m_log;
  
  /**
   * The configuration parameters for the CASTOR ACS daemon.
   */
  const AcsDaemon::CastorConf & m_castorConf;

}; // class AcsMountTapeForRecall

} // namespace acs
} // namespace castor
