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
#include "castor/acs/AcsLibraryInteraction.hpp"
#include "castor/log/Logger.hpp"
#include "castor/acs/CastorConf.hpp"

namespace castor     {
namespace acs        {

/**
 * Class responsible for mounting tapes for read/write access through ACS API .
 */
class AcsMountTapeReadWrite: public AcsLibraryInteraction {

public:

  /**
   * Constructor.
   */
  AcsMountTapeReadWrite(
    const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive,
    Acs &acsWrapper,log::Logger &log,
    const CastorConf &castorConf);

  /**
   * Destructor.
   */
  ~AcsMountTapeReadWrite() throw();

  /**
   * Execute mount request through ACS API.
   */
  void execute() const;
  
protected:

  /**
   * mounts the tape with the specified VID into the drive with the specified
   * drive ID.
   *
   * This method does not return until the mount has either succeeded, failed or
   * the specified timeout has been reached.
   */
  void syncMountTapeReadWrite() const;
  
  /**
   * Sends the mount request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendMountTapeReadWriteRequest(const SEQ_NO seqNumber) const;
 
  /**
   * Throws castor::exception::MountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processMountTapeReadWriteResponse(
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
   * Object providing C wrapper for ACS commands.
   */
  Acs &m_acsWrapper;
  
  /**
   * Logger.
   */
  log::Logger &m_log;
  
  /**
   * The configuration parameters for the CASTOR ACS daemon.
   */
  const CastorConf & m_castorConf;
  
}; // class AcsMountTapeReadWrite

} // namespace acs
} // namespace castor
