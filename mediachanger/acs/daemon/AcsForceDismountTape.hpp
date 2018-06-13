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

#pragma once

#include "mediachanger/acs/Acs.hpp"
#include "mediachanger/acs/daemon/AcsdConfiguration.hpp"
#include "mediachanger/acs/AcsLibraryInteraction.hpp"
#include "common/log/log.hpp"
#include "common/log/SyslogLogger.hpp"

namespace cta        {
namespace mediachanger     {
namespace acs        {
namespace daemon       {

/**
 * Class responsible for dismounting tapes through ACS API.
 */
class AcsForceDismountTape: public cta::mediachanger::acs::AcsLibraryInteraction {

public:

  /**
   * Constructor.
   */
  AcsForceDismountTape(
    const std::string &vid,
    const uint32_t acs,
    const uint32_t lsm,
    const uint32_t panel,
    const uint32_t drive,
    cta::mediachanger::acs::Acs &acsWrapper,
    log::Logger& log,
    const AcsdConfiguration &ctaConf);

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
   * Throws cta::exception::DismountFailed if the mount was not
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
  
  log::Logger &m_log;

  /**
   * The configuration parameters for the CTA ACS daemon.
   */
  const AcsdConfiguration m_ctaConf;

}; // class AcsForceDismountTape

} // namespace daemon
} // namespace acs
} // namespace mediachanger
} // namespace cta
