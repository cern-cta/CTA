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
#include "AcsdConfiguration.hpp"
#include "mediachanger/acs/AcsLibraryInteraction.hpp"

namespace cta        {
namespace mediachanger        {
namespace acs     {
namespace daemon        {

/**
 * Class responsible for mounting tapes for read/write access through ACS API.
 */
class AcsMountTapeReadWrite: public cta::mediachanger::acs::AcsLibraryInteraction {

public:

  /**
   * Constructor.
   */
  AcsMountTapeReadWrite(
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
   * Throws cta::exception::MountFailed if the mount was not
   * successful.
   *
   * @param buf The mount-response message.
   */
  void processMountTapeReadWriteResponse(
    ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const;
  
  /**
   * Queries ACS for information about the volume identifier.
   *
   * This method does not return until the information has been successfully
   * retrieved, an error has occurred or the specified timeout has been
   * reached.
   *
   * @return The string presentation of the query volume response.
   */
  std::string syncQueryVolume() const;
  
  /**
   * Sends the query volume  request to ACSLS.
   *
   * @param seqNumber The sequence number to be used in the request.
   */
  void sendQueryVolumeRequest(const SEQ_NO seqNumber) const;

  /**
   * Extracts the volume status from the specified query-response message and
   * returns it in human-readable form.
   *
   * @param buf The query-response message.
   * @return    The string presentation of the query volume response.
   */
  std::string processQueryResponse(
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
  
  log::Logger& m_log;

  /**
   * The configuration parameters for the CTA ACS daemon.
   */
  const AcsdConfiguration m_ctaConf;
  
}; // class AcsMountTapeReadWrite

} // namespace daemon
} // namepsace acs 
} // namespace mediachanger
} // namespace cta
