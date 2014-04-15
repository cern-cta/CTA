/******************************************************************************
 *         castor/tape/tapeserver/daemon/RmcImpl.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/Rmc.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A concrete implementation of the interface to the rmc daemon.
 */
class RmcImpl: public Rmc {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param rmcHostName The name of the host on which the rmcd daemon is
   * running.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  RmcImpl(log::Logger &log, const std::string &rmcHostName, const unsigned short rmcPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~RmcImpl() throw();

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * specified drive.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The drive in one of the following three forms corresponding
   * to the three supported drive-loader types, namely acs, manual and smc:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   */
  void mountTape(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * specified drive.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The drive in one of the following three forms corresponding
   * to the three supported drive-loader types, namely acs, manual and smc:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   */
  void unmountTape(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

  /**
   * Enumeration of the different types of drive.
   */
  enum RmcDriveType {
    RMC_DRIVE_TYPE_ACS,
    RMC_DRIVE_TYPE_MANUAL,
    RMC_DRIVE_TYPE_SCSI,
    RMC_DRIVE_TYPE_UNKNOWN};

  /**
   * Returns the type of the specified drive.
   *
   * @param drive The drive in one of the following three forms corresponding
   * to the three supported drive-loader types, namely acs, manual and smc:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   * @return The drive type.
   */
  RmcDriveType getDriveType(const std::string &drive) throw();

protected:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The name of the host on which the rmcd daemon is running.
   */
  const std::string m_rmcHostName;

  /**
   * The TCP/IP port on which the rmcd daemon is listening.
   */
  const unsigned short m_rmcPort;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * specified drive in an ACS compatible tape-library.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The drive in the following form:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER".
   */
  void mountTapeAcs(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

  /**
   * Logs a request to the tape-operator to manually mount the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void mountTapeManual(const std::string &vid) throw(castor::exception::Exception);

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * specified drive in a SCSI compatible tape-library.
   *    
   * @param vid The volume identifier of the tape.
   * @param drive The drive in the following form:
   * "smc@rmc_host,drive_ordinal".
   */
  void mountTapeScsi(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * specified drive in an ACS compatible tape-library.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The drive in the following form:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER".
   */
  void unmountTapeAcs(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

  /**
   * Logs a request to the tape-operator to manually unmount the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void unmountTapeManual(const std::string &vid) throw(castor::exception::Exception);

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * specified drive in a SCSI compatible tape-library.
   *    
   * @param vid The volume identifier of the tape.
   * @param drive The drive in the following form:
   * "smc@rmc_host,drive_ordinal".
   */
  void unmountTapeScsi(const std::string &vid, const std::string &drive) throw(castor::exception::Exception);

}; // class RmcImpl

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

