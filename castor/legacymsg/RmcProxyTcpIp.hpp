/******************************************************************************
 *         castor/tape/tapeserver/daemon/RmcProxyTcpIp.hpp
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
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/RmcMountMsgBody.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/legacymsg/RmcUnmountMsgBody.hpp"

#include <unistd.h>
#include <sys/types.h>

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the rmc daemon.
 */
class RmcProxyTcpIp: public RmcProxy {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  RmcProxyTcpIp(log::Logger &log, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~RmcProxyTcpIp() throw();

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in one of the following three forms:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   */
  void mountTape(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in one of the following three forms:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   */
  void unmountTape(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Enumeration of the different types of library slot.
   */
  enum RmcLibrarySlotType {
    RMC_LIBRARY_SLOT_TYPE_ACS,
    RMC_LIBRARY_SLOT_TYPE_MANUAL,
    RMC_LIBRARY_SLOT_TYPE_SCSI,
    RMC_LIBRARY_SLOT_TYPE_UNKNOWN};

  /**
   * Returns the type of the specified string representation of a library slot.
   *
   * @param librarySlot The library slot in one of the following three forms:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   * @return The library slot type.
   */
  RmcLibrarySlotType getLibrarySlotType(const std::string &librarySlot) throw();

protected:

  /**
   * The size of buffer used to marshal or unmarshal RMC messages.
   */
  static const int RMC_MSGBUFSIZ = 256;

  /**
   * The user ID of the current process.
   */
  const uid_t m_uid;

  /**
   * The group ID of the current process.
   */
  const gid_t m_gid; 

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * tape drive located in the specified ACS library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in the following form:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER".
   */
  void mountTapeAcs(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Logs a request to the tape-operator to manually mount the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void mountTapeManual(const std::string &vid) ;

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * drive located in the specified SCSI library slot.
   *    
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in the following form:
   * "smc@rmc_host,drive_ordinal".
   */
  void mountTapeScsi(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * drive located in the specified an ACS library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in the following form:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER".
   */
  void unmountTapeAcs(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Logs a request to the tape-operator to manually unmount the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void unmountTapeManual(const std::string &vid) ;

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * drive located in the specified SCSI compatible library slot.
   *    
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in the following form:
   * "smc@rmc_host,drive_ordinal".
   */
  void unmountTapeScsi(const std::string &vid, const std::string &librarySlot) ;

  /**
   * Connects to the rmcd daemon.
   *
   * @param hostName The name of the host on which the rmcd daemon is running.
   * @return The socket-descriptor of the connection with the rmcd daemon.
   */
  int connectToRmc(const std::string &hostName) const ;

  /**
   * Writes an RMC_SCSI_MOUNT message with the specifed body to the specified
   * connection.
   *
   * @param fd The file descriptor of the connection.
   * @param body The body of the message.
   */
  void writeRmcMountMsg(const int fd, const RmcMountMsgBody &body) ;

  /**
   * Reads the header of an RMC_MAGIC message from the specified connection.
   *
   * @param fd The file descriptor of the connection.
   * @return The message header.
   */
  MessageHeader readRmcMsgHeader(const int fd) ;

  /**
   * Writes an RMC_SCSI_UNMOUNT message with the specifed body to the specified
   * connection.
   *
   * @param fd The file descriptor of the connection.
   * @param body The body of the message.
   */
  void writeRmcUnmountMsg(const int fd, const RmcUnmountMsgBody &body) ;

}; // class RmcProxyTcpIp

} // namespace legacymsg
} // namespace castor

