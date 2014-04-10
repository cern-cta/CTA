/******************************************************************************
 *         castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"
#include "castor/tape/tapeserver/daemon/Vmgr.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sys/types.h>
#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A temporary class implementing a mount session in order to test the vdqm
 * protocol.
 */
class DebugMountSessionForVdqmProtocol {

public:

  /**
   * Constructor.
   *
   * @param hostName The name of the host on which the tapeserverd daemon is running.
   * @param job The job received from the vdwqmd daemon.
   * @param logger The interface to the CASTOR logging system.
   * @param tpConfig The contents of /etc/castor/TPCONFIG.
   * @param vdqm The object representing the vdqmd daemon.
   * @param vmgr The object representing the vmgrd daemon.
   */
  DebugMountSessionForVdqmProtocol(
    const std::string &hostName,
    const legacymsg::RtcpJobRqstMsgBody &job,
    castor::log::Logger &logger,
    const utils::TpconfigLines &tpConfig,
    Vdqm &vdqm,
    Vmgr &vmgr) throw();

  /**
   * The only method. It will execute (like a task, that it is)
   */
  void execute() throw (castor::exception::Exception);

private:

  /**
   * Timeout in seconds to be used for network operations.
   */
  const int m_netTimeout;

  /**
   * The ID of the mount-session process.
   */
  const pid_t m_sessionPid;

  /**
   * The name of the host on which the tapeserverd daemon is running.
   */
  const std::string m_hostName;

  /**
   * The job received from the vdwqmd daemon.
   */
  const legacymsg::RtcpJobRqstMsgBody m_job;

  /**
   *
   */
  castor::log::Logger &m_log;

  /**
   * The contents of /etc/castor/TPCONFIG.
   */
  const utils::TpconfigLines &m_tpConfig;

  /**
   * The object representing the vdqmd daemon.
   */
  Vdqm &m_vdqm;

  /**
   * The object representing the vmgrd daemon.
   */
  Vmgr &m_vmgr;

  /**
   * Tells the vdqm to assign the process ID of the mount session process to
   * the tape drive.
   */
  void assignSessionPidToDrive() throw (castor::exception::Exception);

  /**
   * Mounts the tape and notifies the vdqm accordingly.
   *
   * @param vid The volume identifier of the tape.
   */
  void mountTape(const std::string &vid) throw (castor::exception::Exception);

  /**
   * Transfers file to/from the tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void transferFiles(const std::string &vid) throw (castor::exception::Exception);

  /**
   * Requests the vdqm to release the tape drive.
   *
   * @param forceUnmount Set to true if the release of the tape drive must
   * result in the tape being unmounted.
   */
  void releaseDrive(const bool forceUnmount) throw (castor::exception::Exception);

  /**
   * Dismounts the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void unmountTape(const std::string &vid) throw (castor::exception::Exception);

  /**
   * Gets the volume to be mounted from the client.
   *
   * clientMsgSeqNb           Client-message sequence number.
   *
   * @return                  A pointer to the volume message received from the
   *                          client or NULL if there is no volume to mount.
   *                          The caller is responsible for deallocating the
   *                          message.
   */
  tapegateway::Volume *getVolume(const uint64_t clientMsgSeqNb) const throw(castor::exception::Exception);

  /**
   * Connects to the client, sends the specified request message and then
   * receives the reply.
   *
   * @param requestTypeName  The name of the type of the request.  This name
   *                         will be used in logging messages.
   * @param request          Out parameter: The request to be sent to the
   *                         client.
   * @return                 A pointer to the reply object.  It is the
   *                         responsibility of the caller to deallocate the
   *                         memory of the reply object.
   */
  IObject *connectSendRequestAndReceiveReply(const char *const requestTypeName, IObject &request) const throw(castor::exception::Exception);

  /**
   * Throws an exception if there is a mount transaction ID mismatch and/or
   * a aggregator transaction ID mismatch.
   *
   * @param messageTypeName           The type name of the client message.
   * @param actualMountTransactionId  The actual mount transaction ID.
   * @param expectedTapebridgeTransId The expected aggregator transaction ID.
   * @param actualTapebridgeTransId   The actual aggregator transaction ID.
   */
  void checkTransactionIds(
    const char *const messageTypeName,
    const uint32_t    actualMountTransactionId,
    const uint64_t    expectedTapebridgeTransId,
    const uint64_t    actualTapebridgeTransId) const
    throw(castor::exception::Exception);

  /**
   * Translates the specified incoming EndNotificationErrorReport message into
   * the throwing of the appropriate exception.
   *
   * This method is not typical because it throws an exception if it succeeds.
   *
   * @param obj The received IObject which represents the
   *            OBJ_EndNotificationErrorReport message.
   */
  void throwEndNotificationErrorReport(IObject *const obj) const throw(castor::exception::Exception);

}; // class DebugMountSessionForVdqmProtocol

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

