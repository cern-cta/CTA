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

#include "castor/log/Logger.hpp"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerFrame.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerMsgType.hpp"

#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for forking processes.
 */
class ProcessForker {
public:

  /**
   * Constructor.
   *
   * This class takes ownership of the socket used to communicate with the
   * client.  The destructor of this class will close the file-descriptor.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param cmdSocket The file-descriptor of the socket used to receive commands
   * from the ProcessForker proxy.
   * @param reaperSocket The file-descriptor of the socket used to notify the
   * TapeDaemon parent process of the termination of a process forked by the
   * ProcessForker.
   * @param hostName The name of the host on which the tapeserverd daemon is
   * running.
   */
  ProcessForker(log::Logger &log, const int cmdSocket, const int reaperSocket,
    const std::string &hostName) throw();

  /**
   * Destructor.
   *
   * Closes the  file-descriptor of the socket used to communicate with the
   * client.
   */
  ~ProcessForker() throw();

  /**
   * Executes the main event loop of the ProcessForker.
   */
  void execute();

private:

  /**
   * The maximum permitted size in bytes for the payload of a frame sent between
   * the ProcessForker and its proxy.
   */
  static const ssize_t s_maxPayloadLen = 1024;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The file-descriptor of the socket used to receive commands from the
   * ProcessForker proxy.
   */
  const int m_cmdSocket;

  /**
   * The file-descriptor of the socket used to notify the TapeDaemon parent
   * process of the termination of a process forked by the ProcessForker.
   */
  const int m_reaperSocket;

  /**
   * The name of the host on which the tapeserverd daemon is running.
   */ 
  const std::string m_hostName;

  /**
   * Idempotent method that closes the socket used for receving commands
   * from the ProcessForker proxy.
   */
  void closeCmdReceiverSocket() throw();

  /**
   * Structure defining the result of a message handler.
   */
  struct MsgHandlerResult {
    /**
     * True if the main event loop should continue.
     */
    bool continueMainEventLoop;

    /**
     * The reply frame.
     */
    ProcessForkerFrame reply;

    /**
     * Constructor.
     */
    MsgHandlerResult() throw():
      continueMainEventLoop(false) {
    }
  }; // struct MsgHandlerResult

  /**
   * Handles any pending events.
   *
   * @return true if the main event loop should continue.
   */
  bool handleEvents();

  /**
   * Return strue if there is a pending message from the ProcessForker proxy.
   */
  bool thereIsAPendingMsg();

  /**
   * Reads in and handles a single message from the ProcessForker proxy.
   */
  bool handleMsg();

  /**
   * Dispatches the appropriate handler method for the message contained within
   * the specified frame;
   *
   * @param frame The frame containing the message.
   * @return The result of dispatching the message handler.
   */
  MsgHandlerResult dispatchMsgHandler(const ProcessForkerFrame &frame);

  /**
   * Handles a StopProcessForker message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleStopProcessForkerMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkLabel message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkLabelMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkDataTransfer message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkDataTransferMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkCleaner message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkCleanerMsg(const ProcessForkerFrame &frame);

  /**
   * Runs the data-transfer session.  This method is to be called within the
   * child process responsible for running the data-transfer session.
   *
   * @param rqst The ForkDataTransfer message.
   * @return The value to be used when exiting the child process.
   */
  int runDataTransferSession(const messages::ForkDataTransfer &rqst);

  /**
   * Gets the drve configuration information from the specified ForkDataTransfer
   * message.
   *
   * @param msg The ForkDataTransfer message.
   * @return The drive configuration.
   */
  utils::DriveConfig getDriveConfig(const messages::ForkDataTransfer &msg);

  /**
   * Gets the configuration of the data-transfer session from the specified
   * ForkDataTransfer message.
   *
   * @param msg The ForkDataTransfer message.
   * @return The configuration of the data-transfer session.
   */
  castor::tape::tapeserver::daemon::DataTransferSession::CastorConf
    getDataTransferConfig(const messages::ForkDataTransfer &msg);

  /**
   * Gets the VDQM job from the specified ForkDataTransfer message.
   *
   * @param msg The ForkDataTransfer message.
   * @return The VDQM job.
   */
  castor::legacymsg::RtcpJobRqstMsgBody getVdqmJob(
    const messages::ForkDataTransfer &msg);

  /**
   * Instantiates a ZMQ context.
   *
   * @param sizeOfIOThreadPoolForZMQ The size of the IO thread pool to be used
   * by ZMQ.
   * @return The ZMQ context.
   */
  void *instantiateZmqContext(const int sizeOfIOThreadPoolForZMQ);

  /**
   * Reaps any zombie processes.
   */
  void reapZombies() throw();

  /**
   * Handles the specified reaped zombie.
   *
   * @param pid The process ID of the reaped zombie.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedZombie(const pid_t pid, const int waitpidStat) throw();

  /**
   * Logs the fact that the specified child process has terminated.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logChildProcessTerminated(const pid_t pid, const int waitpidStat)
    throw();

  /**
   * Notifies the TapeDaemon parent process that a child process of the
   * ProcessForker has terminated.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void notifyTapeDaemonOfTerminatedProcess(const pid_t pid,
    const int waitpidStat);

  /**
   * Notifies the TapeDaemon parent process that a child process of the
   * ProcessForker exited.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void notifyTapeDaemonOfExitedProcess(const pid_t pid,
    const int waitpidStat);

  /**
   * Notifies the TapeDaemon parent process that a child process of the
   * ProcessForker crashed.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void notifyTapeDaemonOfCrashedProcess(const pid_t pid,
    const int waitpidStat);

}; // class ProcessForker

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
