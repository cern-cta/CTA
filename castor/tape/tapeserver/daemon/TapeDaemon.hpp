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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"
#include "castor/tape/utils/DriveConfigMap.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"

#include <iostream>
#include <list>
#include <map>
#include <poll.h>
#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Daemon responsible for reading and writing data from and to one or more tape
 * drives drives connected to a tape server.
 */
class TapeDaemon : public castor::server::Daemon {

public:

  /**
   * Constructor.
   *
   * @param argc The argc of main().
   * @param argv The argv of main().
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param driveConfig The configuration of the tape drives.
   * @param cupv Proxy object representing the cupvd daemon.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param vmgr Proxy object representing the vmgrd daemon.
   * @param reactor The reactor responsible for dispatching the I/O events of
   * the parent process of the tape server daemon.
   * @param capUtils Object providing utilities for working UNIX capabilities.
   */
  TapeDaemon(
    const int argc,
    char **const argv,
    std::ostream &stdOut,
    std::ostream &stdErr,
    log::Logger &log,
    const int netTimeout,
    const utils::DriveConfigMap &driveConfigs,
    legacymsg::CupvProxy &cupv,
    legacymsg::VdqmProxy &vdqm,
    legacymsg::VmgrProxy &vmgr,
    reactor::ZMQReactor &reactor,
    castor::server::ProcessCap &capUtils);

  /**
   * Destructor.
   */
  ~TapeDaemon() throw();

  /**
   * The main entry function of the daemon.
   *
   * @return The return code of the process.
   */
  int main() throw();
  
protected:

  /**
   * Returns the name of the host on which the daemon is running.
   */
  std::string getHostName() const;

  /**
   * Exception throwing main() function.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  void exceptionThrowingMain(const int argc, char **const argv);

  /**
   * Logs the start of the daemon.
   */
  void logStartOfDaemon(const int argc, const char *const *const argv) throw();

  /**
   * Creates a string that contains the specified command-line arguments
   * separated by single spaces.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  std::string argvToString(const int argc, const char *const *const argv)
    throw();

  /**
   * Idempotent method that destroys the ZMQ context.
   */
  void destroyZmqContext() throw();

  /**
   * Sets the dumpable attribute of the current process to true.
   */
  void setDumpable();

  /**
   * Sets the capabilities of the current process.
   *
   * @text The string representation the capabilities that the current
   * process should have.
   */
  void setProcessCapabilities(const std::string &text);

  /**
   * Socket pair used to send commands to the ProcessForker.
   */
  struct ForkerCmdPair {

    /**
     * Bi-directional socket used by the TapeDaemon parent process to send
     * commands to the process forker and receive replies in return.
     */
    int tapeDaemon;

    /**
     * Bi-directional socket used by the ProcessForker to receive commands
     * from the TapeDaemon parent process and send back replies.
     */
    int processForker;

    /**
     * Constructor.
     *
     * This constructor sets both members to -1 which represents an invalid
     * file descriptor.
     */
    ForkerCmdPair(): tapeDaemon(-1), processForker(-1) {
    }
  }; // struct ForkerCmdPair

  /**
   * Creates the socket pair to be used to control the ProcessForker.
   *
   * @return The socket pair.
   */
  ForkerCmdPair createForkerCmdPair();

  /**
   * Socket pair used by the ProcessForker to notify the TapeDaemon parent
   * process of the termination of ProcessForker child processes.
   */
  struct ForkerReaperPair {

    /**
     * Socket used by the TapeDaemon receive process termination notifications
     * from the ProcessForker.
     */
    int tapeDaemon;

    /**
     * Socket used by the ProcessForker to send process termination
     * notifications to the TapeDaemon parent process.
     */
    int processForker;

    /**
     * Constructor.
     *
     * This constructor sets both members to -1 which represents an invalid
     * file descriptor.
     */
    ForkerReaperPair(): tapeDaemon(-1), processForker(-1) {
    }
  }; // struct ForkerReaperPair

  /**
   * Creates the socket pair to be used by the ProcessForker to notify the
   * TapeDaemon parent process of the termination of ProcessForker processes.
   *
   * @return The socket pair.
   */
  ForkerReaperPair createForkerReaperPair();

  /**
   * C++ wrapper around socketpair() that converts a failure into a C++
   * exception.
   *
   * @return The socket pair.
   */
  std::pair<int, int> createSocketPair();

  /**
   * Forks the ProcessForker and closes the appropriate communication sockets
   * within the parent and child processes.
   *
   * PLEASE NOTE: No sockets should be registered with m_reactor before this
   * method is called.  This method will NOT call m_reactor.clear() in the
   * client process.  This is because it is possible to put ZMQ sockets into the
   * reactor and one should not manipulate such sockets in two difefrent threads
   * or processes.  Specifically do not call setUpReactor() until
   * forkProcessForker() has been called.
   *
   * @param cmdPair Socket pair used to send commands to the ProcessForker.
   * @param reaperPair Socket pair used by the ProcessForker to notify the
   * TapeDaemon parent process of the termination of ProcessForker child
   * processes.
   * @return The process identifier of the ProcessForker.
   */
  pid_t forkProcessForker(const ForkerCmdPair &cmdPair,
    const ForkerReaperPair &reaperPair);

  /**
   * Closes both the sockets of the specified socket pair.
   *
   * @param cmdPair The socket pair to be close.
   */
  void closeForkerCmdPair(const ForkerCmdPair &cmdPair) const;

  /**
   * Closes both the sockets of the specified socket pair.
   *
   * @param reaperPair The socket pair to be close.
   */
  void closeForkerReaperPair(const ForkerReaperPair &reaperPair) const;

  /**
   * Acting on behalf of the TapeDaemon parent process this method closes the
   * ProcessForker side of the socket pair used to control the ProcessForker.
   *
   * @param cmdPair The socket pair used to control the ProcessForker.
   */
  void closeProcessForkerSideOfCmdPair(const ForkerCmdPair &cmdPair) const;

  /**
   * Acting on behalf of the TapeDaemon parent process this method closes the
   * ProcessForker side of the socket pair used by the ProcessForker to report
   * process terminations.
   *
   * @param reaperPair The socket pair used by the ProcessForker to report
   * process terminations.
   */
  void closeProcessForkerSideOfReaperPair(const ForkerReaperPair &reaperPair)
    const;

  /**
   * Acting on behalf of the ProcessForker process this method closes the
   * TapeDaemon side of the socket pair used to control the ProcessForker.
   *
   * @param cmdPair The socket pair used to control the ProcessForker.
   */
  void closeTapeDaemonSideOfCmdPair(const ForkerCmdPair &cmdPair) const;

  /**
   * Acting on behalf of the ProcessForker process this method closes the
   * TapeDaemon side of the socket pair used by the ProcessForker to report
   * process terminations.
   *
   * @param reaperPair The socket pair used by the ProcessForker to report
   * process terminations.
   */
  void closeTapeDaemonSideOfReaperPair(const ForkerReaperPair &reaperPair)
    const;

  /**
   * Runs the ProcessForker.
   *
   * @param cmdReceiverSocket The socket used to receive commands for the
   * ProcessForker.
   * @param reaperSenderSocket The socket used to send process termination
   * reports to the TapeDaemon parent process.
   * @return the exit code to be used for the process running the ProcessForker.
   */
  int runProcessForker(const int cmdReceiverSocket,
    const int reaperSenderSocket) throw();

  /**
   * Blocks the signals that should not asynchronously disturb the daemon.
   */
  void blockSignals() const;

  /**
   * Registers the tape drives controlled by the tape server daemon with the
   * vdqmd daemon.
   */
  void registerTapeDrivesWithVdqm() ;

  /**
   * Registers the specified tape drive with ethe vdqmd daemon.
   */
  void registerTapeDriveWithVdqm(const std::string &unitName);

  /**
   * Initialises the ZMQ context.
   */
  void initZmqContext();

  /**
   * Sets up the reactor.
   *
   * @param reaperSocket The TapeDaemon side of the socket pair used by the
   * ProcessForker  to report the termination of its child processes.
   */
  void setUpReactor(const int reaperSocket);

  /**
   * Creates the handler to handle the incoming connection from the
   * ProcessForker.
   *
   * @param reaperSocket The TapeDaemon side of the socket pair used by the
   * ProcessForker  to report the termination of its child processes.
   */
  void createAndRegisterProcessForkerConnectionHandler(const int reaperSocket);

  /**
   * Creates the handler to accept connections from the vdqmd daemon and
   * registers it with the reactor.
   */
  void createAndRegisterVdqmAcceptHandler() ;

  /**
   * Creates the handler to accept connections from the admin commands and
   * registers it with the reactor.
   */
  void createAndRegisterAdminAcceptHandler() ;

  /**
   * Creates the handler to accept connections from the label tape
   * command-line tool and registers it with the reactor.
   */
  void createAndRegisterLabelCmdAcceptHandler();

  /**
   * Creates the handler to handle messages from forked sessions.
   */
  void createAndRegisterTapeMessageHandler();
  
  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop();

  /**
   * Handles any pending events.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleEvents();

  /**
   * Handles any pending signals.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handlePendingSignals() throw();

  /**
   * Reaps any zombie processes.
   */
  void reapZombies() throw();

  /**
   * Handles the specified reaped process.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedProcess(const pid_t pid, const int waitpidStat) throw();

  /**
   * Handles the specified reaped ProcessForker.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedProcessForker(const pid_t pid, const int waitpidStat)
    throw();

  /**
   * Logs the fact that the specified child process has terminated.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logChildProcessTerminated(const pid_t pid, const int waitpidStat)
    throw();

  /**
   * Marshals the specified source tape rc reply message structure into the
   * specified destination buffer.
   *
   * @param dst    The destination buffer.
   * @param dstLen The length of the destination buffer.
   * @param rc     The return code to reply.
   * @return       The total length of the header.
   */
  size_t marshalTapeRcReplyMsg(char *const dst, const size_t dstLen,
    const int rc) ;
  
  /**
   * Request the vdqmd daemon to release the tape drive associated with the
   * session child-process with the specified process ID.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param pid The process ID of the session child-process.
   */
  void requestVdqmToReleaseDrive(const utils::DriveConfig &driveConfig,
    const pid_t pid);

  /**
   * Notifies the vdqm that the tape associated with the session child-process
   * with the specified process ID has been unmounted.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid The identifier of the unmounted volume.
   * @param pid The process ID of the session child-process.
   */
  void notifyVdqmTapeUnmounted(const utils::DriveConfig &driveConfig,
    const std::string &vid, const pid_t pid);

  /**
   * The argc of main().
   */
  const int m_argc;

  /**
   * The argv of main().
   */
  char **const m_argv;

  /**
   * Timeout in seconds to be used when performing network I/O.
   */
  const int m_netTimeout;

  /**
   * The configuration of the tape drives.
   */
  const utils::DriveConfigMap m_driveConfigs;

  /**
   * Proxy object representing the cupvd daemon.
   */
  legacymsg::CupvProxy &m_cupv;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * Proxy object representing the vmgrd daemon.
   */
  legacymsg::VmgrProxy &m_vmgr;

  /**
   * The reactor responsible for dispatching the file-descriptor event-handlers
   * of the tape server daemon.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * Object providing utilities for working UNIX capabilities.
   */
  castor::server::ProcessCap &m_capUtils;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;

  /**
   * Proxy object used to send commands to the ProcessForker.
   */
  ProcessForkerProxy *m_processForker;

  /**   
   * The process identifier of the ProcessForker.
   */
  pid_t m_processForkerPid;

  /**
   * Catalogue used to keep track of both the initial and current state of
   * each tape drive being controlled by the tapeserverd daemon.
   */
  Catalogue *m_catalogue;

  /**
   * The ZMQ context.
   */
  void *m_zmqContext;

}; // class TapeDaemon

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

