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
#include "castor/legacymsg/NsProxyFactory.hpp"
#include "castor/legacymsg/RmcProxyFactory.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/messages/TapeserverProxyFactory.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
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
   * @param driveConfig The configuration of the tape drives.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param vmgr Proxy object representing the vmgrd daemon.
   * @param rmcFactory Factory to create proxy objects representing the rmcd
   * daemon.
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
    const utils::DriveConfigMap &driveConfigs,
    legacymsg::VdqmProxy &vdqm,
    legacymsg::VmgrProxy &vmgr,
    legacymsg::RmcProxyFactory &rmcFactory,
    messages::TapeserverProxyFactory &tapeserverProxyFactory,
    legacymsg::NsProxyFactory &nsProxyFactory,
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
   * Forks the ProcessForker.
   */
  void forkProcessForker();

  /**
   * Runs the ProcessForker.
   *
   * @param cmdReceiverSocket The socket used to receive commands for the
   * ProcessForker.
   * @return the exit code to be used for the process running the ProcessForker.
   */
  int runProcessForker(const int cmdReceiverSocket) throw();

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
   */
  void setUpReactor() ;

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
   * Creates the handler to discuss through zmq socket to the forked sessions
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
   * Handles the specified reaped session.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedSession(const pid_t pid, const int waitpidStat) throw();

  /**
   * Logs the fact that the specified child process has terminated.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logChildProcessTerminated(const pid_t pid, const int waitpidStat)
    throw();

  /**
   * Dispatches the appropriate post-processor of repaed sessions based on
   * the specified session type.
   *
   * @sessionType The type of the reaped session.
   * @param pid The process ID of the reaped session.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void dispatchReapedSessionHandler(
   const DriveCatalogueEntry::SessionType sessionType,
   const pid_t pid,
   const int waitpidStat);

  /**
   * Does the required post processing for the specified reaped session.
   *
   * @param pid The process ID of the reaped session.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedDataTransferSession(const pid_t pid,
    const int waitpidStat);
  
  /**
   * Does the required post processing for the specified reaped session.
   *
   * @param pid The process ID of the reaped session.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedCleanerSession(const pid_t pid,
    const int waitpidStat);

  /**
   * Sets the state of the tape drive asscoiated with the specified
   * child process to down within the vdqmd daemon.
   *
   * @param pid The process ID of the child process.
   * @param driveConfig The configuration of the tape drive.
   */
  void setDriveDownInVdqm(const pid_t pid,
    const utils::DriveConfig &driveConfig);
  
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
   * Handles the specified reaped session.
   *
   * @param pid The process ID of the reaped session.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void handleReapedLabelSession(const pid_t pid, const int waitpidStat);

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
   * Forks a data-transfer process for every tape drive entry in the
   * tape drive catalogue that is waiting for such a fork to be carried out.
   *
   * There may be more than one child-process to fork because there may be
   * more than one tape drive connected to the tape server and the previous
   * call to m_reactor.handleEvents() handled requests to start a mount
   * session on more than one of the connected tape drives.
   */
  void forkDataTransferSessions() throw();

  /**
   * Forks a data-transfer process for the specified tape drive.
   *
   * @param drive The tape-drive entry in the tape-drive catalogue.
   */ 
  void forkDataTransferSession(DriveCatalogueEntry *drive) throw();

  /**
   * Runs the data-transfer session.  This method is to be called within the
   * child process responsible for running the data-transfer session.
   *
   * @param drive The catalogue entry of the tape drive to be used during the
   * session.
   */
  void runDataTransferSession(const DriveCatalogueEntry *drive) throw();

  /**
   * Forks a label-session child-process for every tape drive entry in the
   * tape drive catalogue that is waiting for such a fork to be carried out.
   *
   * There may be more than one child-process to fork because there may be
   * more than one tape drive connected to the tape server and the previous
   * call to m_reactor.handleEvents() handled requests to start a label
   * session on more than one of the connected tape drives.
   */
  void forkLabelSessions();

  /**
   * Forks a label-session child-process for the specified tape drive.
   *
   * @param drive The tape-drive entry in the tape-drive catalogue.
   */
  void forkLabelSession(DriveCatalogueEntry *drive);

  /**
   * Runs the label session. This method is to be called within the child
   * process responsible for running the label session.
   *
   * @param drive The catalogue entry of the tape drive to be used during the
   * session.
   * @param labelCmdConnection The file descriptor of the connection with the
   * command-line tool castor-tape-label.
   */
  void runLabelSession(const DriveCatalogueEntry *drive,
    const int labelCmdConnection) throw();
  
  /**
   * Forks a cleaner-session child-process for every tape drive entry in the
   * tape drive catalogue that is waiting for such a fork to be carried out.
   *
   * There may be more than one child-process to fork because there may be
   * more than one tape drive connected to the tape server and the previous
   * call to m_reactor.handleEvents() handled requests to start a cleaner
   * session on more than one of the connected tape drives.
   */
  void forkCleanerSessions();

  /**
   * Forks a cleaner-session child-process for the specified tape drive.
   *
   * @param drive The tape-drive entry in the tape-drive catalogue.
   */
  void forkCleanerSession(DriveCatalogueEntry *drive);

  /**
   * Runs the cleaner session. This method is to be called within the child
   * process responsible for running the cleaner session.
   *
   * @param drive The catalogue entry of the tape drive to be used during the
   * session.
   * @param vid The identifier of the mounted volume (one can pass the empty string 
   * in case it is unknown, as it not used except for logging purposes).
   */
  void runCleanerSession(DriveCatalogueEntry *drive, const std::string &vid) throw();

  /**
   * Catalogue used to keep track of both the initial and current state of
   * each tape drive being controlled by the tapeserverd daemon.
   */
  DriveCatalogue m_driveCatalogue;

  /**
   * The argc of main().
   */
  const int m_argc;

  /**
   * The argv of main().
   */
  char **const m_argv;

  /**
   * The configuration of the tape drives.
   */
  const utils::DriveConfigMap m_driveConfigs;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * Proxy object representing the vmgrd daemon.
   */
  legacymsg::VmgrProxy &m_vmgr;

  /**
   * Factory to create proxy objects representing the rmcd daemon.
   */
  legacymsg::RmcProxyFactory &m_rmcFactory;
  
  /**
   * The proxy object representing the tapeserver daemon.
   */
  messages::TapeserverProxyFactory &m_tapeserverFactory;
  
  /**
   * The proxy object representing the nameserver daemon.
   */
  legacymsg::NsProxyFactory &m_nsFactory;

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
   * The ZMQ context.
   */
  void *m_zmqContext;

}; // class TapeDaemon

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

