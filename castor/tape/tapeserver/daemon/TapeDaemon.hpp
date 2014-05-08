/******************************************************************************
 *         castor/tape/tapeserver/daemon/TapeDaemon.hpp
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
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/io/PollReactor.hpp"
#include "castor/legacymsg/RmcProxyFactory.hpp"
#include "castor/legacymsg/TapeserverProxyFactory.hpp"
#include "castor/legacymsg/VdqmProxyFactory.hpp"
#include "castor/legacymsg/VmgrProxyFactory.hpp"
#include "castor/server/Daemon.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/utils/TpconfigLines.hpp"
#include "castor/tape/utils/utils.hpp"

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
   * @param tpconfigLines The parsed lines of /etc/castor/TPCONFIG.
   * @param vdqmFactory Factory to create proxy objects representing the vdqmd
   * daemon.
   * @param vmgrFactory Factory to create proxy objects representing the vmgrd
   * daemon.
   * @param rmcFactory Factory to create proxy objects representing the rmcd
   * daemon.
   * @param reactor The reactor responsible for dispatching the I/O events of
   * the parent process of the tape server daemon.
   */
  TapeDaemon(
    const int argc,
    char **const argv,
    std::ostream &stdOut,
    std::ostream &stdErr,
    log::Logger &log,
    const utils::TpconfigLines &tpconfigLines,
    legacymsg::VdqmProxyFactory &vdqmFactory,
    legacymsg::VmgrProxyFactory &vmgrFactory,
    legacymsg::RmcProxyFactory &rmcFactory,
    legacymsg::TapeserverProxyFactory &tapeserverProxyFactory,
    io::PollReactor &reactor) throw(castor::exception::Exception);

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
  std::string getHostName() const throw(castor::exception::Exception);

  /**
   * Exception throwing main() function.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  void exceptionThrowingMain(const int argc, char **const argv)
    throw(castor::exception::Exception);

  /**
   * Logs the start of the daemon.
   */
  void logStartOfDaemon(const int argc, const char *const *const argv) throw();

  /**
   * Parses the /etc/castor/TPCONFIG files in order to determine the drives
   * attached to the tape server.
   */
  void parseTpconfig() throw(castor::exception::Exception);

  /**
   * Writes the specified TPCONFIG lines to the logging system.
   *
   * @param lines The lines parsed from /etc/castor/TPCONFIG.
   */
  void logTpconfigLines(const utils::TpconfigLines &lines) throw();

  /**
   * Writes the specified TPCONFIG lines to the logging system.
   *
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void logTpconfigLine(const utils::TpconfigLine &line) throw();

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
   * Blocks the signals that should not asynchronously disturb the daemon.
   */
  void blockSignals() const throw(castor::exception::Exception);

  /**
   * Registers the tape drives controlled by the tape server daemon with the
   * vdqmd daemon.
   */
  void registerTapeDrivesWithVdqm() throw(castor::exception::Exception);

  /**
   * Registers the specified tape drive with ethe vdqmd daemon.
   */
  void registerTapeDriveWithVdqm(const std::string &unitName)
    throw(castor::exception::Exception);

  /**
   * Sets up the reactor.
   */
  void setUpReactor() throw(castor::exception::Exception);

  /**
   * Creates the handler to accept connections from the vdqmd daemon and
   * registers it with the reactor.
   */
  void createAndRegisterVdqmAcceptHandler() throw(castor::exception::Exception);

  /**
   * Creates the handler to accept connections from the admin commands and
   * registers it with the reactor.
   */
  void createAndRegisterAdminAcceptHandler() throw(castor::exception::Exception);
  
  /**
   * Creates the handler to accept connections from the mount session(s) and
   * registers it with the reactor.
   */
  void createAndRegisterMountSessionAcceptHandler() throw(castor::exception::Exception);

  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop() throw(castor::exception::Exception);

  /**
   * Handles any pending events.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleEvents() throw(castor::exception::Exception);

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
   * Reaps the specified zombie process.
   *
   * @param sessionPid The process ID of the zombie mount-session child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void reapZombie(const pid_t sessionPid, const int waitpidStat) throw();

  /**
   * Logs the fact that the specified mount-session child-process has terminated.
   *
   * @param sessionPid The process ID of the mount-session child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logMountSessionProcessTerminated(const pid_t sessionPid, const int waitpidStat) throw();

  /**
   * Does the required post processing for the specified reaped session.
   *
   * @param sessionPid The process ID of the session child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void postProcessReapedDataTransferSession(const pid_t sessionPid, const int waitpidStat) throw();

  /**
   * Sets the state of the tape drive asscoiated with the specified
   * mount-session process to down within the vdqmd daemon.
   *
   * If for any reason the state of the drive within the vdqmd daemon cannot
   * be set to down, then this method logs an appropriate error message but
   * does not throw an exception.
   *
   * @param sessionPid The process ID of the mount-session child-process.
   */
  void setDriveDownInVdqm(const pid_t sessionPid) throw();
  
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
    const int rc) throw(castor::exception::Exception);
  
  /**
   * Writes a job reply message to the specified connection.
   *
   * @param fd The file descriptor of the connection with the admin command.
   * @param rc The return code to reply.
   * 
   */
  void writeTapeRcReplyMsg(const int fd, const int rc)
    throw(castor::exception::Exception);

  /**
   * Does the required post processing for the specified reaped session.
   *
   * @param sessionPid The process ID of the session child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void postProcessReapedLabelSession(const pid_t sessionPid, const int waitpidStat) throw();

  /**
   * Notifies the vdqm that the tape associated with the session child-process
   * with the specified process ID has been unmounted.
   *
   * @param sessionPid The process ID of the session child-process.
   */
  void notifyVdqmTapeUnmounted(const pid_t sessionPid) throw();

  /**
   * Notifies the associated client label-command of the end of the label
   * session.
   *
   * @param sessionPid The process ID of the session child-process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void notifyLabelCmdOfEndOfSession(const pid_t sessionPid, const int waitpidStat)
    throw(castor::exception::Exception);

  /**
   * Forks a mount-session child-process for every tape drive entry in the
   * tape drive catalogue that is waiting for such a fork to be carried out.
   *
   * There may be more than one child-process to fork because there may be
   * more than one tape drive connected to the tape server and the previous
   * call to m_reactor.handleEvents() handled requests to start a mount
   * session on more than one of the connected tape drives.
   */
  void forkMountSessions() throw();

  /**
   * Forks a mount-session child-process for the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */ 
  void forkMountSession(const std::string &unitName) throw();

  /**
   * Runs the mount session.  This method is to be called within the child
   * process responsible for running the mount session.
   *
   * @param unitName The unit name of the tape drive.
   */
  void runMountSession(const std::string &unitName) throw();

  /**
   * Forks a label-session child-process for every tape drive entry in the
   * tape drive catalogue that is waiting for such a fork to be carried out.
   *
   * There may be more than one child-process to fork because there may be
   * more than one tape drive connected to the tape server and the previous
   * call to m_reactor.handleEvents() handled requests to start a label
   * session on more than one of the connected tape drives.
   */
  void forkLabelSessions() throw();

  /**
   * Forks a label-session child-process for the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  void forkLabelSession(const std::string &unitName) throw();

  /**
   * Runs the label session.  This method is to be called within the child
   * process responsible for running the label session.
   *
   * @param unitName The unit name of the tape drive.
   */
  void runLabelSession(const std::string &unitName) throw();

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
   * The parsed lines of /etc/castor/TPCONFIG.
   */
  const utils::TpconfigLines m_tpconfigLines;

  /**
   * Factory to create proxy objects representing the vdqmd daemon.
   */
  legacymsg::VdqmProxyFactory &m_vdqmFactory;

  /**
   * Factory to create proxy objects representing the vmgrd daemon.
   */
  legacymsg::VmgrProxyFactory &m_vmgrFactory;

  /**
   * Factory to create proxy objects representing the rmcd daemon.
   */
  legacymsg::RmcProxyFactory &m_rmcFactory;
  
  /**
   * The proxy object representing the tapeserver daemon.
   */
  legacymsg::TapeserverProxyFactory &m_tapeserverFactory;

  /**
   * The reactor responsible for dispatching the file-descriptor event-handlers
   * of the tape server daemon.
   */
  io::PollReactor &m_reactor;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;

}; // class TapeDaemon

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

