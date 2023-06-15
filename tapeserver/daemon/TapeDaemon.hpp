/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/threading/Daemon.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "common/processCap/ProcessCap.hpp"
#include <signal.h>

namespace cta {
namespace tape {
namespace daemon {

/** Daemon responsible for reading and writing data from and to one or more tape
 * drives drives connected to a tape server. */

class TapeDaemon : public cta::server::Daemon {
public:
  /** Constructor.
   * @param commandLine The parameters extracted from the command line.
   * @param log The object representing the API of the CTA logging system.
   * @param globalConfig The configuration of the tape server.
   * @param capUtils Object providing utilities for working UNIX capabilities. */
  TapeDaemon(const cta::daemon::CommandLineParams& commandLine,
             cta::log::Logger& log,
             const TapedConfiguration& globalConfig,
             cta::server::ProcessCap& capUtils);

  virtual ~TapeDaemon();

  /** The main entry function of the daemon.
   * @return The return code of the process. */
  int main();

private:
  bool isMaintenanceProcessDisabled() const;

protected:
  /** Enumeration of the possible tape-daemon states. */
  enum State { TAPEDAEMON_STATE_RUNNING, TAPEDAEMON_STATE_SHUTTINGDOWN };

  /** Return the string representation of the specified tape-daemon state.
   * @param The tape-daemon state.
   * @return The string representation. */
  static const char* stateToStr(const State state) throw();

  /** The current state of the tape-server daemon. */
  State m_state;

  /** The absolute time at which the shutdown sequence was started. */
  time_t m_startOfShutdown;

  /** Returns the name of the host on which the daemon is running. */
  std::string getHostName() const;

  /** Exception throwing main() function. */
  void exceptionThrowingMain();

  /** Sets the dumpable attribute of the current process to true. */
  void setDumpable();

  /** Sets the capabilities of the current process.
   *
   * @text The string representation the capabilities that the current
   * process should have. */
  void setProcessCapabilities(const std::string& text);

  /** Socket pair used to send commands to the DriveProcess. */
  struct DriveSocketPair {
    /** Bi-directional socket used by the TapeDaemon parent process to send
     * commands to the process forker and receive replies in return. */
    int tapeDaemon;

    /** Bi-directional socket used by the ProcessForker to receive commands
     * from the TapeDaemon parent process and send back replies.  */
    int driveProcess;

    /** Constructor.
     * Sets members to -1 which represents an invalid file descriptor. */
    DriveSocketPair() : tapeDaemon(-1), driveProcess(-1) {}

    /** Close utility. Closes both sockets */
    void closeBoth();
    /** Close utility. Closes drive's socket */
    void closeDriveEnd();
    /** Close utility. Closes daemon's socket */
    void closeDaemonEnd();
  };  // struct DriveSocketPair

  /** Creates the socket pair to be used to control the ProcessForker.
   * @return The socket pair. */
  DriveSocketPair createDriveSocketPair();

  /**
   * Forks a drive process.
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
   * by the ProcessForker.
   * @return The process identifier of the ProcessForker.
   */
  pid_t forkDriveProcess(const DriveSocketPair& drivePair);

  /** Runs the driveProcess after fork
   *
   * @param heartbeatSocket The socket used to send heartbeat to.
   * @return the exit code to be used for the process running the DriveProcess. */
  int runDriveProcess(const int heartbeatSocket);

  /** Blocks the signals that should not asynchronously disturb the daemon. */
  void blockSignals() const;

  /**
   * Creates the handler to handle the incoming connection from the
   * ProcessForker.
   *
   * @param reaperSocket The TapeDaemon side of the socket pair used by the
   * ProcessForker  to report the termination of its child processes.
   */
  void createAndRegisterProcessForkerConnectionHandler(const int reaperSocket);

  /**
   * Creates the handler to handle messages from forked sessions.
   */
  void createAndRegisterTapeMessageHandler();

  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop();

  /**
   * Handles any pending IO events.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleIOEvents() throw();

  /**
   * Handles a tick in time.  Time driven actions such as alarms should be
   * implemented here.
   *
   * This method does not have to be called at any time precise interval,
   * though it should be called at least twice as fast as the quickest reaction
   * time imposed on the catalogue.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleTick() throw();

  /**
   * Handles any pending signals.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handlePendingSignals() throw();

  /**
   * Handles the specified signals.
   *
   * @param sig The number of the signal.
   * @param sigInfo Information about the signal.
   * @return True if the main event loop should continue, else false.
   */
  bool handleSignal(const int sig, const siginfo_t& sigInfo);

  /**
   * Handles a SIGINT signal.
   *
   * @param sigInfo Information about the signal.
   * @return True if the main event loop should continue, else false.
   */
  bool handleSIGINT(const siginfo_t& sigInfo);

  /**
   * Handles a SIGTERM signal.
   *
   * @param sigInfo Information about the signal.
   * @return True if the main event loop should continue, else false.
   */
  bool handleSIGTERM(const siginfo_t& sigInfo);

  /**
   * Handles a SIGCHLD signal.
   *
   * @param sigInfo Information about the signal.
   * @return True if the main event loop should continue, else false.
   */
  bool handleSIGCHLD(const siginfo_t& sigInfo);

  /**
   * Logs the fact that the specified child process has terminated.
   *
   * @param pid The process ID of the child process.
   * @param waitpidStat The status information given by a call to waitpid().
   */
  void logChildProcessTerminated(const pid_t pid, const int waitpidStat) throw();

  /** The tape server's configuration */
  const TapedConfiguration& m_globalConfiguration;

  /**
   * Object providing utilities for working UNIX capabilities.
   */
  cta::server::ProcessCap& m_capUtils;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;

};  // class TapeDaemon

}  // namespace daemon
}  // namespace tape
}  // namespace cta
