/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "SignalHandlerThread.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"

#include <memory>
#include <ostream>


// Global signal flag state to know which actions do we have to take.
// The signal handler thread takes care of setting them to 1 without
// causnig an interruption of the process. The daemon is responsible
// of taking action on the different received signals. Write something for the opposite case.
volatile sig_atomic_t g_sigTERMINT = 0; // Either SIGTERM or SIGINT. Stop the daemon.
volatile sig_atomic_t g_sigHUP = 0;  // Update configuration file. This requires falling back to init stage.
volatile sig_atomic_t g_sigUSR1 = 0; // Log rotate.


namespace cta::server {

/**
 * This class contains the code common to all daemon classes.
 *
 * The code common to all daemon classes includes daemonization and logging.
 */
class Daemon {
public:
  /**
   * Constructor
   *
   * @param log Object representing the API of the logging system
   */
  explicit Daemon(cta::log::Logger& log) noexcept;

  /**
   *
   */
  void runDaemon() noexcept;

  /**
   *
   */


  /**
   * Destructor
   */
  virtual ~Daemon() = default;

private:
/**
   * Signal Handler Thread
   */
  std::unique_ptr<SignalHandlerThread> m_signalHandlerThread;

  /**
   * Object representing the API of the CTA logging system.
   */
  cta::log::Logger &m_log;

  // Daemon status codes as specified in
  // https://refspecs.linuxbase.org/LSB_3.1.1/LSB-Core-generic/LSB-Core-generic/iniscrptact.html
  enum DaemonStatus : uint8_t {
    Running = 0, // Program is running or service is OK
    Dead = 1,
    DeadAndLockFile = 2,
    NotRunning = 3, // Program is not running
    Uknown = 4, //Unknow Status
    //   5 -  99 should be reserved for future LSB usage
    // 100 - 149 reserved for distribution use
    // 150 - 199 <- Values we can use
    Init = 150
    // 200 - 254 Reserved
  };

  // Base set of error codes the Daemon can emit.
  enum BaseErrorCodes {
    ConfigError = 0,     // Syntax error, missing something
    ConnectionError = 1, // Cannot contact some of the required endpoints.

  };

  // When we start the daemon we will always be in the init stage.
  int m_daemonStatus;
  int m_daemonErrorCode;

  // Notify SystemD upon completion of taks.
  // Blablabla
  DaemonManagerNotifier m_serviceManager;

  // Utility function to convert an error code to human readable output.
  virtual std::string errCodeToString(const ErrorCode& errCode) const noexcept;

  /**
   * The init stage of every daemon must:
   *   1. Set proper signal handling
   *   2. Set logging system
   *   3.
   */
  virtual int initStage();

  int setLoggingSystem();

  /**
   * Run Stage
   */
  virtual int runStage();

}; // class Daemon

} // namespace cta::server
