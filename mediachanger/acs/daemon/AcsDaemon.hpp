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
#include "common/threading/Daemon.hpp"
#include "mediachanger/reactor/ZMQReactor.hpp"
#include "AcsdConfiguration.hpp"
#include "AcsPendingRequests.hpp"
#include "common/Constants.hpp"
#include "common/log/SyslogLogger.hpp"
#include "AcsdCmdLine.hpp"
#include "common/processCap/ProcessCap.hpp"
#include <signal.h>

#include <getopt.h>

namespace cta {   
namespace mediachanger {   
namespace acs {  
namespace daemon {

/**
 * CTA ACS daemon responsible for mounting and dismounting tapes for ACS.
 */
class AcsDaemon : public server::Daemon {

public:
  /**
   * Constructor.
   *
   * @param argc The argc of main().
   * @param argv The argv of main().
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param reactor The reactor responsible for dispatching the I/O requests to
   * the CTA ACS daemon.
   * @param config The CTA configuration parameters used by the CTA ACS
   * daemon.
   */
  AcsDaemon(
    const int argc,
    char **const argv,
    log::Logger& log,
    cta::mediachanger::reactor::ZMQReactor &reactor,
    const AcsdConfiguration &config);

  /**
   * Destructor.
   */
  ~AcsDaemon();

  /**
   * The main entry function of the daemon.
   *
   * @return The return code of the process.
   */
  int main();
  
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
  void logStartOfDaemon(const int argc, const char *const *const argv);

  /**
   * Creates a string that contains the specified command-line arguments
   * separated by single spaces.
   *
   * @param argc The number of command-line arguments.
   * @param argv The array of command-line arguments.
   */
  std::string argvToString(const int argc, const char *const *const argv)
   ;

  /**
   * Idempotent method that destroys the ZMQ context.
   */
  void destroyZmqContext();

  /**
   * Sets the dumpable attribute of the current process to true.
   */
  void setDumpable();

  /**
   * Blocks the signals that should not asynchronously disturb the daemon.
   */
  void blockSignals() const;
  
  /**
   * Initialises the ZMQ context.
   */
  void initZmqContext();
  /**
   * Sets up the reactor.
   */
  void setUpReactor();
 
  /**
   * Creates the handler to handle messages for the acs Zmq requests.
   */
  void createAndRegisterAcsMessageHandler();
  
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
  bool handlePendingSignals();
  
  /**
   * Handles any pending Acs requests.
   *
   */
  void handlePendingRequests();

  /**
   * The argc of main().
   */
  const int m_argc;

  /**
   * The argv of main().
   */
  char **const m_argv;

  log::Logger &m_log;
  /**
   * The reactor responsible for dispatching the file-descriptor event-handlers
   * of the CTA ACS daemon.
   */
  cta::mediachanger::reactor::ZMQReactor &m_reactor;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName;

  /**
   * The name of the host on which the daemon is running. 
   */
  const std::string m_hostName;

  /**
   * The ZMQ context.
   */
  void *m_zmqContext;
  
  /**
   * The CTA configuration parameters used by the CTA ACS daemon.
   */
  const AcsdConfiguration m_config;
  
  /**
   * The object to handle requests to the CTA ACS daemon.
   */
  AcsPendingRequests m_acsPendingRequests;

private:
 
  /**
   * Flag indicating whether the server should run in foreground or background
   * mode.
   */
  bool m_foreground;

}; // class AcsDaemon

} // namespace daemon
} // namespace acs
} // namespace mediachanger
} // namespace cta
