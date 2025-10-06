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


#include <getopt.h>
#include <string>
#include <iostream>

#include "common/exception/Errnum.hpp"
#include "common/CmdLineParams.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/process/threading/System.hpp"
#include "common/utils/utils.hpp"
#include "Maintenance.hpp"
#include "rdbms/Login.hpp"

#include "version.h"

namespace cta::maintenance {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//
// @param argc The number of command-line arguments.
// @param argv The command-line arguments.
// @param log The logging system.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const common::Config config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);

  {
    std::list<cta::log::Param> params = {cta::log::Param("version", CTA_VERSION)};
    log(cta::log::INFO, "Starting cta-maintenance", params);
  }

  // Set specific static headers for tape daemon
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["instance"] = config.getOptionValueStr("cta.instance_name").value();
  staticParamMap["sched_backend"] = config.getOptionValueStr("cta.scheduler_backend_name").value();
  log.setStaticParams(staticParamMap);

  // Adjust log mask to the log level potentionally set in the configuration file
  log.setLogMask(config.getOptionValueStr("cta.log.level").value());
  {
    std::list<cta::log::Param> params = {cta::log::Param("logMask", config.getOptionValueStr("cta.log.level").value())};
    log(log::INFO, "Set log mask", params);
  }

  // Create the maintenance object
  cta::maintenance::Maintenance daemon(lc, config);

  // Start loop
  daemon.run();

  return 0;
}

} // namespace cta::maintenance

int main(const int argc, char **const argv) {
  using namespace cta;

  // Interpret the command line
  std::unique_ptr<common::CmdLineParams> cmdLineParams;
  cmdLineParams.reset(new common::CmdLineParams(argc, argv, "cta-maintenance"));

  std::string shortHostName;
  try {
    shortHostName = utils::getShortHostname();
  } catch (const exception::Errnum &ex) {
    std::cerr << "Failed to get short host name." << ex.getMessage().str();
    return EXIT_FAILURE;
  }

  std::unique_ptr<log::Logger> logPtr;
  logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));


  const common::Config config(cmdLineParams->configFileLocation);

  // Change user and group
  const std::string userName = config.getOptionValueStr("cta.daemon_user").value();
  const std::string groupName = config.getOptionValueStr("cta.daemon_group").value();

  try {
    (*logPtr)(log::INFO, "Setting user name and group name of current process",
                  {{"userName", userName}, {"groupName", groupName}});
    cta::System::setUserAndGroup(userName, groupName);
  } catch (exception::Exception& ex) {
    std::list<log::Param> params = {
      log::Param("exceptionMessage", ex.getMessage().str())};
    (*logPtr)(log::ERR, "Caught an unexpected CTA, cta-taped cannot start", params);
    return EXIT_FAILURE;
  }

  // Try to instantiate the logging system API
  try {
    if(cmdLineParams->logToFile) {
      logPtr.reset(new log::FileLogger(shortHostName, "cta-maintenance", cmdLineParams->logFilePath, log::DEBUG));
    } else if(cmdLineParams->logToStdout) {
      logPtr.reset(new log::StdoutLogger(shortHostName, "cta-maintenance"));
    }
    if (!cmdLineParams->logFormat.empty()) {
      logPtr->setLogFormat(cmdLineParams->logFormat);
    }
  } catch (exception::Exception& ex) {
    std::cerr << "Failed to instantiate object representing CTA logging system: " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }

  log::Logger& log = *logPtr;

  log(log::INFO, "Launching cta-maintenance", cmdLineParams->toLogParams());

  int programRc = EXIT_FAILURE;
  try {
    programRc = maintenance::exceptionThrowingMain(config, log);
  }  catch(exception::Exception &ex) {
    std::list<cta::log::Param> params = {
      cta::log::Param("exceptionMessage", ex.getMessage().str())};
    log(log::ERR, "Caught an unexpected CTA exception, cta-maintenance cannot start", params);
    sleep(1);
  } catch(std::exception &se) {
    std::list<cta::log::Param> params = {cta::log::Param("what", se.what())};
    log(log::ERR, "Caught an unexpected standard exception, cta-maintenance cannot start", params);
    sleep(1);
  } catch(...) {
    log(log::ERR, "Caught an unexpected and unknown exception, cta-maintenance cannot start");
    sleep(1);
  }

  return programRc;
}

