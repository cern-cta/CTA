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

#include "common/exception/Exception.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/DismountCmd.hpp"
#include "mediachanger/DismountCmdLine.hpp"

#include <exception>
#include <iostream>

/**
 * An exception throwing version of main().
 *
 * @param argc The number of command-line arguments including the program name.
 * @param argv The command-line arguments.
 * @return The exit value of the program.
 */
static int exceptionThrowingMain(const int argc, char *const *const argv);

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  using namespace cta;

  std::string errorMessage;

  try {
    const int rc = exceptionThrowingMain(argc, argv);
    return rc;
  } catch(cta::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, ane exception was throw
  // and errorMessage has been set accordingly

  std::cerr << "Aborting: " << errorMessage << std::endl;
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const int argc, char *const *const argv) {
  using namespace cta;

  const std::string hostName = utils::getShortHostname();
  const std::string programName = mediachanger::DismountCmdLine::getProgramName();
  log::StdoutLogger log(hostName, programName);
  mediachanger::MediaChangerFacade mc(log);
  
  mediachanger::DismountCmd cmd(std::cin, std::cout, std::cerr, mc);

  return cmd.exceptionThrowingMain(argc, argv);
}
