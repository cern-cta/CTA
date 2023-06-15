/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/PostgresSchedDB/schema/CmdLineTool.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <unistd.h>

namespace cta {
namespace postgresscheddb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CmdLineTool::CmdLineTool(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) noexcept :
m_in(inStream),
m_out(outStream),
m_err(errStream) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CmdLineTool::~CmdLineTool() noexcept {}

//------------------------------------------------------------------------------
// getUsername
//------------------------------------------------------------------------------
std::string CmdLineTool::getUsername() {
  char buf[256];

  if (getlogin_r(buf, sizeof(buf))) {
    return "UNKNOWN";
  }
  else {
    return buf;
  }
}

//------------------------------------------------------------------------------
// getHostname
//------------------------------------------------------------------------------
std::string CmdLineTool::getHostname() {
  char buf[256];

  if (gethostname(buf, sizeof(buf))) {
    return "UNKNOWN";
  }
  else {
    buf[sizeof(buf) - 1] = '\0';
    return buf;
  }
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CmdLineTool::main(const int argc, char* const* const argv) {
  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  }
  catch (exception::CommandLineNotParsed& ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  }
  catch (exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  }
  catch (std::exception& se) {
    errorMessage = se.what();
  }
  catch (...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, an exception was throw
  // and errorMessage has been set accordingly

  m_err << "Aborting: " << errorMessage << std::endl;
  if (cmdLineNotParsed) {
    m_err << std::endl;
    printUsage(m_err);
  }
  return 1;
}

}  // namespace postgresscheddb
}  // namespace cta
