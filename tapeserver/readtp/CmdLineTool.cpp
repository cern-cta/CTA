/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "tapeserver/readtp/CmdLineTool.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <unistd.h>

namespace cta {
namespace tapeserver {
namespace readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CmdLineTool::CmdLineTool(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream) noexcept:
  m_in(inStream),
  m_out(outStream),
  m_err(errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CmdLineTool::~CmdLineTool() noexcept {
}

//------------------------------------------------------------------------------
// getUsername
//------------------------------------------------------------------------------
std::string CmdLineTool::getUsername() {
  char buf[256];

  if(getlogin_r(buf, sizeof(buf))) {
    return "UNKNOWN";
  } else {
    return buf;
  }
}

//------------------------------------------------------------------------------
// getHostname
//------------------------------------------------------------------------------
std::string CmdLineTool::getHostname() {
  char buf[256];

  if(gethostname(buf, sizeof(buf))) {
    return "UNKNOWN";
  } else {
    buf[sizeof(buf) - 1] = '\0';
    return buf;
  }
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CmdLineTool::main(const int argc, char *const *const argv) {
  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  } catch(exception::CommandLineNotParsed &ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  } catch(exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, an exception was throw
  // and errorMessage has been set accordingly

  m_err << "Aborting: " << errorMessage << std::endl;
  if(cmdLineNotParsed) {
    m_err << std::endl;
    printUsage(m_err);
  }
  return 1;
}

} // namespace readtp
} // namespace tapeserver
} // namespace cta
