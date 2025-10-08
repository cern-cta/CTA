/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CmdLineTool.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

#include <cstring>
#include <unistd.h>

namespace cta::catalogue {

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
// default destructor for abstract base class
//------------------------------------------------------------------------------
CmdLineTool::~CmdLineTool() = default;

//------------------------------------------------------------------------------
// getUsername
//------------------------------------------------------------------------------
std::string CmdLineTool::getUsername() {
  char buf[256];

  if (getlogin_r(buf, sizeof(buf)) != 0) {
    return "UNKNOWN";
  }
  return std::string(buf);
}

//------------------------------------------------------------------------------
// getHostname
//------------------------------------------------------------------------------
std::string CmdLineTool::getHostname() {
  char buf[256];

  if (gethostname(buf, sizeof(buf)) != 0) {
    return "UNKNOWN";
  }
  return std::string(buf);
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

} // namespace cta::catalogue
