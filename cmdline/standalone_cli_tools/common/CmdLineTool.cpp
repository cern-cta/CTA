/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "cmdline/standalone_cli_tools/common/CmdLineTool.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/exception/UserError.hpp"

#include <iostream>
#include <string>
#include <unistd.h>

namespace cta::cliTool {

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
CmdLineTool::~CmdLineTool() noexcept = default;

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
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  } catch(exception::CommandLineNotParsed &ue) {
    errorMessage = ue.getMessage().str();
  } catch(exception::UserError &ue) {
    errorMessage = ue.getMessage().str();
  } catch(std::runtime_error &ex) {
    errorMessage = ex.what();
  } catch(exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  std::cerr << errorMessage << std::endl;
  return 1;
}

} // namespace cta::cliTool
