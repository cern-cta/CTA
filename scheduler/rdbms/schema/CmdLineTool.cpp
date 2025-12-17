/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/CmdLineTool.hpp"

#include "common/exception/CommandLineNotParsed.hpp"

#include <array>
#include <unistd.h>

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CmdLineTool::CmdLineTool(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) noexcept
    : m_in(inStream),
      m_out(outStream),
      m_err(errStream) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CmdLineTool::~CmdLineTool() = default;

//------------------------------------------------------------------------------
// getUsername
//------------------------------------------------------------------------------
std::string CmdLineTool::getUsername() {
  std::array<char, 256> buf;
  buf.fill('\0');

  if (getlogin_r(buf.data(), buf.size()) == 0) {
    buf[buf.size() - 1] = '\0';
    return std::string(buf.data());
  } else {
    return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// getHostname
//------------------------------------------------------------------------------
std::string CmdLineTool::getHostname() {
  std::array<char, 256> buf;
  buf.fill('\0');

  if (gethostname(buf.data(), buf.size()) == 0) {
    buf[buf.size() - 1] = '\0';
    return std::string(buf.data());
  } else {
    return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CmdLineTool::cltMain(const int argc, char* const* const argv) {
  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  } catch (exception::CommandLineNotParsed& ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  } catch (exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  } catch (std::exception& se) {
    errorMessage = se.what();
  } catch (...) {
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

}  // namespace cta::schedulerdb
