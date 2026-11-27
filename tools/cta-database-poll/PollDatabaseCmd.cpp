/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "PollDatabaseCmd.hpp"

#include "PollDatabaseCmdLineArgs.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <unistd.h>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PollDatabaseCmd::PollDatabaseCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream)
    : CmdLineTool(inStream, outStream, errStream) {}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int PollDatabaseCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const PollDatabaseCmdLineArgs cmdLineArgs(argc, argv);

  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);

  const uint64_t nbConns = 2;
  rdbms::ConnPool connPool(dbLogin, nbConns);

  bool hadFailure = false;
  for (uint64_t i = 0; i < cmdLineArgs.numberOfSecondsToKeepPolling; i++) {
    try {
      m_out << "Querying the database" << std::endl;
      auto conn = connPool.getConn();
      conn.getTableNames();
      if (cmdLineArgs.untilReady) {
        m_out << "Database connection ready" << std::endl;
        return 0;
      }
    } catch (exception::Exception& ex) {
      hadFailure = true;
      m_out << "Database error: " << ex.getMessage().str() << std::endl;
    }

    m_out << "Sleeping for 1 second" << std::endl;
    m_out << "Elapsed seconds = " << i + 1 << std::endl;
    sleep(1);
  }

  return (hadFailure || cmdLineArgs.untilReady) ? 1 : 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void PollDatabaseCmd::printUsage(std::ostream& os) {
  PollDatabaseCmdLineArgs::printUsage(os);
}

}  // namespace cta::catalogue
