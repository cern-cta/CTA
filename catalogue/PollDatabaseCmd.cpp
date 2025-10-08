/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <unistd.h>

#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/PollDatabaseCmd.hpp"
#include "catalogue/PollDatabaseCmdLineArgs.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PollDatabaseCmd::PollDatabaseCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) :
  CmdLineTool(inStream, outStream, errStream) { }

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int PollDatabaseCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const PollDatabaseCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const auto dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t nbConns = 2;
  rdbms::ConnPool connPool(dbLogin, nbConns);

  uint32_t elapsedSeconds = 0;
  for(uint32_t i = 0; i < cmdLineArgs.numberOfSecondsToKeepPolling; i++) {
    try {
      m_out << "Querying the database" << std::endl;
      auto conn = connPool.getConn();
      conn.getTableNames();
    } catch(exception::Exception &ex) {
      m_out << "Database error: " << ex.getMessage().str() << std::endl;
    }

    m_out << "Sleeping for 1 second" << std::endl;
    elapsedSeconds++;
    m_out << "Elapsed seconds = " << elapsedSeconds << std::endl;
    sleep(1);
  }

  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void PollDatabaseCmd::printUsage(std::ostream &os) {
  PollDatabaseCmdLineArgs::printUsage(os);
}

} // namespace cta::catalogue
