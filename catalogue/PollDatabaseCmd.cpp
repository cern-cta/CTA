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
