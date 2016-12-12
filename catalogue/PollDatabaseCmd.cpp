/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/PollDatabaseCmd.hpp"
#include "catalogue/PollDatabaseCmdLineArgs.hpp"
#include "rdbms/ConnFactoryFactory.hpp"
#include "rdbms/ConnPool.hpp"

#include <unistd.h>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PollDatabaseCmd::PollDatabaseCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
PollDatabaseCmd::~PollDatabaseCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int PollDatabaseCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const PollDatabaseCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    PollDatabaseCmdLineArgs::printUsage(m_out);
    return 0;
  }

  const auto dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  auto factory = rdbms::ConnFactoryFactory::create(dbLogin); 
  const uint64_t nbConns = 1;
  rdbms::ConnPool connPool(*factory, nbConns);

  uint32_t elapsedSeconds = 0;
  for(uint32_t i = 0; i < cmdLineArgs.numberOfSecondsToKeepPolling; i++) {
    try {
      m_out << "Querying the database" << std::endl;
      auto conn = connPool.getConn();
      conn->getTableNames();
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

} // namespace catalogue
} // namespace cta
