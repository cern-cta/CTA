/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "rdbms/ConnPool.hpp"
#include "rdbms/AutocommitMode.hpp"
#include "StatisticsCmd.hpp"
#include "catalogue/SchemaChecker.hpp"


namespace cta {
namespace statistics {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
StatisticsCmd::StatisticsCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
StatisticsCmd::~StatisticsCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int StatisticsCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const StatisticsCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }
  
  std::cout << cmdLineArgs.catalogueDbConfigPath << std::endl;
  std::cout << cmdLineArgs.statisticsDbConfigPath << std::endl;

  const uint64_t maxNbConns = 1;
    
  auto loginCatalogue = rdbms::Login::parseFile(cmdLineArgs.catalogueDbConfigPath);
  rdbms::ConnPool catalogueConnPool(loginCatalogue, maxNbConns);
  auto catalogueConn = catalogueConnPool.getConn();
  
  auto loginStatistics = rdbms::Login::parseFile(cmdLineArgs.statisticsDbConfigPath);
  rdbms::ConnPool statisticsConnPool(loginStatistics, maxNbConns);
  auto statisticsConn = statisticsConnPool.getConn();

  /*cta::catalogue::SchemaChecker catalogueSchemaChecker(loginCatalogue.dbType,catalogueConn);
  cta::catalogue::SchemaChecker statisticsSchemaChecker(loginStatistics.dbType,statisticsConn);*/
  
//  catalogueSchemaChecker.useSQLiteSchemaComparer();
  
  return 0;
}
//------------------------------------------------------------------------------
// tableExists
//------------------------------------------------------------------------------
bool StatisticsCmd::tableExists(const std::string tableName, rdbms::Conn &conn) const {
  const auto names = conn.getTableNames();
  for(const auto &name : names) {
    if(tableName == name) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void StatisticsCmd::printUsage(std::ostream &os) {
  StatisticsCmdLineArgs::printUsage(os);
}


} // namespace catalogue
} // namespace cta
