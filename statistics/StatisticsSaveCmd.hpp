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

#pragma once

#include "catalogue/CmdLineTool.hpp"
#include "catalogue/CatalogueSchema.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "StatisticsSaveCmdLineArgs.hpp"
#include "StatisticsSchema.hpp"

namespace cta {
namespace statistics {

/**
 * Command-line tool for verifying the catalogue schema.
 */
class StatisticsSaveCmd: public cta::catalogue::CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  StatisticsSaveCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  /**
   * Destructor.
   */
  ~StatisticsSaveCmd() noexcept;

private:

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  void printUsage(std::ostream &os) override;
  
  /**
   * Verifies the user input
   * throws an exception if the command line args
   * are not correct
   * @param cmdLineArgs the command line arguments provided by the user
   */
  void verifyCmdLineArgs(const StatisticsSaveCmdLineArgs& cmdLineArgs) const;
  
  /**
   * Create the tables needed to fill the CTA statistics database
   * @param statisticsDatabaseConn the database where the CTA statistics will be stored
   * @param statisticsSchema the schema that will be used to insert the CTA statistics tables
   */
  void buildStatisticsDatabase(cta::rdbms::Conn &statisticsDatabaseConn,const StatisticsSchema & statisticsSchema);
  
  /**
   * Asks the user if he really wants to drop the CTA statistics tables from the database 
   * @param dbLogin the CTA statistics database
   * @return true if the user confirmed, false otherwise
   */
  bool userConfirmDropStatisticsSchemaFromDb(const rdbms::Login &dbLogin);
  
  /**
   * Drop the tables associated to the CTA statistics
   * @param statisticsDatabaseConn the database where the CTA statistics tables will be deleted
   */
  void dropStatisticsDatabase(cta::rdbms::Conn &statisticsDatabaseConn, const StatisticsSchema & statisticsSchema);
  
  /**
   * Check the content of the Catalogue database regarding what is needed to compute the statistics
   * @param catalogueDatabaseConn the connection to the Catalogue database
   * @param dbType the dbType of the Catalogue database
   */
  void checkCatalogueSchema(cta::rdbms::Conn &catalogueDatabaseConn, cta::rdbms::Login::DbType dbType);
  
  /**
   * Checks that the tables needed for statistics are present in the statistics database
   * @param statisticsDatabaseConn the connection to the statistics database
   * @param dbType the dbType of the statistics database
   */
  void checkStatisticsSchema(cta::rdbms::Conn &statisticsDatabaseConn,cta::rdbms::Login::DbType dbType);
  
}; // class VerifySchemaCmd

} // namespace statistics
} // namespace cta
