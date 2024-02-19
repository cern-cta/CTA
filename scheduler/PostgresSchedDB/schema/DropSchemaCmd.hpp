/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#pragma once

#include "scheduler/PostgresSchedDB/schema/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

namespace cta::postgresscheddb {

/**
 * Command-line tool that drops the schema of the scheduler database.
 */
class DropSchemaCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  DropSchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  /**
   * Destructor.
   */
  ~DropSchemaCmd() override;

  /**
   * Checks if the IS_PRODUCTION bit is set on the CTA_SCHEDULER table
   * @return true if the IS_PRODUCTION bit is set, false otherwise
   */
  static bool isProductionSet(rdbms::Conn & conn);

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
   * Asks the user to confirm that they want to drop the schema of the scheduler
   * database.
   *
   * @param dbLogin The database login.
   * @return True if the user confirmed.
   */
  bool userConfirmsDropOfSchema(const rdbms::Login &dbLogin);

  /**
   * Unconditionally drops the schema of the scheduler database associated with
   * the specified database connection.
   *
   * @param dbType The database type.
   * @param conn The database connection.
   */
  void dropSchedulerSchema(const rdbms::Login::DbType &dbType, rdbms::Conn &conn);

  /**
   * Drops the database tables with the specified names.
   *
   * @param conn The database connection.
   */
  void dropDatabaseTables(rdbms::Conn &conn);

  /**
   * Drops the database sequences with the specified names.
   *
   * @param conn The database connection.
   */
  void dropDatabaseSequences(rdbms::Conn &conn);

  /**
   * Checks if we can check the IS_PRODUCTION bit. This allows the backward-compatibility of
   * this tool if we use it against a schema that does not have the IS_PRODUCTION bit.
   * @param conn the connection to the Scheduler database
   * @param dbType the type of the Scheduler database
   * @return true if the production bit is set, false otherwise
   */
  bool isProductionProtectionCheckable(rdbms::Conn & conn, const cta::rdbms::Login::DbType dbType);

}; // class DropSchemaCmd

} // namespace cta::postgresscheddb
