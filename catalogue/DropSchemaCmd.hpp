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

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

namespace cta {
namespace catalogue {

/**
 * Command-line tool that drops the schema of the catalogue database.
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
  ~DropSchemaCmd() noexcept;

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
   * Asks the user to confirm that they want to drop the schema of the catalogue
   * database.
   *
   * @param dbLogin The database login.
   * @return True if the user confirmed.
   */
  bool userConfirmsDropOfSchema(const rdbms::Login &dbLogin);

  /**
   * Unconditionally drops the schema of the catalogue database associated with
   * the specified database connection.
   *
   * @param dbType The database type.
   * @param conn The database connection.
   */
  void dropCatalogueSchema(const rdbms::Login::DbType &dbType, rdbms::Conn &conn);

  /**
   * Unconditionally drops the schema of the catalogue database associated with
   * the specified database connection.
   *
   * @param conn The database connection.
   */
  void dropSqliteCatalogueSchema(rdbms::Conn &conn);

  /**
   * Unconditionally drops the schema of the catalogue database associated with
   * the specified database connection.
   *
   * @param conn The database connection.
   */
  void dropMysqlCatalogueSchema(rdbms::Conn &conn);

  /**
   * Unconditionally drops the schema of the catalogue database associated with
   * the specified database connection.
   *
   * @param conn The database connection.
   */
  void dropPostgresCatalogueSchema(rdbms::Conn &conn);

  /**
   * Drops the database tables with the specified names.
   *
   * @param tablesToDrop The names of the database tables to be dropped.
   */
  void dropDatabaseTables(rdbms::Conn &conn, const std::list<std::string> &tablesToDrop);

  /**
   * Unconditionally drops the schema of the catalogue database associated with
   * the specified database connection.
   *
   * @param conn The database connection.
   */
  void dropOracleCatalogueSchema(rdbms::Conn &conn);

  /**
   * Drops the database sequences with the specified names.
   *
   * @param conn The database connection.
   * @param seqeuncesToDrop The names of the database sequences to be dropped.
   */
  void dropDatabaseSequences(rdbms::Conn &conn, const std::list<std::string> &sequencesToDrop);

}; // class DropSchemaCmd

} // namespace catalogue
} // namespace cta
