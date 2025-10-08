/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

/**
 * Command-line tool for creating the catalogue schema.
 */
class CreateSchemaCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  CreateSchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

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
   * Returns true if the table with the specified name exists in the database
   * schema of teh specified database connection.
   *
   * @param tableName The name of the database table.
   * @param conn The database connection.
   * @return True if the table exists.
   */
  bool tableExists(const std::string& tableName, const rdbms::Conn& conn) const;

  /**
   * Parses the specified string of multiple SQL statements separated by
   * semicolons and calls executeNonQuery() for each statement found.
   *
   * Please note that this method does not support statements that themselves
   * contain one more semicolons.
   *
   * @param conn The database connection.
   * @param sqlStmts Multiple SQL statements separated by semicolons.
   * Statements that themselves contain one more semicolons are not supported.
   */
  void executeNonQueries(rdbms::Conn &conn, const std::string &sqlStmts) const;

}; // class CreateSchemaCmd

} // namespace cta::catalogue
