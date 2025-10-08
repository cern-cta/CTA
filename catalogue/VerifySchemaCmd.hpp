/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/CmdLineTool.hpp"
#include "catalogue/CatalogueSchema.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

/**
 * Command-line tool for verifying the catalogue schema.
 */
class VerifySchemaCmd: public CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  VerifySchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  enum class VerifyStatus { OK, INFO, ERROR, UNKNOWN };

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
   * schema of the specified database connection.
   *
   * @param tableName The name of the database table.
   * @param conn The database connection.
   * @return True if the table exists.
   */
  bool tableExists(const std::string& tableName, const rdbms::Conn& conn) const;

  /*
   * Returns true if the catalogue is upgrading
   *
   * @param conn The database connection
   * @return True if the catalogue is upgrading
   */
  bool isUpgrading(rdbms::Conn *conn) const;
};

} // namespace cta::catalogue
