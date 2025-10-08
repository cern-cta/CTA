/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "StatisticsSaveCmdLineArgs.hpp"

namespace cta::statistics {

/**
 * Command-line tool for verifying the catalogue schema
 */
class StatisticsSaveCmd : public catalogue::CmdLineTool {
  using catalogue::CmdLineTool::CmdLineTool;

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
   * Check the content of the Catalogue database regarding what is needed to compute the statistics
   * @param catalogueDatabaseConn the connection to the Catalogue database
   * @param dbType the dbType of the Catalogue database
   */
  void checkCatalogueSchema(cta::rdbms::Conn* catalogueDatabaseConn, cta::rdbms::Login::DbType dbType) const;
};

} // namespace cta::statistics
