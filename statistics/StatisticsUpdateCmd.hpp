/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/CatalogueSchema.hpp"
#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "StatisticsUpdateCmdLineArgs.hpp"

namespace cta::statistics {

/**
 * Command-line tool for verifying the catalogue schema
 */
class StatisticsUpdateCmd : public catalogue::CmdLineTool {
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
};

} // namespace cta::statistics
