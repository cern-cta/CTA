/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>

namespace cta::schedulerdb {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-scheduler-schema-create.
 */
struct CreateSchemaCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help = false;

  /**
   * Path to the file containing the connection details of the scheduler
   * database.
   */
  std::string dbConfigPath;

    /**
   * Version of the schedulerdb to be created
   */
  std::optional<std::string> schedulerdbVersion;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  CreateSchemaCmdLineArgs(const int argc, char *const *const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);
};

} // namespace cta::schedulerdb
