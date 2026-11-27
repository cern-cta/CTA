/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-database-poll.
 */
struct PollDatabaseCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help = false;

  /// Exit on the first successful query and fail if polling finishes without success.
  bool untilReady = false;

  /**
   * Polling duration in seconds, excluding connection and query time.
   */
  uint64_t numberOfSecondsToKeepPolling = 0;

  /**
   * Path to the catalogue or PostgreSQL scheduler connection file.
   */
  std::string dbConfigPath;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  PollDatabaseCmdLineArgs(const int argc, char* const* const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream& os);
};

}  // namespace cta::catalogue
