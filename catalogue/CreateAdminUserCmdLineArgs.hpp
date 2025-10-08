/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::catalogue {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-catalogue-admin-user-create
 */
struct CreateAdminUserCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help = false;

  /**
   * Path to the file containing the connection details of the catalogue
   * database.
   */
  std::string dbConfigPath;

  /**
   * The name of the admin user to be created in the catalogue database.
   */
  std::string adminUsername;

  /**
   * The optional comment describing the creation of the admin user.
   */
  std::string comment;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  CreateAdminUserCmdLineArgs(const int argc, char *const *const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);
};

} // namespace cta::catalogue
