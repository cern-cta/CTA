/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string>

/**
 * Structure to store the command-line arguments of cta-rdbmsUnitTests.
 */
struct RdbmsUnitTestsCmdLineArgs {
  /**
   * Absolute path to the file containing the database connection details.
   */
  std::string dbConfigPath;
};

/**
 * Declaration of the global variable used to store the command-line arguments
 * so that they are visible to the tests.
 */
extern RdbmsUnitTestsCmdLineArgs g_cmdLineArgs;
