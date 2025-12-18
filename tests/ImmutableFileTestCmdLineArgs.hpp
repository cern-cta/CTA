/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <XrdCl/XrdClURL.hh>
#include <ostream>
#include <string>

namespace cta {

/**
 * Structure to store command-line arguments.
 */
struct ImmutableFileTestCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help = false;

  /**
   * The XRootd URL of the file to be modified.
   */
  XrdCl::URL fileUrl;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  ImmutableFileTestCmdLineArgs(const int argc, char* const* const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream& os);
};

}  // namespace cta
