/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/CmdLineTool.hpp"

namespace cta::catalogue {

/**
 * Command-line tool that creates an admin user
 */
class CreateAdminUserCmd : public CmdLineTool {
public:
  /**
   * Constructor
   *
   * @param inStream Standard input stream
   * @param outStream Standard output stream
   * @param errStream Standard error stream
   */
  CreateAdminUserCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream);

private:
  /**
   * An exception throwing version of main()
   *
   * @param argc The number of command-line arguments including the program name
   * @param argv The command-line arguments
   * @return The exit value of the program
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;

  /**
   * Prints the usage message of the command-line tool
   *
   * @param os The output stream to which the usage message is to be printed
   */
  void printUsage(std::ostream& os) override;
};

} // namespace cta::catalogue
