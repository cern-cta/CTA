/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <istream>
#include <ostream>

namespace cta::frontend::grpc::server {

/**
 * Class to store the command-line arguments of the command-line tool
 * named cta-frontend-async-grpc.
 */
class FrontendCmd {
public:
  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  FrontendCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream) noexcept;
  ~FrontendCmd() noexcept = default;
  /**
   * The object's implementation of main() that should be called from the main()
   * of the program.
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int main(const int argc, char** argv);

private:
  std::istream &m_in; // Standard input stream
  std::ostream &m_out; // Standard output stream
  std::ostream &m_err; // Standard error stream

  std::string m_strExecName; // Executable name of this program

  // Print usage help
  void printUsage(std::ostream &osHelp) const;

};

} // namespace cta::frontend::grpc::server
