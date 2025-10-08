/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <istream>
#include <ostream>

namespace cta::cliTool {

/**
 * Abstract class implementing common code and data structures for a
 * command-line tool.
 */
class CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  CmdLineTool(std::istream &inStream, std::ostream &outStream, std::ostream &errStream) noexcept;

  /**
   * Pure-virtual destructor to guarantee this class is abstract.
   */
  virtual ~CmdLineTool() noexcept = 0;

  /**
   * The object's implementation of main() that should be called from the main()
   * of the program.
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int main(const int argc, char *const *const argv);

protected:

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  virtual int exceptionThrowingMain(const int argc, char *const *const argv) = 0;

  /**
   * Standard input stream.
   */
  std::istream &m_in;

  /**
   * Standard output stream.
   */
  std::ostream &m_out;

  /**
   * Standard error stream.
   */
  std::ostream &m_err;

  /**
   * Returns the name of the user running the command-line tool.
   *
   * @return The name of the user running the command-line tool.
   */
  static std::string getUsername();

  /**
   * Returns the name of the host on which the command-line tool is running.
   *
   * @return The name of the host on which the command-line tool is running.
   */
  static std::string getHostname();

}; // class CmdLineTool

} // namespace cta::cliTool
