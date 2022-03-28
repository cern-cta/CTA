/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <istream>
#include <ostream>

namespace cta {
namespace catalogue {

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
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  virtual void printUsage(std::ostream &os) = 0;

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

} // namespace catalogue
} // namespace cta
