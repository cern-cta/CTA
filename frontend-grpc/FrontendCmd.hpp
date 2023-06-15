/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

#include <string>
#include <istream>
#include <ostream>

namespace cta {
namespace frontend {
namespace grpc {
namespace server {

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
  FrontendCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) noexcept;
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
  std::istream& m_in;   // Standard input stream
  std::ostream& m_out;  // Standard output stream
  std::ostream& m_err;  // Standard error stream

  std::string m_strExecName;  // Executable name of this program

  // Print usage help
  void printUsage(std::ostream& osHelp) const;
};

}  // namespace server
}  // namespace grpc
}  // namespace frontend
}  // namespace cta
