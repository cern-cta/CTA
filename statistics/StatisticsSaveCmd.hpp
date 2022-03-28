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

#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "StatisticsSaveCmdLineArgs.hpp"

namespace cta {
namespace statistics {

/**
 * Command-line tool for verifying the catalogue schema.
 */
class StatisticsSaveCmd: public cta::catalogue::CmdLineTool {
 public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  StatisticsSaveCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  /**
   * Destructor.
   */
  ~StatisticsSaveCmd() noexcept;

 private:
  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  void printUsage(std::ostream &os) override;

  /**
   * Verifies the user input
   * throws an exception if the command line args
   * are not correct
   * @param cmdLineArgs the command line arguments provided by the user
   */
  void verifyCmdLineArgs(const StatisticsSaveCmdLineArgs& cmdLineArgs) const;

  /**
   * Check the content of the Catalogue database regarding what is needed to compute the statistics
   * @param catalogueDatabaseConn the connection to the Catalogue database
   * @param dbType the dbType of the Catalogue database
   */
  void checkCatalogueSchema(cta::rdbms::Conn* catalogueDatabaseConn, cta::rdbms::Login::DbType dbType);
};  // class VerifySchemaCmd

}  // namespace statistics
}  // namespace cta
