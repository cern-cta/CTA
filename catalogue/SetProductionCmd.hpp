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

#include "CmdLineTool.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

class SetProductionCmd: public CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  SetProductionCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

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
   * Returns true if the IS_PRODUCTION flag is settable, false otherwise
   * @param login the database login informations
   * @param conn the connection to the database
   * @return true if the IS_PRODUCTION flag is settable, false otherwise
   */
  bool isProductionSettable(const cta::rdbms::Login & login, cta::rdbms::Conn & conn) const;
  
  /**
   * Set the IS_PRODUCTION flag to true on the CTA Catalogue
   * @param conn the connection to the CTA Catalogue database
   */
  void setProductionFlag(cta::rdbms::Conn & conn) const;
};

} // namespace cta::catalogue
