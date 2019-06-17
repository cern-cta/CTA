/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/CmdLineTool.hpp"
#include "rdbms/Conn.hpp"

namespace cta {
namespace catalogue {

/**
 * Command-line tool for verifying the catalogue schema.
 */
class VerifySchemaCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  VerifySchemaCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  /**
   * Destructor.
   */
  ~VerifySchemaCmd() noexcept;
  
  enum class VerifyStatus { OK, INFO, ERROR, UNKNOWN };

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
   * Returns true if the table with the specified name exists in the database
   * schema of the specified database connection.
   *
   * @param tableName The name of the database table.
   * @param conn The database connection.
   * @return True if the table exists.
   */
  bool tableExists(const std::string tableName, rdbms::Conn &conn) const;
  
  /**
   * Verifies schema version values in the database against the catalogue schema
   * version values.
   * Returns verification status as result.
   * 
   * @param schemaVersion The map of the catalogue schema version values.
   * @param schemaDbVersion The map of the database schema version values.
   * @return The verification status code.
   */
  VerifyStatus verifySchemaVersion(const std::map<std::string, uint64_t> &schemaVersion, 
    const std::map<std::string, uint64_t> &schemaDbVersion) const;
  
  /**
   * Verifies table names in the database against the catalogue schema table names.
   * Returns verification status as result.
   * 
   * @param schemaTableNames The list of the catalogue schema table names.
   * @param dbTableNames The list of the database table names.
   * @return The verification status code.
   */
  VerifyStatus verifyTableNames(const std::list<std::string> &schemaTableNames, 
    const std::list<std::string> &dbTableNames) const;
  
  /**
   * Verifies index names in the database against the catalogue schema index names.
   * Returns verification status as result.
   * 
   * @param schemaIndexNames The list of the catalogue schema index names.
   * @param dbIndexNames The list of the database index names.
   * @return The verification status code.
   */
  VerifyStatus verifyIndexNames(const std::list<std::string> &schemaIndexNames, 
    const std::list<std::string> &dbIndexNames) const;
  
  /**
   * Verifies sequence names in the database against the catalogue schema sequence names.
   * Returns verification status as result.
   * 
   * @param schemaSequenceNames The list of the catalogue schema sequence names.
   * @param dbSequenceNames The list of the database sequence names.
   * @return The verification status code.
   */
  VerifyStatus verifySequenceNames(const std::list<std::string> &schemaSequenceNames, 
    const std::list<std::string> &dbSequenceNames) const;
  
  /**
   * Verifies trigger names in the database against the catalogue schema trigger names.
   * Returns verification status as result.
   * 
   * @param schemaTriggerNames The list of the catalogue schema trigger names.
   * @param dbTrgigerNames The list of the database trigger names.
   * @return The verification status code.
   */
  VerifyStatus verifyTriggerNames(const std::list<std::string> &schemaTriggerNames, 
    const std::list<std::string> &dbTrgigerNames) const;
}; // class VerifySchemaCmd

} // namespace catalogue
} // namespace cta
