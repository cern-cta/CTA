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
#include "catalogue/CatalogueSchema.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

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
  
#if 0
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
   * Verifies columns in the database according their description in
   * the catalogue schema. This method verifies tables in the database which
   * are present in the catalogue schema and also checks if some tables are
   * missed in the database.
   * Returns verification status as result.
   * 
   * @param schemaTableNames The list of the catalogue schema table names.
   * @param dbTableNames The list of the database table names.
   * @param schema The reference to the catalogue schema.
   * @param conn The reference to the database connection.
   * @return The verification status code.
   */
  VerifyStatus verifyColumns(const std::list<std::string> &schemaTableNames, 
    const std::list<std::string> &dbTableNames, const CatalogueSchema &schema,
    const rdbms::Conn &conn) const;
  
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
#endif
  
}; // class VerifySchemaCmd

} // namespace catalogue
} // namespace cta
