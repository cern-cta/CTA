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

#include <string>
#include <map>
#include <memory>

namespace cta {
namespace catalogue {

/**
 * This class transforms a statement into a SQLite compatible one
 */
class DbToSQLiteStatementTransformer {
public:
  /**
   * Constructs a DbToSQLiteStatementTransformer
   * @param statement the statement to transform into a SQLite compatible one
   */
  DbToSQLiteStatementTransformer(const std::string& statement);
  virtual ~DbToSQLiteStatementTransformer();
  /**
   * Transform the statement as a SQLite compatible one
   * @return the statement compatible with SQLite
   */
  virtual std::string transform();

protected:
  std::string m_statement;
};

/**
 * Transform a CREATE GLOBAL TEMPORARY TABLE as a CREATE TABLE statement for SQLite
 * @param statement the CREATE GLOBQL TEMPORARY TABLE statement
 */
class CreateGlobalTempTableToSQLiteStatementTransformer : public DbToSQLiteStatementTransformer {
public:
  CreateGlobalTempTableToSQLiteStatementTransformer(const std::string& statement);
  std::string transform() override;
};

/**
 * Transform a CREATE INDEX or CREATE UNIQUE INDEX statement for SQLite.
 * INDEX statements in SQLite cannot contain functions.
 *
 * @param statement the CREATE INDEX statement
 */
class IndexStatementTransformer : public DbToSQLiteStatementTransformer {
public:
  IndexStatementTransformer(const std::string& statement);
  std::string transform() override;
};

/**
 * Delete the statement passed in parameter
 * @param statement the statement to delete
 */
class DeleteStatementTransformer : public DbToSQLiteStatementTransformer {
public:
  DeleteStatementTransformer(const std::string& statement);
  std::string transform() override;
};

/**
 * Factory of transformer classes
 * This will instanciate the correct transformer according to the statement passed in parameter
 */
class DbToSQLiteStatementTransformerFactory {
  enum StatementType {
    CREATE_TABLE,
    CREATE_INDEX,
    CREATE_GLOBAL_TEMPORARY_TABLE,
    CREATE_SEQUENCE,
    INSERT_INTO_CTA_VERSION,
    SKIP /*This statement is deleted*/
  };

private:
  static const std::map<std::string, StatementType> regexToStatementMap;
  /**
   * Initialize a map in order to map a Regex to a StatementType
   * This will allow to know to what StatementType corresponds a statement
   * @return the initialized map<RegexString,StatementType>
   */
  static std::map<std::string, StatementType> initializeRegexToStatementMap();
  /**
   * Returns the StatementType corresponding to the statement passed in parameter
   * @param statement the statement that we want to know its StatementType
   * @return the StatementType corresponding to the statement passed in parameter
   */
  static StatementType statementToStatementType(const std::string& statement);

public:
  /**
   * Create a DbToSQLiteStatementTransformer according to the statement passed in parameter
   * @param statement the statement to transform via the DbToSQLiteStatementTransformer
   * @return a DbToSQLiteStatementTransformer instance according to the statement passed in parameter
   */
  static std::unique_ptr<DbToSQLiteStatementTransformer> create(const std::string& statement);
};

}  // namespace catalogue
}  // namespace cta
