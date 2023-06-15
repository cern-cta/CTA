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

#include <iostream>
#include <regex>

#include "DbToSQLiteStatementTransformer.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace catalogue {

/**********************************/
/* DbToSQLiteStatementTransformer */
/**********************************/
DbToSQLiteStatementTransformer::DbToSQLiteStatementTransformer(const std::string& statement) : m_statement(statement) {}

DbToSQLiteStatementTransformer::~DbToSQLiteStatementTransformer() {}

std::string DbToSQLiteStatementTransformer::transform() {
  return m_statement;
}

/**********************************/
/* DbToSQLiteStatementTransformer */
/**********************************/

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/
CreateGlobalTempTableToSQLiteStatementTransformer::CreateGlobalTempTableToSQLiteStatementTransformer(
  const std::string& statement) :
DbToSQLiteStatementTransformer(statement) {}

std::string CreateGlobalTempTableToSQLiteStatementTransformer::transform() {
  utils::searchAndReplace(m_statement, "GLOBAL TEMPORARY", "TEMPORARY");
  utils::searchAndReplace(m_statement, "ON COMMIT DELETE ROWS;", ";");
  return m_statement;
}

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/

/*****************************/
/* IndexStatementTransformer */
/*****************************/
IndexStatementTransformer::IndexStatementTransformer(const std::string& statement) :
DbToSQLiteStatementTransformer(statement) {}

std::string IndexStatementTransformer::transform() {
  utils::searchAndReplace(m_statement, "UNIQUE ", " ");
  // This is a bit crude, but it will work so long as we don't have indexes with multiple nested functions...
  std::regex lowerRegex("LOWER\\(([^\\)]*)\\)");
  m_statement = std::regex_replace(m_statement, lowerRegex, "\\1", std::regex_constants::format_sed);
  return m_statement;
}

/*****************************/
/* IndexStatementTransformer */
/*****************************/

/**********************************/
/* DeleteStatementTransformer     */
/**********************************/
DeleteStatementTransformer::DeleteStatementTransformer(const std::string& statement) :
DbToSQLiteStatementTransformer(statement) {}

std::string DeleteStatementTransformer::transform() {
  return "";
}

/**********************************/
/* DeleteStatementTransformer     */
/**********************************/

/*****************************************************/
/* DbToSQLiteStatementTransformerFactory             */
/*****************************************************/
std::unique_ptr<DbToSQLiteStatementTransformer>
  DbToSQLiteStatementTransformerFactory::create(const std::string& statement) {
  StatementType stmtType = statementToStatementType(statement);
  std::unique_ptr<DbToSQLiteStatementTransformer> ret;
  switch (stmtType) {
    case StatementType::CREATE_GLOBAL_TEMPORARY_TABLE:
      ret.reset(new CreateGlobalTempTableToSQLiteStatementTransformer(statement));
      break;
    case StatementType::CREATE_INDEX:
      ret.reset(new IndexStatementTransformer(statement));
      break;
    case StatementType::CREATE_SEQUENCE:  //CREATE SEQUENCE is not SQLite compatible, we delete this statement
    case StatementType::SKIP:
      ret.reset(new DeleteStatementTransformer(statement));
      break;
    default:
      //CREATE TABLE, INSERT INTO CTA_VERSION, OTHERS
      //If the statement does not need modification, we return it as it is
      ret.reset(new DbToSQLiteStatementTransformer(statement));
      break;
  }
  return ret;
}

const std::map<std::string, DbToSQLiteStatementTransformerFactory::StatementType>
  DbToSQLiteStatementTransformerFactory::regexToStatementMap =
    DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap();

DbToSQLiteStatementTransformerFactory::StatementType
  DbToSQLiteStatementTransformerFactory::statementToStatementType(const std::string& statement) {
  for (auto& kv : regexToStatementMap) {
    utils::Regex regexToTest(kv.first);
    if (regexToTest.exec(statement).size() != 0) {
      return kv.second;
    }
  }
  return StatementType::SKIP;
}

std::map<std::string, DbToSQLiteStatementTransformerFactory::StatementType>
  DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap() {
  std::map<std::string, StatementType> ret;
  ret["CREATE TABLE ([a-zA-Z_]+)"] = StatementType::CREATE_TABLE;
  ret["CREATE (UNIQUE )?INDEX ([a-zA-Z_]+)"] = StatementType::CREATE_INDEX;
  ret["CREATE GLOBAL TEMPORARY TABLE ([a-zA-Z_]+)"] = StatementType::CREATE_GLOBAL_TEMPORARY_TABLE;
  ret["CREATE SEQUENCE ([a-zA-Z_]+)"] = StatementType::CREATE_SEQUENCE;
  ret["INSERT INTO CTA_CATALOGUE([a-zA-Z_]+)"] = StatementType::INSERT_INTO_CTA_VERSION;
  ret["INSERT INTO ([a-zA-Z_]+)"] = StatementType::SKIP;
  return ret;
}

/*****************************************************/
/* DbToSQLiteStatementTransformerFactory             */
/*****************************************************/

}  // namespace catalogue
}  // namespace cta
