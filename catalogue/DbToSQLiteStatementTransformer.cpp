/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DbToSQLiteStatementTransformer.hpp"

#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"

#include <iostream>
#include <regex>

namespace cta::catalogue {

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/
CreateGlobalTempTableToSQLiteStatementTransformer::CreateGlobalTempTableToSQLiteStatementTransformer(
  const std::string& statement)
    : DbToSQLiteStatementTransformer(statement) {}

std::string CreateGlobalTempTableToSQLiteStatementTransformer::transform() {
  utils::searchAndReplace(getStatement(), "GLOBAL TEMPORARY", "TEMPORARY");
  utils::searchAndReplace(getStatement(), "ON COMMIT DELETE ROWS;", ";");
  return getStatement();
}

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/

/*****************************/
/* IndexStatementTransformer */
/*****************************/
IndexStatementTransformer::IndexStatementTransformer(const std::string& statement)
    : DbToSQLiteStatementTransformer(statement) {}

std::string IndexStatementTransformer::transform() {
  utils::searchAndReplace(getStatement(), "UNIQUE ", " ");
  // This is a bit crude, but it will work so long as we don't have indexes with multiple nested functions...
  std::regex lowerRegex("LOWER\\(([^\\)]*)\\)");
  getStatement() = std::regex_replace(getStatement(), lowerRegex, "\\1", std::regex_constants::format_sed);
  return getStatement();
}

/*****************************/
/* IndexStatementTransformer */
/*****************************/

/**********************************/
/* DeleteStatementTransformer     */
/**********************************/
DeleteStatementTransformer::DeleteStatementTransformer(const std::string& statement)
    : DbToSQLiteStatementTransformer(statement) {}

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

const std::map<std::string, DbToSQLiteStatementTransformerFactory::StatementType, std::less<>>
  DbToSQLiteStatementTransformerFactory::regexToStatementMap =
    DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap();

DbToSQLiteStatementTransformerFactory::StatementType
DbToSQLiteStatementTransformerFactory::statementToStatementType(const std::string& statement) {
  for (const auto& [regex, type] : regexToStatementMap) {
    utils::Regex regexToTest(regex);
    if (!regexToTest.exec(statement).empty()) {
      return type;
    }
  }
  return StatementType::SKIP;
}

std::map<std::string, DbToSQLiteStatementTransformerFactory::StatementType, std::less<>>
DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap() {
  std::map<std::string, StatementType, std::less<>> ret;
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

}  // namespace cta::catalogue
