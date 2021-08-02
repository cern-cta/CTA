/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <iostream>

#include "DbToSQLiteStatementTransformer.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"


namespace cta {
namespace catalogue {
  
/**********************************/
/* DbToSQLiteStatementTransformer */
/**********************************/
DbToSQLiteStatementTransformer::DbToSQLiteStatementTransformer(const std::string &statement):m_statement(statement) {
}

DbToSQLiteStatementTransformer::~DbToSQLiteStatementTransformer() {
}

std::string DbToSQLiteStatementTransformer::transform(){
  return m_statement;
}

/**********************************/
/* DbToSQLiteStatementTransformer */
/**********************************/

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/

CreateGlobalTempTableToSQLiteStatementTransformer::CreateGlobalTempTableToSQLiteStatementTransformer(const std::string &statement):DbToSQLiteStatementTransformer(statement){}

std::string CreateGlobalTempTableToSQLiteStatementTransformer::transform(){
  utils::searchAndReplace(m_statement,"GLOBAL TEMPORARY","TEMPORARY");
  utils::searchAndReplace(m_statement,"ON COMMIT DELETE ROWS;",";");
  return m_statement;
}

/*****************************************************/
/* CreateGlobalTempTableToSQLiteStatementTransformer */
/*****************************************************/

/**********************************/
/* DeleteStatementTransformer     */
/**********************************/

DeleteStatementTransformer::DeleteStatementTransformer(const std::string& statement):DbToSQLiteStatementTransformer(statement){}

std::string DeleteStatementTransformer::transform(){
  return "";
}

/**********************************/
/* DeleteStatementTransformer     */
/**********************************/

/*****************************************************/
/* DbToSQLiteStatementTransformerFactory             */
/*****************************************************/
std::unique_ptr<DbToSQLiteStatementTransformer> DbToSQLiteStatementTransformerFactory::create(const std::string &statement){
  typedef DbToSQLiteStatementTransformerFactory::StatementType StatementType;
  StatementType stmtType = statementToStatementType(statement);
  std::unique_ptr<DbToSQLiteStatementTransformer> ret;
  switch(stmtType){
    case StatementType::CREATE_GLOBAL_TEMPORARY_TABLE:
      ret.reset(new CreateGlobalTempTableToSQLiteStatementTransformer(statement));
      break;
    case StatementType::CREATE_SEQUENCE://CREATE SEQUENCE is not SQLite compatible, we delete this statement
    case StatementType::SKIP:
      ret.reset(new DeleteStatementTransformer(statement));
      break;
    default:
      //CREATE TABLE, CREATE INDEX, INSERT INTO CTA_VERSION, OTHERS
      //If the statement does not need modification, we return it as it is
      ret.reset(new DbToSQLiteStatementTransformer(statement));
      break;
  }
  return ret;
}

const std::map<std::string,DbToSQLiteStatementTransformerFactory::StatementType> DbToSQLiteStatementTransformerFactory::regexToStatementMap = DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap();

DbToSQLiteStatementTransformerFactory::StatementType DbToSQLiteStatementTransformerFactory::statementToStatementType(const std::string &statement){
  for(auto &kv: regexToStatementMap){
    utils::Regex regexToTest(kv.first);
    if(regexToTest.exec(statement).size() != 0){
      return kv.second;
    }
  }
  return StatementType::SKIP;
}

std::map<std::string,DbToSQLiteStatementTransformerFactory::StatementType> DbToSQLiteStatementTransformerFactory::initializeRegexToStatementMap()
{
  typedef DbToSQLiteStatementTransformerFactory::StatementType StatementType;
  std::map<std::string,StatementType> ret;
  ret["CREATE TABLE ([a-zA-Z_]+)"] = StatementType::CREATE_TABLE;
  ret["CREATE INDEX ([a-zA-Z_]+)"] = StatementType::CREATE_INDEX;
  ret["CREATE GLOBAL TEMPORARY TABLE ([a-zA-Z_]+)"] = StatementType::CREATE_GLOBAL_TEMPORARY_TABLE;
  ret["CREATE SEQUENCE ([a-zA-Z_]+)"] = StatementType::CREATE_SEQUENCE;
  ret["INSERT INTO CTA_CATALOGUE([a-zA-Z_]+)"] = StatementType::INSERT_INTO_CTA_VERSION;
  ret["INSERT INTO ([a-zA-Z_]+)"] = StatementType::SKIP;
  return ret;
}

/*****************************************************/
/* DbToSQLiteStatementTransformerFactory             */
/*****************************************************/

}}