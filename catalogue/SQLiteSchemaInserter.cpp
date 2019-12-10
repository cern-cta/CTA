/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include "SQLiteSchemaInserter.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "DbToSQLiteStatementTransformer.hpp"
#include "common/log/DummyLogger.hpp"
#include <iostream>
#include <fstream>

namespace cta {
namespace catalogue {
  
SQLiteSchemaInserter::SQLiteSchemaInserter(const std::string & schemaVersion, const cta::rdbms::Login::DbType &dbType, const std::string &allVersionsSchemaDirectory,const std::string &sqliteFileName):m_schemaVersion(schemaVersion),m_dbType(dbType),m_allVersionSchemaDirectory(allVersionsSchemaDirectory),m_sqliteFileName(sqliteFileName) {
  log::DummyLogger dl("dummy","dummy");
  m_sqliteCatalogue = new SchemaCreatingSqliteCatalogue(dl,m_sqliteFileName,1,1,false);
  m_conn = m_sqliteCatalogue->m_connPool.getConn();
}

SQLiteSchemaInserter::~SQLiteSchemaInserter() {
  if(m_sqliteCatalogue != nullptr) {
    delete m_sqliteCatalogue;
  }
  ::remove(m_sqliteFileName.c_str());
}


void SQLiteSchemaInserter::insert() {
  std::list<std::string> statements = getAllStatementsFromSchema(readSchemaFromFile());
  std::string transformedSchema;
  for(auto& stmt: statements){
    transformedSchema.append(DbToSQLiteStatementTransformerFactory::create(stmt)->transform());
  }
  m_sqliteCatalogue->executeNonQueries(m_conn,transformedSchema);
}

void SQLiteSchemaInserter::executeStatements(const std::string &sqliteStatements){
  
}

std::list<std::string> SQLiteSchemaInserter::getAllStatementsFromSchema(const std::string &schema){
  std::list<std::string> statements;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  try {
    while(std::string::npos != (findResult = schema.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(schema.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        statements.push_back(sqlStmt+";");
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return statements;
}

std::string SQLiteSchemaInserter::readSchemaFromFile() {
  std::string schemaFilePath = getSchemaFilePath();
  std::ifstream schemaFile(schemaFilePath);
  if(schemaFile.fail()){
    throw cta::exception::Exception("In SQLiteSchemaInserter::readSchemaFromFile, unable to open the file located in "+schemaFilePath);
  }
  std::string content((std::istreambuf_iterator<char>(schemaFile)),(std::istreambuf_iterator<char>()));
  return content;
}

std::string SQLiteSchemaInserter::getSchemaFilePath() {
  return m_allVersionSchemaDirectory+m_schemaVersion+"/"+getDatabaseType()+c_catalogueFileNameTrailer;
}

std::string SQLiteSchemaInserter::getDatabaseType() {
  switch(m_dbType){
  case rdbms::Login::DBTYPE_IN_MEMORY:
  case rdbms::Login::DBTYPE_SQLITE:
    return "sqlite";
  case rdbms::Login::DBTYPE_POSTGRESQL:
    return "postgres";
  case rdbms::Login::DBTYPE_MYSQL:
    return "mysql";
  case rdbms::Login::DBTYPE_ORACLE:
    return "oracle";
  case rdbms::Login::DBTYPE_NONE:
    throw exception::Exception("The database type should not be DBTYPE_NONE");
  default:
    {
      exception::Exception ex;
      ex.getMessage() << "Unknown database type: value=" << m_dbType;
      throw ex;
    }
  }
}

}}