/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "StatisticsGetter.hpp"
#include "rdbms/Login.hpp"
#include "common/make_unique.hpp"

namespace cta {
namespace statistics{
    
  StatisticsGetter::StatisticsGetter(cta::rdbms::Conn& connection):m_conn(connection) {
    
  }


  StatisticsGetter::~StatisticsGetter() {
  }
  
  std::unique_ptr<Statistics> StatisticsGetter::getAllStatistics() const {
    const char * const sql = 
    "SELECT "
      "VIRTUAL_ORGANIZATION_NAME AS VO,"
      "SUM(NB_MASTER_FILES) AS TOTAL_MASTER_FILES_VO,"
      "SUM(MASTER_DATA_IN_BYTES) AS TOTAL_MASTER_DATA_BYTES_VO,"
      "SUM(NB_COPY_NB_1) AS TOTAL_NB_COPY_1_VO,"
      "SUM(COPY_NB_1_IN_BYTES) AS TOTAL_NB_COPY_1_BYTES_VO,"
      "SUM(NB_COPY_NB_GT_1) AS TOTAL_NB_COPY_NB_GT_1_VO,"
      "SUM(COPY_NB_GT_1_IN_BYTES) AS TOTAL_COPY_NB_GT_1_IN_BYTES_VO "
    "FROM "
      "TAPE "
    "INNER JOIN "
      "TAPE_POOL ON TAPE_POOL.TAPE_POOL_ID = TAPE.TAPE_POOL_ID "
    "INNER JOIN "
      "VIRTUAL_ORGANIZATION ON TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
    "GROUP BY VIRTUAL_ORGANIZATION_NAME";
    try {
      auto stmt = m_conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      //Build the Statitistics with the result set and return them
      Statistics::Builder builder(rset);
      return builder.build();
    } catch(cta::exception::Exception &ex) {
      ex.getMessage().str(std::string(__PRETTY_FUNCTION__) + ": " + ex.getMessage().str());
      throw;
    }
  }
  
  std::unique_ptr<StatisticsGetter> StatisticsGetterFactory::create(cta::rdbms::Conn& conn, cta::rdbms::Login::DbType dbType) {
    typedef cta::rdbms::Login::DbType DbType;
    std::unique_ptr<StatisticsGetter> ret;
    switch(dbType){
      case DbType::DBTYPE_IN_MEMORY:
      case DbType::DBTYPE_SQLITE:
      case DbType::DBTYPE_MYSQL:
      case DbType::DBTYPE_ORACLE:
      case DbType::DBTYPE_POSTGRESQL:
        ret = cta::make_unique<StatisticsGetter>(conn);
        break;
      case DbType::DBTYPE_NONE:
        throw cta::exception::Exception("In StatisticsGetterFactory::create(): DBTYPE_NONE provided.");
      default:
        throw cta::exception::Exception("In StatisticsGetterFactory::create(): Unknown database type");
    }
    return std::move(ret);
  }
  
}}


