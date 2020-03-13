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

#include "TapeStatisticsUpdater.hpp"
#include "rdbms/Login.hpp"

namespace cta { namespace statistics {
  
TapeStatisticsUpdater::TapeStatisticsUpdater(cta::rdbms::Conn &conn):m_conn(conn) {
}

TapeStatisticsUpdater::~TapeStatisticsUpdater() {
}

void TapeStatisticsUpdater::updateTapeStatistics() {
  const char * const sql = 
  "UPDATE TAPE TAPE_TO_UPDATE SET"
  "("
    "DIRTY,"
    "NB_MASTER_FILES,"
    "MASTER_DATA_IN_BYTES,"
    "NB_COPY_NB_1,"
    "COPY_NB_1_IN_BYTES,"
    "NB_COPY_NB_GT_1,"
    "COPY_NB_GT_1_IN_BYTES"
  ")"
  " = "
  "("
    "SELECT "
      "'0' AS DIRTY,"
      "COALESCE(NON_EMPTY_TAPE_STATS.NB_MASTER_FILES, 0) AS COMPUTED_NB_MASTER_FILES,"
      "COALESCE(NON_EMPTY_TAPE_STATS.MASTER_DATA_IN_BYTES, 0) AS COMPUTED_MASTER_DATA_IN_BYTES,"
      "COALESCE(NON_EMPTY_TAPE_STATS.NB_COPY_NB_1,0) AS NB_COPY_NB_1,"
      "COALESCE(NON_EMPTY_TAPE_STATS.COPY_NB_1_IN_BYTES,0) AS COPY_NB_1_IN_BYTES,"
      "COALESCE(NON_EMPTY_TAPE_STATS.NB_COPY_NB_GT_1,0) AS NB_COPY_NB_GT_1,"
      "COALESCE(NON_EMPTY_TAPE_STATS.COPY_NB_GT_1_IN_BYTES,0) AS COPY_NB_GT_1_IN_BYTES "
    "FROM "
      "TAPE T "
    "LEFT OUTER JOIN "
    "("
      "SELECT "
        "TAPE_FILE.VID,"
        "'0' AS DIRTY,"
        "COUNT(TAPE_FILE.FSEQ) AS NB_MASTER_FILES,"
        "SUM(ARCHIVE_FILE.SIZE_IN_BYTES) AS MASTER_DATA_IN_BYTES,"
        "CASE WHEN TAPE_FILE.COPY_NB = 1 THEN COUNT(TAPE_FILE.FSEQ) ELSE 0 END AS NB_COPY_NB_1,"
        "CASE WHEN TAPE_FILE.COPY_NB = 1 THEN SUM(ARCHIVE_FILE.SIZE_IN_BYTES) ELSE 0 END AS COPY_NB_1_IN_BYTES,"
        "CASE WHEN TAPE_FILE.COPY_NB > 1 THEN COUNT(TAPE_FILE.FSEQ) ELSE 0 END AS NB_COPY_NB_GT_1,"
        "CASE WHEN TAPE_FILE.COPY_NB > 1 THEN SUM(ARCHIVE_FILE.SIZE_IN_BYTES) ELSE 0 END AS COPY_NB_GT_1_IN_BYTES "
      "FROM "
        "TAPE_FILE "
      "INNER JOIN ARCHIVE_FILE ON "
        "TAPE_FILE.ARCHIVE_FILE_ID = ARCHIVE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "TAPE_FILE.SUPERSEDED_BY_VID IS NULL AND "
        "TAPE_FILE.SUPERSEDED_BY_FSEQ IS NULL "
      "GROUP BY TAPE_FILE.VID, TAPE_FILE.COPY_NB"
    ") NON_EMPTY_TAPE_STATS ON "
      "T.VID = NON_EMPTY_TAPE_STATS.VID "
    "WHERE T.VID = TAPE_TO_UPDATE.VID"
  ") "
  "WHERE TAPE_TO_UPDATE.DIRTY='1'";
  try {
    auto stmt = m_conn.createStmt(sql);
    stmt.executeNonQuery();
    m_nbUpdatedTapes = stmt.getNbAffectedRows();
  } catch(cta::exception::Exception &ex) {
    ex.getMessage().str(std::string(__PRETTY_FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

uint64_t TapeStatisticsUpdater::getNbUpdatedTapes() {
  return m_nbUpdatedTapes;
}

std::unique_ptr<TapeStatisticsUpdater> TapeStatisticsUpdaterFactory::create(cta::rdbms::Login::DbType dbType, cta::rdbms::Conn& conn){
  typedef cta::rdbms::Login::DbType DbType;
  std::unique_ptr<TapeStatisticsUpdater> ret;
  switch(dbType){
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
    case DbType::DBTYPE_MYSQL:
      throw cta::exception::Exception("In TapeStatisticsUpdaterFactory::create(), the "+cta::rdbms::Login::dbTypeToString(dbType)+" database type is not supported.");
    case DbType::DBTYPE_ORACLE:
    case DbType::DBTYPE_POSTGRESQL:
      ret.reset(new TapeStatisticsUpdater(conn));
      return std::move(ret);
    default:
      throw cta::exception::Exception("In TapeStatisticsUpdaterFactory::create(), unknown database type.");
  }
}


}}

