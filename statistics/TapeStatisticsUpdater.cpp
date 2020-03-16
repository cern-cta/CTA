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
      "COALESCE(SUM(SUMMARIZED_STATS.NB_COPY_NB_1) + SUM(SUMMARIZED_STATS.NB_COPY_NB_GT_1),0) AS NB_MASTER_FILES,"
      "COALESCE(SUM(SUMMARIZED_STATS.COPY_NB_1_IN_BYTES) + SUM(SUMMARIZED_STATS.COPY_NB_GT_1_IN_BYTES),0) AS MASTER_DATA_IN_BYTES,"
      "COALESCE(SUM(SUMMARIZED_STATS.NB_COPY_NB_1),0) AS NB_COPY_NB_1,"
      "COALESCE(SUM(SUMMARIZED_STATS.COPY_NB_1_IN_BYTES),0) AS COPY_NB_1_IN_BYTES,"
      "COALESCE(SUM(SUMMARIZED_STATS.NB_COPY_NB_GT_1),0) AS NB_COPY_NB_GT_1,"
      "COALESCE(SUM(SUMMARIZED_STATS.COPY_NB_GT_1_IN_BYTES),0) AS COPY_NB_GT_1_IN_BYTES "
    "FROM "
      "TAPE T "
    "LEFT OUTER JOIN "
    "("
      "SELECT "
        "STATS.VID AS VID,"
        "CASE WHEN COPY_NB = 1 THEN STATS.NB_FILES ELSE 0 END AS NB_COPY_NB_1,"
        "CASE WHEN COPY_NB = 1 THEN STATS.DATA_IN_BYTES ELSE 0 END AS COPY_NB_1_IN_BYTES,"
        "CASE WHEN COPY_NB > 1 THEN STATS.NB_FILES ELSE 0 END AS NB_COPY_NB_GT_1,"
        "CASE WHEN COPY_NB > 1 THEN STATS.DATA_IN_BYTES ELSE 0 END AS COPY_NB_GT_1_IN_BYTES "
      "FROM "
      "("
        "SELECT "
          "TAPE_FILE.VID AS VID,"
          "TAPE_FILE.COPY_NB AS COPY_NB,"
          "COUNT(TAPE_FILE.FSEQ) AS NB_FILES,"
          "SUM(ARCHIVE_FILE.SIZE_IN_BYTES) AS DATA_IN_BYTES "
        "FROM "
          "TAPE_FILE "
        "INNER JOIN "
          "ARCHIVE_FILE "
        "ON "
          "TAPE_FILE.ARCHIVE_FILE_ID = ARCHIVE_FILE.ARCHIVE_FILE_ID "
        "WHERE "
          "TAPE_FILE.SUPERSEDED_BY_VID IS NULL AND TAPE_FILE.SUPERSEDED_BY_FSEQ IS NULL AND TAPE_FILE.VID = TAPE_TO_UPDATE.VID "
        "GROUP BY TAPE_FILE.VID, TAPE_FILE.COPY_NB"
      ") STATS "
      "GROUP BY STATS.VID, STATS.COPY_NB, STATS.NB_FILES, STATS.DATA_IN_BYTES"
    ") SUMMARIZED_STATS ON SUMMARIZED_STATS.VID = TAPE_TO_UPDATE.VID "
    "WHERE T.VID = TAPE_TO_UPDATE.VID "
    "GROUP BY T.VID"
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

