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

#include "DatabaseStatisticsService.hpp"


namespace cta { namespace statistics {
  
DatabaseStatisticsService::DatabaseStatisticsService(cta::rdbms::Conn & databaseConnection):m_conn(databaseConnection) {
}

DatabaseStatisticsService::~DatabaseStatisticsService() {
}

void DatabaseStatisticsService::updateStatisticsPerTape(){
  //to update the statistics, we will first select the DIRTY tapes ordered by VID and we will run an update for each row.
  
  const char * const selectVids = "SELECT TAPE.VID AS VID FROM TAPE WHERE TAPE.DIRTY='1' ORDER BY TAPE.VID";
  
  const char * const updateSql = 
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
          "TAPE_FILE.VID = TAPE_TO_UPDATE.VID "
        "GROUP BY TAPE_FILE.VID, TAPE_FILE.COPY_NB"
      ") STATS "
      "GROUP BY STATS.VID, STATS.COPY_NB, STATS.NB_FILES, STATS.DATA_IN_BYTES"
    ") SUMMARIZED_STATS ON SUMMARIZED_STATS.VID = TAPE_TO_UPDATE.VID "
    "WHERE T.VID = TAPE_TO_UPDATE.VID "
    "GROUP BY T.VID"
  ") "
  "WHERE TAPE_TO_UPDATE.VID = :VID";
  
  try {
    auto selectStmt = m_conn.createStmt(selectVids);
    auto rset = selectStmt.executeQuery();
    while(rset.next()){
      //For all DIRTY tapes, update its statistics
      auto updateStmt = m_conn.createStmt(updateSql);
      updateStmt.bindString(":VID",rset.columnString("VID"));
      updateStmt.executeNonQuery();
      m_nbUpdatedTapes += updateStmt.getNbAffectedRows();
    }
  } catch(cta::exception::Exception &ex) {
    ex.getMessage().str(std::string(__PRETTY_FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void DatabaseStatisticsService::saveStatistics(const cta::statistics::Statistics& statistics) {
  //First we save the general FILE statistics, then we go for the per-vo statisticss
  saveFileStatistics(statistics);
  saveStatisticsPerVo(statistics);
}

void DatabaseStatisticsService::saveFileStatistics(const cta::statistics::Statistics& statistics) {
  try {
    const char * const sql = 
    "INSERT INTO "
      "FILE_STATISTICS "
      "("
        "NB_MASTER_FILES,"
        "MASTER_DATA_IN_BYTES,"
        "NB_COPY_NB_1,"
        "NB_COPY_NB_1_IN_BYTES,"
        "NB_COPY_NB_GT_1,"
        "NB_COPY_NB_GT_1_IN_BYTES,"
        "UPDATE_TIME"
    ") "
    "VALUES "
    "("
      ":NB_MASTER_FILES,"
      ":MASTER_DATA_IN_BYTES,"
      ":NB_COPY_NB_1,"
      ":NB_COPY_NB_1_IN_BYTES,"
      ":NB_COPY_NB_GT_1,"
      ":NB_COPY_NB_GT_1_IN_BYTES,"
      ":UPDATE_TIME"
    ")";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":NB_MASTER_FILES",statistics.getTotalFiles());
    stmt.bindUint64(":MASTER_DATA_IN_BYTES",statistics.getTotalBytes());
    stmt.bindUint64(":NB_COPY_NB_1",statistics.getTotalFilesCopyNb1());
    stmt.bindUint64(":NB_COPY_NB_1_IN_BYTES",statistics.getTotalBytesCopyNb1());
    stmt.bindUint64(":NB_COPY_NB_GT_1",statistics.getTotalFilesCopyNbGt1());
    stmt.bindUint64(":NB_COPY_NB_GT_1_IN_BYTES",statistics.getTotalBytesCopyNbGt1());
    stmt.bindUint64(":UPDATE_TIME",statistics.getUpdateTime());
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void DatabaseStatisticsService::saveStatisticsPerVo(const cta::statistics::Statistics& statistics) {
  try {
    const char * const sql = 
    "INSERT INTO "
      "VO_STATISTICS "
      "("
        "VO,"
        "NB_MASTER_FILES,"
        "MASTER_DATA_IN_BYTES,"
        "NB_COPY_NB_1,"
        "NB_COPY_NB_1_IN_BYTES,"
        "NB_COPY_NB_GT_1,"
        "NB_COPY_NB_GT_1_IN_BYTES,"
        "UPDATE_TIME"
      ") "
      "VALUES "
      "("
        ":VO,"
        ":NB_MASTER_FILES,"
        ":MASTER_DATA_IN_BYTES,"
        ":NB_COPY_NB_1,"
        ":NB_COPY_NB_1_IN_BYTES,"
        ":NB_COPY_NB_GT_1,"
        ":NB_COPY_NB_GT_1_IN_BYTES,"
        ":UPDATE_TIME"
      ")";
    for(const auto & stat: statistics.getAllVOStatistics()){
      auto stmt = m_conn.createStmt(sql);
      auto voFileStatistics = stat.second;
      stmt.bindString(":VO",stat.first);
      stmt.bindUint64(":NB_MASTER_FILES",voFileStatistics.nbMasterFiles);
      stmt.bindUint64(":MASTER_DATA_IN_BYTES",voFileStatistics.masterDataInBytes);
      stmt.bindUint64(":NB_COPY_NB_1",voFileStatistics.nbCopyNb1);
      stmt.bindUint64(":NB_COPY_NB_1_IN_BYTES",voFileStatistics.copyNb1InBytes);
      stmt.bindUint64(":NB_COPY_NB_GT_1",voFileStatistics.nbCopyNbGt1);
      stmt.bindUint64(":NB_COPY_NB_GT_1_IN_BYTES",voFileStatistics.copyNbGt1InBytes);
      stmt.bindUint64(":UPDATE_TIME",statistics.getUpdateTime());
      stmt.executeNonQuery();
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}



std::unique_ptr<cta::statistics::Statistics> DatabaseStatisticsService::getStatistics() {
  const char * const sql = 
  "SELECT "
    "VIRTUAL_ORGANIZATION_NAME AS VO,"
    "COALESCE(SUM(TAPE.NB_MASTER_FILES),0) AS TOTAL_MASTER_FILES_VO,"
    "COALESCE(SUM(TAPE.MASTER_DATA_IN_BYTES),0) AS TOTAL_MASTER_DATA_BYTES_VO,"
    "COALESCE(SUM(TAPE.NB_COPY_NB_1),0) AS TOTAL_NB_COPY_1_VO,"
    "COALESCE(SUM(TAPE.COPY_NB_1_IN_BYTES),0) AS TOTAL_NB_COPY_1_BYTES_VO,"
    "COALESCE(SUM(TAPE.NB_COPY_NB_GT_1),0) AS TOTAL_NB_COPY_NB_GT_1_VO,"
    "COALESCE(SUM(TAPE.COPY_NB_GT_1_IN_BYTES),0) AS TOTAL_COPY_NB_GT_1_IN_BYTES_VO "
  "FROM "
    "VIRTUAL_ORGANIZATION "
  "LEFT OUTER JOIN "
    "TAPE_POOL ON VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID = TAPE_POOL.VIRTUAL_ORGANIZATION_ID "
  "LEFT OUTER JOIN "
    "TAPE ON TAPE.TAPE_POOL_ID =  TAPE_POOL.TAPE_POOL_ID "
  "GROUP BY VIRTUAL_ORGANIZATION_NAME";
  try {
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    //Build the Statitistics with the result set and return them
    return Statistics::Builder().build(rset);
  } catch(cta::exception::Exception &ex) {
    ex.getMessage().str(std::string(__PRETTY_FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}



}}