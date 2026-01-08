/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountQueueCleanup.hpp"

namespace cta::maintd {

  MountQueueCleanup::MountQueueCleanup(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                       size_t batchSize) : m_lc(lc),
                                                           m_catalogue(catalogue),
                                                           m_RelationalDB(pgs),
                                                           m_batchSize(batchSize) {
    log::ScopedParamContainer params(m_lc);
    params.add("batchSize", m_batchSize);
    m_lc.log(cta::log::INFO, "Created MountQueueCleanup routine.");
  }

  void MountQueueCleanup::execute() {
    cta::utils::Timer timer;
    // Get all active mount IDs for drives which do have an active mount registered in the catalogue
    std::unordered_map<std::string, std::optional<uint64_t>> driveNameMountIdOpt = m_catalogue.getTapeDriveMountIDs();

    // Get all active mount IDs from the Scheduler DB
    std::vector<uint64_t> activeMountIds_scheduler = m_RelationalDB.getActiveMountIDs();

    std::vector<uint64_t> activeMountIds_catalogue;
    activeMountIds_catalogue.reserve(driveNameMountIdOpt.size());

    for (const auto& kv : driveNameMountIdOpt) {
        if (kv.second) {
            activeMountIds_catalogue.emplace_back(*kv.second);
        }
    }
    m_RelationalDB.cleanInactiveMountQueues(activeMountIds_catalogue,m_batchSize);
    pending
    // Insert temporary table with active mounts to Scheduler DB;
    // bind vector<int64_t> as a BIGINT[] parameter
    execute(
      "INSERT INTO active_mounts (mount_id) "
      "SELECT unnest($1::bigint[])",
      activeMountIds
    );
    /*
    CREATE TEMP TABLE active_mounts (
      mount_id BIGINT PRIMARY KEY
    ) ON COMMIT PRESERVE ROWS;
     SELECT p.*
FROM pending p
WHERE NOT EXISTS (
    SELECT 1
    FROM active_mounts a
    WHERE a.mount_id = p.mount_id
);
     SELECT p.*
FROM pending p
LEFT JOIN active_mounts a
    ON a.mount_id = p.mount_id
WHERE a.mount_id IS NULL;
     WITH batch AS (
    SELECT p.id
    FROM pending p
    WHERE p.updated_at > :last_processed
      AND NOT EXISTS (
          SELECT 1
          FROM active_mounts a
          WHERE a.mount_id = p.mount_id
      )
    ORDER BY p.updated_at
    LIMIT 10000
)
UPDATE pending
SET status = 'INACTIVE'
WHERE id IN (SELECT id FROM batch);
    */
    //std::string sql = "DELETE FROM ARCHIVE_ACTIVE_QUEUE WHERE STATUS = :STATUS";
    //auto stmt = m_conn.createStmt(sql);
    //stmt.bindString(":STATUS", to_string(cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion));
    //stmt.executeNonQuery();
    try {
      m_conn.commit();
    } catch (exception::Exception &ex) {
      lc.log(log::ERR, "In RelationalDBQCR::execute(): failed to delete rows of ARCHIVE_ACTIVE_QUEUE" +
                     ex.getMessageValue());
      m_conn.rollback();
    }
    auto ndelrows = stmt.getNbAffectedRows();
    auto tdelsec = timer.secs(cta::utils::Timer::resetCounter);
    lc.log(log::INFO, std::string("In RelationalDBQCR::execute(): Deleted ") +
                      std::to_string(ndelrows) +
                      std::string(" rows from the ARCHIVE_ACTIVE_QUEUE. Operation took ") +
                      std::to_string(tdelsec) + std::string(" seconds."));
    */
    /* Autovacuum might be heavy and degrading performance
     * optionally we can partition the table and drop partitions,
     * this drop operation locks only the specific partition
     * without affecting other parts of the table
     * 1. get the current time stamps in epoch and check existing partitions
     * 2. create new partitions if needed
     * 3. check and drop partitions which are ready to be dropped
     * SELECT
     * COUNT(*) = COUNT(CASE WHEN status = 'completed' THEN 1 END) AS all_rows_completed
     * FROM
     * partition_name;  -- Replace partition_name with the actual name
     */
  }


  std::string MountQueueCleanup::getName() const {
    return "MountQueueCleanupRoutine";
  }

}  // namespace cta::maintd
