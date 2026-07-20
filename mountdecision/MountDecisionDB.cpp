/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecisionDB.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Rset.hpp"

#include <map>
#include <set>
#include <tuple>

namespace cta::mountdecision {

namespace {

std::optional<std::string> nonEmptyOptional(std::optional<std::string> value) {
  if (value.has_value() && value->empty()) {
    return std::nullopt;
  }
  return value;
}

template<typename Stmt>
void bindCommonMountCandidateFields(Stmt& stmt, const MountCandidate& candidate) {
  stmt.bindString(":CANDIDATE_KEY", candidate.candidateKey);
  stmt.bindString(":MOUNT_TYPE", cta::common::dataStructures::toString(candidate.mountType));
  stmt.bindString(":LOGICAL_LIBRARY", candidate.logicalLibrary);
  stmt.bindString(":TAPE_POOL", candidate.tapePool);
  stmt.bindString(":VO", candidate.vo);
  stmt.bindString(":VID", nonEmptyOptional(candidate.vid));
  stmt.bindString(":ACTIVITY", nonEmptyOptional(candidate.activity));
  stmt.bindUint64(":PRIORITY", candidate.priority);
  stmt.bindUint64(":MIN_REQUEST_AGE", candidate.minRequestAge);
  stmt.bindUint64(":FILES_QUEUED", candidate.filesQueued);
  stmt.bindUint64(":BYTES_QUEUED", candidate.bytesQueued);
  stmt.bindUint64(":OLDEST_JOB_START_TIME", candidate.oldestJobStartTime);
  stmt.bindUint64(":YOUNGEST_JOB_START_TIME", candidate.youngestJobStartTime);
  stmt.bindDouble(":RATIO_OF_MOUNT_QUOTA_USED", candidate.ratioOfMountQuotaUsed);
  stmt.bindUint64(":CANDIDATE_SCORE", candidate.candidateScore);
  stmt.bindUint64(":OVERRIDE_CANDIDATE_SCORE", candidate.overrideCandidateScore);
  stmt.bindString(":MEDIA_TYPE", nonEmptyOptional(candidate.mediaType));
  stmt.bindUint64(":LABEL_FORMAT",
                  candidate.vid.has_value() ? std::optional<uint64_t>(candidate.labelFormat) : std::nullopt);
  stmt.bindString(":VENDOR", nonEmptyOptional(candidate.vendor));
  stmt.bindUint64(":CAPACITY_IN_BYTES",
                  candidate.vid.has_value() ? std::optional<uint64_t>(candidate.capacityInBytes) : std::nullopt);
  stmt.bindUint64(":LAST_FSEQ", candidate.lastFSeq);
  stmt.bindString(":ENCRYPTION_KEY_NAME", nonEmptyOptional(candidate.encryptionKeyName));
  stmt.bindString(":CREATED_BY", nonEmptyOptional(candidate.createdBy));
}

using ReservationSlotKey = std::tuple<std::string, std::string, std::string, std::string, std::optional<std::string>>;
using ReservationSlotCounts = std::map<ReservationSlotKey, uint64_t>;

ReservationSlotKey getReservationSlotKey(const MountCandidate& candidate) {
  std::optional<std::string> vid;
  if (cta::common::dataStructures::getMountBasicType(candidate.mountType)
      == cta::common::dataStructures::MountType::Retrieve) {
    vid = candidate.vid;
  }
  return {cta::common::dataStructures::toString(candidate.mountType),
          candidate.logicalLibrary,
          candidate.tapePool,
          candidate.vo,
          vid};
}

MountCandidate makeBlockedCandidate(const MountCandidate& candidate, const std::string& reason) {
  auto blockedCandidate = candidate;
  blockedCandidate.state = "Blocked";
  blockedCandidate.stateReason = reason;
  return blockedCandidate;
}

MountCandidate mountCandidateFromRset(const rdbms::Rset& rset) {
  MountCandidate candidate;
  candidate.candidateKey = rset.columnString("CANDIDATE_KEY");
  candidate.mountType = common::dataStructures::strToMountType(rset.columnString("MOUNT_TYPE"));
  candidate.logicalLibrary = rset.columnString("LOGICAL_LIBRARY");
  candidate.tapePool = rset.columnString("TAPE_POOL");
  candidate.vo = rset.columnString("VO");
  candidate.vid = rset.columnOptionalString("VID");
  candidate.activity = rset.columnOptionalString("ACTIVITY");
  candidate.priority = rset.columnUint64("PRIORITY");
  candidate.minRequestAge = rset.columnUint64("MIN_REQUEST_AGE");
  candidate.filesQueued = rset.columnUint64("FILES_QUEUED");
  candidate.bytesQueued = rset.columnUint64("BYTES_QUEUED");
  candidate.oldestJobStartTime = rset.columnUint64("OLDEST_JOB_START_TIME");
  candidate.youngestJobStartTime = rset.columnUint64("YOUNGEST_JOB_START_TIME");
  candidate.ratioOfMountQuotaUsed = rset.columnDouble("RATIO_OF_MOUNT_QUOTA_USED");
  candidate.candidateScore = rset.columnUint64("CANDIDATE_SCORE");
  candidate.overrideCandidateScore = rset.columnOptionalUint64("OVERRIDE_CANDIDATE_SCORE");
  candidate.mediaType = rset.columnOptionalString("MEDIA_TYPE").value_or("");
  candidate.labelFormat = rset.columnOptionalUint64("LABEL_FORMAT").value_or(0);
  candidate.vendor = rset.columnOptionalString("VENDOR").value_or("");
  candidate.capacityInBytes = rset.columnOptionalUint64("CAPACITY_IN_BYTES").value_or(0);
  candidate.lastFSeq = rset.columnOptionalUint64("LAST_FSEQ");
  candidate.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
  candidate.state = rset.columnString("STATE");
  candidate.stateReason = rset.columnOptionalString("STATE_REASON");
  candidate.createdBy = rset.columnOptionalString("CREATED_BY");
  return candidate;
}

MountCandidateRecord mountCandidateRecordFromRset(const rdbms::Rset& rset) {
  MountCandidateRecord record;
  record.candidateId = rset.columnUint64("CANDIDATE_ID");
  record.candidate = mountCandidateFromRset(rset);
  record.reservedByHost = rset.columnOptionalString("RESERVED_BY_HOST");
  record.reservedByDrive = rset.columnOptionalString("RESERVED_BY_DRIVE");
  record.reservedTime = rset.columnOptionalUint64("RESERVED_TIME");
  record.reservationHeartbeatTime = rset.columnOptionalUint64("RESERVATION_HEARTBEAT_TIME");
  record.creationTime = rset.columnUint64("CREATION_TIME");
  record.lastUpdateTime = rset.columnUint64("LAST_UPDATE_TIME");
  return record;
}

ReservationSlotCounts getLiveReservationSlotCounts(rdbms::Conn& conn, const uint64_t reservationTimeoutSeconds) {
  const char* const sql = R"SQL(
    SELECT
      MOUNT_TYPE AS MOUNT_TYPE,
      LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
      TAPE_POOL AS TAPE_POOL,
      VO AS VO,
      VID AS VID,
      COUNT(*) AS RESERVED_COUNT
    FROM
      SCHEDULER_MOUNT_CANDIDATES
    WHERE
      STATE = 'Reserved'
      AND COALESCE(RESERVATION_HEARTBEAT_TIME, RESERVED_TIME, 0)
          >= EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER - :RESERVATION_TIMEOUT_SECONDS
    GROUP BY
      MOUNT_TYPE,
      LOGICAL_LIBRARY,
      TAPE_POOL,
      VO,
      VID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":RESERVATION_TIMEOUT_SECONDS", reservationTimeoutSeconds);
  auto rset = stmt.executeQuery();

  ReservationSlotCounts ret;
  while (rset.next()) {
    MountCandidate reservedCandidate;
    reservedCandidate.mountType = common::dataStructures::strToMountType(rset.columnString("MOUNT_TYPE"));
    reservedCandidate.logicalLibrary = rset.columnString("LOGICAL_LIBRARY");
    reservedCandidate.tapePool = rset.columnString("TAPE_POOL");
    reservedCandidate.vo = rset.columnString("VO");
    reservedCandidate.vid = rset.columnOptionalString("VID");
    ret[getReservationSlotKey(reservedCandidate)] += rset.columnUint64("RESERVED_COUNT");
  }
  return ret;
}

std::set<std::string, std::less<>> getLiveReservedCandidateKeys(rdbms::Conn& conn,
                                                                const uint64_t reservationTimeoutSeconds) {
  const char* const sql = R"SQL(
    SELECT
      CANDIDATE_KEY AS CANDIDATE_KEY
    FROM
      SCHEDULER_MOUNT_CANDIDATES
    WHERE
      STATE = 'Reserved'
      AND COALESCE(RESERVATION_HEARTBEAT_TIME, RESERVED_TIME, 0)
          >= EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER - :RESERVATION_TIMEOUT_SECONDS
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":RESERVATION_TIMEOUT_SECONDS", reservationTimeoutSeconds);
  auto rset = stmt.executeQuery();

  std::set<std::string, std::less<>> ret;
  while (rset.next()) {
    ret.insert(rset.columnString("CANDIDATE_KEY"));
  }
  return ret;
}

bool hasLiveReservationSlot(const ReservationSlotCounts& liveReservationSlotCounts, const MountCandidate& candidate) {
  return liveReservationSlotCounts.contains(getReservationSlotKey(candidate));
}

bool tryMatchReservedSlot(ReservationSlotCounts& unmatchedReservationSlotCounts, const MountCandidate& candidate) {
  auto it = unmatchedReservationSlotCounts.find(getReservationSlotKey(candidate));
  if (it == unmatchedReservationSlotCounts.end() || it->second == 0) {
    return false;
  }
  it->second--;
  return true;
}

bool shouldSkipBlockedMountCandidate(const ReservationSlotCounts& liveReservationSlotCounts,
                                     const MountCandidate& candidate) {
  // Blocked archive rows without a VID describe capacity for a tape pool, not a
  // concrete tape. If a live reservation already owns that same group, keeping
  // another blocked row would add noise without giving taped a reservable row.
  if (candidate.state != "Blocked" || candidate.vid.has_value()) {
    return false;
  }
  if (cta::common::dataStructures::getMountBasicType(candidate.mountType)
      != cta::common::dataStructures::MountType::ArchiveAllTypes) {
    return false;
  }
  return hasLiveReservationSlot(liveReservationSlotCounts, candidate);
}

}  // namespace

MountDecisionDB::MountDecisionDB(ConnProvider& connectionProvider) : m_connectionProvider(connectionProvider) {}

std::optional<MountDecisionDB> makeMountDecisionDB(SchedulerDatabase& schedulerDb) {
  auto* connProvider = dynamic_cast<cta::ConnProvider*>(&schedulerDb);
  if (connProvider == nullptr) {
    return std::nullopt;
  }
  return MountDecisionDB(*connProvider);
}

void MountDecisionDB::ping() {
  auto conn = m_connectionProvider.getConn();
  const auto tableNames = conn.getTableNames();
  for (const auto& tableName : tableNames) {
    if (tableName == "MOUNT_DECISION_KV") {
      return;
    }
  }
  throw exception::Exception("Did not find MOUNT_DECISION_KV table in the Mount Decision DB.");
}

void MountDecisionDB::setValue(const std::string& key, const std::string& value) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    INSERT INTO MOUNT_DECISION_KV(
      KEY_NAME,
      VALUE
    ) VALUES (
      :KEY_NAME,
      :VALUE
    )
    ON CONFLICT(KEY_NAME) DO UPDATE SET
      VALUE = EXCLUDED.VALUE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  stmt.bindString(":VALUE", value);
  stmt.executeNonQuery();
}

std::optional<std::string> MountDecisionDB::getValue(const std::string& key) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    SELECT
      VALUE AS VALUE
    FROM
      MOUNT_DECISION_KV
    WHERE
      KEY_NAME = :KEY_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    return std::nullopt;
  }
  return rset.columnString("VALUE");
}

void MountDecisionDB::incrementCounter(const std::string& key) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    INSERT INTO MOUNT_DECISION_KV(
      KEY_NAME,
      VALUE
    ) VALUES (
      :KEY_NAME,
      '1'
    )
    ON CONFLICT(KEY_NAME) DO UPDATE SET
      VALUE = CAST((CAST(MOUNT_DECISION_KV.VALUE AS BIGINT) + 1) AS VARCHAR(255))
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":KEY_NAME", key);
  stmt.executeNonQuery();
}

bool MountDecisionDB::tryAcquireRefreshLock(const std::string& workKey,
                                            const std::string& host,
                                            uint64_t leaseSeconds) {
  auto conn = m_connectionProvider.getConn();
  // Maintd refresh is a single-writer operation, for now.
  // A maintd process can acquire the lock in order to execute the refresh operation.
  // This works when the lock does not exist yet, when the previous lease has expired,
  // or when the same host already owns the lock and is renewing it.
  // TODO: Allow maintd to analyse each mount and give it a score, row-by-row.
  //       This will allow rows to be processed in parallel and remove the need for this lock.
  //       At the moment, the lock is still used because refresh rebuilds the candidate table as a single operation.
  const char* const sql = R"SQL(
    INSERT INTO SCHEDULER_MOUNT_WORK_LOCK(
      WORK_KEY,
      LOCKED_BY_HOST,
      LOCK_EXPIRES_TIME
    ) VALUES (
      :WORK_KEY,
      :LOCKED_BY_HOST,
      EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER + :LEASE_SECONDS
    )
    ON CONFLICT(WORK_KEY) DO UPDATE SET
      LOCKED_BY_HOST = EXCLUDED.LOCKED_BY_HOST,
      LOCKED_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LOCK_HEARTBEAT_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LOCK_EXPIRES_TIME = EXCLUDED.LOCK_EXPIRES_TIME
    WHERE
      SCHEDULER_MOUNT_WORK_LOCK.LOCK_EXPIRES_TIME < EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      OR SCHEDULER_MOUNT_WORK_LOCK.LOCKED_BY_HOST = EXCLUDED.LOCKED_BY_HOST
    RETURNING WORK_KEY
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":WORK_KEY", workKey);
  stmt.bindString(":LOCKED_BY_HOST", host);
  stmt.bindUint64(":LEASE_SECONDS", leaseSeconds);
  auto rset = stmt.executeQuery();
  return rset.next();
}

std::vector<MountCandidateRecord>
MountDecisionDB::blockExpiredReservedMountCandidates(const uint64_t reservationTimeoutSeconds) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    WITH expired AS (
      SELECT
        *
      FROM
        SCHEDULER_MOUNT_CANDIDATES
      WHERE
        STATE = 'Reserved'
        AND COALESCE(RESERVATION_HEARTBEAT_TIME, RESERVED_TIME, 0)
            < EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER - :RESERVATION_TIMEOUT_SECONDS
      FOR UPDATE
    )
    UPDATE SCHEDULER_MOUNT_CANDIDATES candidate SET
      STATE = 'Blocked',
      STATE_REASON = 'Reservation timed out',
      RESERVED_BY_HOST = NULL,
      RESERVED_BY_DRIVE = NULL,
      RESERVED_TIME = NULL,
      RESERVATION_HEARTBEAT_TIME = NULL,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
    FROM expired
    WHERE candidate.CANDIDATE_ID = expired.CANDIDATE_ID
    RETURNING
      candidate.CANDIDATE_ID,
      candidate.CANDIDATE_KEY,
      candidate.MOUNT_TYPE,
      candidate.LOGICAL_LIBRARY,
      candidate.TAPE_POOL,
      candidate.VO,
      candidate.VID,
      candidate.ACTIVITY,
      candidate.PRIORITY,
      candidate.MIN_REQUEST_AGE,
      candidate.FILES_QUEUED,
      candidate.BYTES_QUEUED,
      candidate.OLDEST_JOB_START_TIME,
      candidate.YOUNGEST_JOB_START_TIME,
      candidate.RATIO_OF_MOUNT_QUOTA_USED,
      candidate.CANDIDATE_SCORE,
      candidate.OVERRIDE_CANDIDATE_SCORE,
      candidate.MEDIA_TYPE,
      candidate.LABEL_FORMAT,
      candidate.VENDOR,
      candidate.CAPACITY_IN_BYTES,
      candidate.LAST_FSEQ,
      candidate.ENCRYPTION_KEY_NAME,
      candidate.STATE,
      candidate.STATE_REASON,
      expired.RESERVED_BY_HOST AS RESERVED_BY_HOST,
      expired.RESERVED_BY_DRIVE AS RESERVED_BY_DRIVE,
      expired.RESERVED_TIME AS RESERVED_TIME,
      expired.RESERVATION_HEARTBEAT_TIME AS RESERVATION_HEARTBEAT_TIME,
      candidate.CREATED_BY,
      candidate.CREATION_TIME,
      candidate.LAST_UPDATE_TIME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":RESERVATION_TIMEOUT_SECONDS", reservationTimeoutSeconds);
  auto rset = stmt.executeQuery();

  std::vector<MountCandidateRecord> ret;
  while (rset.next()) {
    ret.push_back(mountCandidateRecordFromRset(rset));
  }
  return ret;
}

void MountDecisionDB::replaceMountCandidates(const std::vector<MountCandidate>& candidates,
                                             const uint64_t reservationTimeoutSeconds) {
  auto conn = m_connectionProvider.getConn();
  const char* const refreshIdSql = R"SQL(
    SELECT NEXTVAL('SCHEDULER_MOUNT_CANDIDATES_REFRESH_ID_SEQ') AS REFRESH_ID
  )SQL";
  uint64_t refreshId = 0;
  {
    auto refreshIdStmt = conn.createStmt(refreshIdSql);
    auto refreshIdRset = refreshIdStmt.executeQuery();
    if (!refreshIdRset.next()) {
      throw exception::Exception("In MountDecisionDB::replaceMountCandidates(): failed to allocate refresh id.");
    }
    refreshId = refreshIdRset.columnUint64("REFRESH_ID");
  }

  try {
    conn.executeNonQuery("BEGIN");

    const char* const upsertSql = R"SQL(
      INSERT INTO SCHEDULER_MOUNT_CANDIDATES(
        CANDIDATE_KEY,
        MOUNT_TYPE,
        LOGICAL_LIBRARY,
        TAPE_POOL,
        VO,
        VID,
        ACTIVITY,
        PRIORITY,
        MIN_REQUEST_AGE,
        FILES_QUEUED,
        BYTES_QUEUED,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RATIO_OF_MOUNT_QUOTA_USED,
        CANDIDATE_SCORE,
        OVERRIDE_CANDIDATE_SCORE,
        MEDIA_TYPE,
        LABEL_FORMAT,
        VENDOR,
        CAPACITY_IN_BYTES,
        LAST_FSEQ,
        ENCRYPTION_KEY_NAME,
        STATE,
        STATE_REASON,
        CREATED_BY,
        LAST_SEEN_REFRESH_ID
      ) VALUES (
        :CANDIDATE_KEY,
        :MOUNT_TYPE,
        :LOGICAL_LIBRARY,
        :TAPE_POOL,
        :VO,
        :VID,
        :ACTIVITY,
        :PRIORITY,
        :MIN_REQUEST_AGE,
        :FILES_QUEUED,
        :BYTES_QUEUED,
        :OLDEST_JOB_START_TIME,
        :YOUNGEST_JOB_START_TIME,
        :RATIO_OF_MOUNT_QUOTA_USED,
        :CANDIDATE_SCORE,
        :OVERRIDE_CANDIDATE_SCORE,
        :MEDIA_TYPE,
        :LABEL_FORMAT,
        :VENDOR,
        :CAPACITY_IN_BYTES,
        :LAST_FSEQ,
        :ENCRYPTION_KEY_NAME,
        :STATE,
        :STATE_REASON,
        :CREATED_BY,
        :LAST_SEEN_REFRESH_ID
      )
      ON CONFLICT (CANDIDATE_KEY) DO UPDATE SET
        MOUNT_TYPE = EXCLUDED.MOUNT_TYPE,
        LOGICAL_LIBRARY = EXCLUDED.LOGICAL_LIBRARY,
        TAPE_POOL = EXCLUDED.TAPE_POOL,
        VO = EXCLUDED.VO,
        VID = EXCLUDED.VID,
        ACTIVITY = EXCLUDED.ACTIVITY,
        PRIORITY = EXCLUDED.PRIORITY,
        MIN_REQUEST_AGE = EXCLUDED.MIN_REQUEST_AGE,
        FILES_QUEUED = EXCLUDED.FILES_QUEUED,
        BYTES_QUEUED = EXCLUDED.BYTES_QUEUED,
        OLDEST_JOB_START_TIME = EXCLUDED.OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME = EXCLUDED.YOUNGEST_JOB_START_TIME,
        RATIO_OF_MOUNT_QUOTA_USED = EXCLUDED.RATIO_OF_MOUNT_QUOTA_USED,
        CANDIDATE_SCORE = EXCLUDED.CANDIDATE_SCORE,
        MEDIA_TYPE = EXCLUDED.MEDIA_TYPE,
        LABEL_FORMAT = EXCLUDED.LABEL_FORMAT,
        VENDOR = EXCLUDED.VENDOR,
        CAPACITY_IN_BYTES = EXCLUDED.CAPACITY_IN_BYTES,
        LAST_FSEQ = EXCLUDED.LAST_FSEQ,
        ENCRYPTION_KEY_NAME = EXCLUDED.ENCRYPTION_KEY_NAME,
        STATE = CASE
          WHEN SCHEDULER_MOUNT_CANDIDATES.STATE = 'Reserved' THEN SCHEDULER_MOUNT_CANDIDATES.STATE
          ELSE EXCLUDED.STATE
        END,
        STATE_REASON = CASE
          WHEN SCHEDULER_MOUNT_CANDIDATES.STATE = 'Reserved' THEN SCHEDULER_MOUNT_CANDIDATES.STATE_REASON
          ELSE EXCLUDED.STATE_REASON
        END,
        CREATED_BY = EXCLUDED.CREATED_BY,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
        LAST_SEEN_REFRESH_ID = EXCLUDED.LAST_SEEN_REFRESH_ID
    )SQL";

    const auto liveReservationSlotCounts = getLiveReservationSlotCounts(conn, reservationTimeoutSeconds);
    const auto liveReservedCandidateKeys = getLiveReservedCandidateKeys(conn, reservationTimeoutSeconds);
    auto unmatchedReservationSlotCounts = liveReservationSlotCounts;
    for (const auto& candidate : candidates) {
      const MountCandidate* candidateToInsert = &candidate;
      std::optional<MountCandidate> blockedCandidate;
      if (liveReservedCandidateKeys.contains(candidate.candidateKey)) {
        tryMatchReservedSlot(unmatchedReservationSlotCounts, candidate);
      } else if (candidate.state == "Available" && tryMatchReservedSlot(unmatchedReservationSlotCounts, candidate)) {
        // Live reservations consume scheduling slots. If the refreshed ordering
        // still contains more rows for this slot, only the surplus remains
        // Available.
        blockedCandidate = makeBlockedCandidate(candidate, "Reservation slot already reserved");
        candidateToInsert = &blockedCandidate.value();
      }

      if (shouldSkipBlockedMountCandidate(liveReservationSlotCounts, *candidateToInsert)) {
        continue;
      }

      auto stmt = conn.createStmt(upsertSql);
      bindCommonMountCandidateFields(stmt, *candidateToInsert);
      stmt.bindString(":STATE", candidateToInsert->state);
      stmt.bindString(":STATE_REASON", nonEmptyOptional(candidateToInsert->stateReason));
      stmt.bindUint64(":LAST_SEEN_REFRESH_ID", refreshId);
      stmt.executeNonQuery();
    }

    {
      const char* const deleteStaleSql = R"SQL(
        DELETE FROM SCHEDULER_MOUNT_CANDIDATES
        WHERE
          STATE <> 'Reserved'
          AND LAST_SEEN_REFRESH_ID <> :LAST_SEEN_REFRESH_ID
      )SQL";
      auto deleteStaleStmt = conn.createStmt(deleteStaleSql);
      deleteStaleStmt.bindUint64(":LAST_SEEN_REFRESH_ID", refreshId);
      deleteStaleStmt.executeNonQuery();
    }

    conn.commit();
  } catch (...) {
    conn.rollback();
    throw;
  }
}

bool MountDecisionDB::hasAvailableMountCandidate(const std::string& logicalLibrary) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    SELECT
      CANDIDATE_ID AS CANDIDATE_ID
    FROM
      SCHEDULER_MOUNT_CANDIDATES
    WHERE
      STATE = 'Available'
      AND LOGICAL_LIBRARY = :LOGICAL_LIBRARY
    LIMIT 1
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":LOGICAL_LIBRARY", logicalLibrary);
  auto rset = stmt.executeQuery();
  return rset.next();
}

std::vector<MountCandidateRecord> MountDecisionDB::listMountCandidates() {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    SELECT
      CANDIDATE_ID,
      CANDIDATE_KEY,
      MOUNT_TYPE,
      LOGICAL_LIBRARY,
      TAPE_POOL,
      VO,
      VID,
      ACTIVITY,
      PRIORITY,
      MIN_REQUEST_AGE,
      FILES_QUEUED,
      BYTES_QUEUED,
      OLDEST_JOB_START_TIME,
      YOUNGEST_JOB_START_TIME,
      RATIO_OF_MOUNT_QUOTA_USED,
      CANDIDATE_SCORE,
      OVERRIDE_CANDIDATE_SCORE,
      MEDIA_TYPE,
      LABEL_FORMAT,
      VENDOR,
      CAPACITY_IN_BYTES,
      LAST_FSEQ,
      ENCRYPTION_KEY_NAME,
      STATE,
      STATE_REASON,
      RESERVED_BY_HOST,
      RESERVED_BY_DRIVE,
      RESERVED_TIME,
      RESERVATION_HEARTBEAT_TIME,
      CREATED_BY,
      CREATION_TIME,
      LAST_UPDATE_TIME
    FROM
      SCHEDULER_MOUNT_CANDIDATES
    ORDER BY
      COALESCE(OVERRIDE_CANDIDATE_SCORE, CANDIDATE_SCORE) DESC,
      OLDEST_JOB_START_TIME,
      CANDIDATE_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::vector<MountCandidateRecord> ret;
  while (rset.next()) {
    ret.push_back(mountCandidateRecordFromRset(rset));
  }
  return ret;
}

void MountDecisionDB::setMountCandidateScoreOverride(const std::string& candidateKey,
                                                     std::optional<uint64_t> overrideCandidateScore) {
  auto conn = m_connectionProvider.getConn();
  try {
    conn.executeNonQuery("BEGIN");

    bool hasVid = false;
    {
      const char* const selectSql = R"SQL(
        SELECT
          VID AS VID
        FROM
          SCHEDULER_MOUNT_CANDIDATES
        WHERE
          CANDIDATE_KEY = :CANDIDATE_KEY
        FOR UPDATE
      )SQL";
      auto selectStmt = conn.createStmt(selectSql);
      selectStmt.bindString(":CANDIDATE_KEY", candidateKey);
      auto rset = selectStmt.executeQuery();
      if (!rset.next()) {
        throw exception::UserError("No mount candidate found with candidate key " + candidateKey);
      }
      hasVid = rset.columnOptionalString("VID").has_value();
    }

    if (!hasVid) {
      throw exception::UserError("Cannot override the score of mount candidate " + candidateKey
                                 + " because it does not identify a tape VID");
    }

    const char* const updateSql = R"SQL(
      UPDATE SCHEDULER_MOUNT_CANDIDATES SET
        OVERRIDE_CANDIDATE_SCORE = :OVERRIDE_CANDIDATE_SCORE,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      WHERE
        CANDIDATE_KEY = :CANDIDATE_KEY
    )SQL";
    auto updateStmt = conn.createStmt(updateSql);
    updateStmt.bindUint64(":OVERRIDE_CANDIDATE_SCORE", overrideCandidateScore);
    updateStmt.bindString(":CANDIDATE_KEY", candidateKey);
    updateStmt.executeNonQuery();

    conn.commit();
  } catch (...) {
    conn.rollback();
    throw;
  }
}

std::optional<ReservedMountCandidate> MountDecisionDB::tryReserveNextMountCandidate(const std::string& logicalLibrary,
                                                                                    const std::string& host,
                                                                                    const std::string& drive) {
  auto conn = m_connectionProvider.getConn();
  try {
    conn.executeNonQuery("BEGIN");

    // Tape servers race on this SELECT. FOR UPDATE SKIP LOCKED lets concurrent
    // callers ignore rows another transaction is already reserving. The
    // same-VID subquery prevents a worse sibling from being selected while the
    // best sibling is locked by another caller.
    const char* const selectSql = R"SQL(
      SELECT candidate.CANDIDATE_ID AS CANDIDATE_ID
      FROM SCHEDULER_MOUNT_CANDIDATES candidate
      WHERE
        candidate.STATE = 'Available'
        AND candidate.LOGICAL_LIBRARY = :LOGICAL_LIBRARY
        AND (
          candidate.VID IS NULL
          OR NOT EXISTS (
            SELECT 1
            FROM SCHEDULER_MOUNT_CANDIDATES reserved
            WHERE
              reserved.STATE = 'Reserved'
              AND reserved.VID = candidate.VID
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM SCHEDULER_MOUNT_CANDIDATES better
          WHERE
            better.STATE = 'Available'
            AND better.LOGICAL_LIBRARY = candidate.LOGICAL_LIBRARY
            AND better.VID = candidate.VID
            AND (
              COALESCE(better.OVERRIDE_CANDIDATE_SCORE, better.CANDIDATE_SCORE)
                > COALESCE(candidate.OVERRIDE_CANDIDATE_SCORE, candidate.CANDIDATE_SCORE)
              OR (
                COALESCE(better.OVERRIDE_CANDIDATE_SCORE, better.CANDIDATE_SCORE)
                  = COALESCE(candidate.OVERRIDE_CANDIDATE_SCORE, candidate.CANDIDATE_SCORE)
                AND better.OLDEST_JOB_START_TIME < candidate.OLDEST_JOB_START_TIME
              )
              OR (
                COALESCE(better.OVERRIDE_CANDIDATE_SCORE, better.CANDIDATE_SCORE)
                  = COALESCE(candidate.OVERRIDE_CANDIDATE_SCORE, candidate.CANDIDATE_SCORE)
                AND better.OLDEST_JOB_START_TIME = candidate.OLDEST_JOB_START_TIME
                AND better.CANDIDATE_ID < candidate.CANDIDATE_ID
              )
            )
        )
      ORDER BY
        COALESCE(candidate.OVERRIDE_CANDIDATE_SCORE, candidate.CANDIDATE_SCORE) DESC,
        candidate.OLDEST_JOB_START_TIME,
        candidate.CANDIDATE_ID
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )SQL";
    uint64_t candidateId = 0;
    {
      auto selectStmt = conn.createStmt(selectSql);
      selectStmt.bindString(":LOGICAL_LIBRARY", logicalLibrary);
      auto selected = selectStmt.executeQuery();
      if (!selected.next()) {
        conn.commit();
        return std::nullopt;
      }
      candidateId = selected.columnUint64("CANDIDATE_ID");
    }

    const char* const reserveSql = R"SQL(
      UPDATE SCHEDULER_MOUNT_CANDIDATES SET
        STATE = 'Reserved',
        STATE_REASON = NULL,
        RESERVED_BY_HOST = :RESERVED_BY_HOST,
        RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE,
        RESERVED_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
        RESERVATION_HEARTBEAT_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      WHERE CANDIDATE_ID = :CANDIDATE_ID
      RETURNING
        CANDIDATE_ID,
        CANDIDATE_KEY,
        MOUNT_TYPE,
        LOGICAL_LIBRARY,
        TAPE_POOL,
        VO,
        VID,
        ACTIVITY,
        PRIORITY,
        MIN_REQUEST_AGE,
        FILES_QUEUED,
        BYTES_QUEUED,
        OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME,
        RATIO_OF_MOUNT_QUOTA_USED,
        CANDIDATE_SCORE,
        OVERRIDE_CANDIDATE_SCORE,
        MEDIA_TYPE,
        LABEL_FORMAT,
        VENDOR,
        CAPACITY_IN_BYTES,
        LAST_FSEQ,
        ENCRYPTION_KEY_NAME,
        STATE,
        STATE_REASON,
        CREATED_BY
    )SQL";
    auto reserveStmt = conn.createStmt(reserveSql);
    reserveStmt.bindString(":RESERVED_BY_HOST", host);
    reserveStmt.bindString(":RESERVED_BY_DRIVE", drive);
    reserveStmt.bindUint64(":CANDIDATE_ID", candidateId);
    ReservedMountCandidate ret;
    {
      auto reserved = reserveStmt.executeQuery();
      if (!reserved.next()) {
        throw exception::Exception("In MountDecisionDB::tryReserveNextMountCandidate(): failed to reserve candidate.");
      }

      ret.candidateId = reserved.columnUint64("CANDIDATE_ID");
      ret.candidate = mountCandidateFromRset(reserved);
    }

    if (ret.candidate.vid.has_value()) {
      const char* const blockSiblingsSql = R"SQL(
        UPDATE SCHEDULER_MOUNT_CANDIDATES SET
          STATE = 'Blocked',
          STATE_REASON = 'Tape already reserved by another candidate',
          LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
        WHERE
          CANDIDATE_ID <> :CANDIDATE_ID
          AND STATE = 'Available'
          AND VID = :VID
      )SQL";
      auto blockSiblingsStmt = conn.createStmt(blockSiblingsSql);
      blockSiblingsStmt.bindUint64(":CANDIDATE_ID", ret.candidateId);
      blockSiblingsStmt.bindString(":VID", ret.candidate.vid);
      blockSiblingsStmt.executeNonQuery();
    }

    conn.commit();
    return ret;
  } catch (...) {
    conn.rollback();
    throw;
  }
}

void MountDecisionDB::blockReservedMountCandidate(const uint64_t candidateId,
                                                  const std::string& host,
                                                  const std::string& drive,
                                                  const std::string& reason) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    UPDATE SCHEDULER_MOUNT_CANDIDATES SET
      STATE = 'Blocked',
      STATE_REASON = :STATE_REASON,
      RESERVED_BY_HOST = NULL,
      RESERVED_BY_DRIVE = NULL,
      RESERVED_TIME = NULL,
      RESERVATION_HEARTBEAT_TIME = NULL,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
    WHERE
      CANDIDATE_ID = :CANDIDATE_ID
      AND STATE = 'Reserved'
      AND RESERVED_BY_HOST = :RESERVED_BY_HOST
      AND RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STATE_REASON", reason);
  stmt.bindUint64(":CANDIDATE_ID", candidateId);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.executeNonQuery();
}

void MountDecisionDB::releaseMountCandidate(const uint64_t candidateId,
                                            const std::string& host,
                                            const std::string& drive) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    DELETE FROM SCHEDULER_MOUNT_CANDIDATES
    WHERE
      CANDIDATE_ID = :CANDIDATE_ID
      AND STATE = 'Reserved'
      AND RESERVED_BY_HOST = :RESERVED_BY_HOST
      AND RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":CANDIDATE_ID", candidateId);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.executeNonQuery();
}

void MountDecisionDB::heartbeatMountCandidate(const uint64_t candidateId,
                                              const std::string& host,
                                              const std::string& drive) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    UPDATE SCHEDULER_MOUNT_CANDIDATES SET
      RESERVATION_HEARTBEAT_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
    WHERE
      CANDIDATE_ID = :CANDIDATE_ID
      AND STATE = 'Reserved'
      AND RESERVED_BY_HOST = :RESERVED_BY_HOST
      AND RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":CANDIDATE_ID", candidateId);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.executeNonQuery();
}

}  // namespace cta::mountdecision
