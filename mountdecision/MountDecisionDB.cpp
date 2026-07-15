/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecisionDB.hpp"

#include "common/exception/Exception.hpp"
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
  stmt.bindUint64(":CANDIDATE_RANK", candidate.candidateRank);
  stmt.bindString(":MEDIA_TYPE", candidate.mediaType);
  stmt.bindUint64(":LABEL_FORMAT", candidate.labelFormat);
  stmt.bindString(":VENDOR", candidate.vendor);
  stmt.bindUint64(":CAPACITY_IN_BYTES", candidate.capacityInBytes);
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
  candidate.candidateRank = rset.columnUint64("CANDIDATE_RANK");
  candidate.mediaType = rset.columnString("MEDIA_TYPE");
  candidate.labelFormat = rset.columnUint64("LABEL_FORMAT");
  candidate.vendor = rset.columnString("VENDOR");
  candidate.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
  candidate.lastFSeq = rset.columnOptionalUint64("LAST_FSEQ");
  candidate.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
  candidate.state = rset.columnString("STATE");
  candidate.stateReason = rset.columnOptionalString("STATE_REASON");
  candidate.createdBy = rset.columnOptionalString("CREATED_BY");
  return candidate;
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
                                            std::optional<uint64_t> pid,
                                            uint64_t leaseSeconds) {
  auto conn = m_connectionProvider.getConn();
  // Maintd refresh is a single-writer operation, for now.
  // A maintd process can acquire the lock in order to execute the refresh operation.
  // This works when the lock does not exist yet, when the previous lease has expired,
  // or when the same process already owns the lock and is renewing it.
  // TODO: Allow maintd to analyse each mount and give it a score, row-by-row.
  //       This will allow rows to be processed in parallel and remove the need for this lock.
  //       At the moment, the lock is needed because we are assigning the score based on the position of the mount in
  //       the ordered list.
  const char* const sql = R"SQL(
    INSERT INTO SCHEDULER_MOUNT_WORK_LOCK(
      WORK_KEY,
      LOCKED_BY_HOST,
      LOCKED_BY_PID,
      LOCK_EXPIRES_TIME
    ) VALUES (
      :WORK_KEY,
      :LOCKED_BY_HOST,
      :LOCKED_BY_PID,
      EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER + :LEASE_SECONDS
    )
    ON CONFLICT(WORK_KEY) DO UPDATE SET
      LOCKED_BY_HOST = EXCLUDED.LOCKED_BY_HOST,
      LOCKED_BY_PID = EXCLUDED.LOCKED_BY_PID,
      LOCKED_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LOCK_HEARTBEAT_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LOCK_EXPIRES_TIME = EXCLUDED.LOCK_EXPIRES_TIME
    WHERE
      SCHEDULER_MOUNT_WORK_LOCK.LOCK_EXPIRES_TIME < EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      OR (
        SCHEDULER_MOUNT_WORK_LOCK.LOCKED_BY_HOST = EXCLUDED.LOCKED_BY_HOST
        AND (
          (EXCLUDED.LOCKED_BY_PID IS NULL AND SCHEDULER_MOUNT_WORK_LOCK.LOCKED_BY_PID IS NULL)
          OR SCHEDULER_MOUNT_WORK_LOCK.LOCKED_BY_PID = EXCLUDED.LOCKED_BY_PID
        )
      )
    RETURNING WORK_KEY
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":WORK_KEY", workKey);
  stmt.bindString(":LOCKED_BY_HOST", host);
  stmt.bindUint64(":LOCKED_BY_PID", pid);
  stmt.bindUint64(":LEASE_SECONDS", leaseSeconds);
  auto rset = stmt.executeQuery();
  return rset.next();
}

void MountDecisionDB::releaseRefreshLock(const std::string& workKey,
                                         const std::string& host,
                                         std::optional<uint64_t> pid) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    DELETE FROM SCHEDULER_MOUNT_WORK_LOCK
    WHERE
      WORK_KEY = :WORK_KEY
      AND LOCKED_BY_HOST = :LOCKED_BY_HOST
      AND ((:LOCKED_BY_PID::bigint IS NULL AND LOCKED_BY_PID IS NULL) OR LOCKED_BY_PID = :LOCKED_BY_PID::bigint)
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":WORK_KEY", workKey);
  stmt.bindString(":LOCKED_BY_HOST", host);
  stmt.bindUint64(":LOCKED_BY_PID", pid);
  stmt.executeNonQuery();
}

void MountDecisionDB::replaceMountCandidates(const std::vector<MountCandidate>& candidates,
                                             const uint64_t reservationTimeoutSeconds) {
  auto conn = m_connectionProvider.getConn();
  try {
    conn.executeNonQuery("BEGIN");
    {
      // Rebuild the table each refresh, but leave live Reserved rows in place.
      // Expired reservations are treated like normal stale rows and can be
      // replaced by the new candidate list.
      const char* const sql = R"SQL(
        DELETE FROM SCHEDULER_MOUNT_CANDIDATES
        WHERE
          STATE <> 'Reserved'
          OR COALESCE(RESERVATION_HEARTBEAT_TIME, RESERVED_TIME, 0)
             < EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER - :RESERVATION_TIMEOUT_SECONDS
      )SQL";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":RESERVATION_TIMEOUT_SECONDS", reservationTimeoutSeconds);
      stmt.executeNonQuery();
    }

    const char* const updateReservedSql = R"SQL(
      UPDATE SCHEDULER_MOUNT_CANDIDATES SET
        MOUNT_TYPE = :MOUNT_TYPE,
        LOGICAL_LIBRARY = :LOGICAL_LIBRARY,
        TAPE_POOL = :TAPE_POOL,
        VO = :VO,
        ACTIVITY = :ACTIVITY,
        PRIORITY = :PRIORITY,
        MIN_REQUEST_AGE = :MIN_REQUEST_AGE,
        FILES_QUEUED = :FILES_QUEUED,
        BYTES_QUEUED = :BYTES_QUEUED,
        OLDEST_JOB_START_TIME = :OLDEST_JOB_START_TIME,
        YOUNGEST_JOB_START_TIME = :YOUNGEST_JOB_START_TIME,
        RATIO_OF_MOUNT_QUOTA_USED = :RATIO_OF_MOUNT_QUOTA_USED,
        CANDIDATE_RANK = :CANDIDATE_RANK,
        MEDIA_TYPE = :MEDIA_TYPE,
        LABEL_FORMAT = :LABEL_FORMAT,
        VENDOR = :VENDOR,
        CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES,
        LAST_FSEQ = :LAST_FSEQ,
        ENCRYPTION_KEY_NAME = :ENCRYPTION_KEY_NAME,
        CREATED_BY = :CREATED_BY,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
      WHERE
        STATE = 'Reserved'
        AND VID = :VID
        AND COALESCE(RESERVATION_HEARTBEAT_TIME, RESERVED_TIME, 0)
            >= EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER - :RESERVATION_TIMEOUT_SECONDS
    )SQL";

    const char* const insertSql = R"SQL(
      INSERT INTO SCHEDULER_MOUNT_CANDIDATES(
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
        CANDIDATE_RANK,
        MEDIA_TYPE,
        LABEL_FORMAT,
        VENDOR,
        CAPACITY_IN_BYTES,
        LAST_FSEQ,
        ENCRYPTION_KEY_NAME,
        STATE,
        STATE_REASON,
        CREATED_BY
      ) VALUES (
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
        :CANDIDATE_RANK,
        :MEDIA_TYPE,
        :LABEL_FORMAT,
        :VENDOR,
        :CAPACITY_IN_BYTES,
        :LAST_FSEQ,
        :ENCRYPTION_KEY_NAME,
        :STATE,
        :STATE_REASON,
        :CREATED_BY
      )
    )SQL";

    std::set<std::string, std::less<>> updatedReservedVids;
    const auto liveReservationSlotCounts = getLiveReservationSlotCounts(conn, reservationTimeoutSeconds);
    auto unmatchedReservationSlotCounts = liveReservationSlotCounts;
    for (const auto& candidate : candidates) {
      if (candidate.vid.has_value() && !updatedReservedVids.contains(candidate.vid.value())) {
        // Preserve taped ownership for a live Reserved row. The scheduling
        // fields and rank are refreshed, while STATE and reservation metadata
        // remain untouched.
        auto updateStmt = conn.createStmt(updateReservedSql);
        bindCommonMountCandidateFields(updateStmt, candidate);
        updateStmt.bindUint64(":RESERVATION_TIMEOUT_SECONDS", reservationTimeoutSeconds);
        updateStmt.executeNonQuery();
        if (updateStmt.getNbAffectedRows() > 0) {
          updatedReservedVids.insert(candidate.vid.value());
          tryMatchReservedSlot(unmatchedReservationSlotCounts, candidate);
          continue;
        }
      }

      const MountCandidate* candidateToInsert = &candidate;
      std::optional<MountCandidate> blockedCandidate;
      if (candidate.vid.has_value() && updatedReservedVids.contains(candidate.vid.value())
          && candidate.state != "Blocked") {
        // A refreshed reserved row already owns this VID. Any lower-ranked row
        // for the same tape is kept as Blocked so the table still explains why
        // it was not pickable.
        blockedCandidate = makeBlockedCandidate(candidate, "Tape already reserved by higher-ranked candidate");
        candidateToInsert = &blockedCandidate.value();
      } else if (candidate.state == "Available" && tryMatchReservedSlot(unmatchedReservationSlotCounts, candidate)) {
        // Live reservations consume scheduling slots. If the refreshed ranking
        // still contains more rows for this slot, only the surplus remains
        // Available.
        blockedCandidate = makeBlockedCandidate(candidate, "Reservation slot already reserved");
        candidateToInsert = &blockedCandidate.value();
      }

      if (shouldSkipBlockedMountCandidate(liveReservationSlotCounts, *candidateToInsert)) {
        continue;
      }

      auto stmt = conn.createStmt(insertSql);
      bindCommonMountCandidateFields(stmt, *candidateToInsert);
      stmt.bindString(":STATE", candidateToInsert->state);
      stmt.bindString(":STATE_REASON", nonEmptyOptional(candidateToInsert->stateReason));
      stmt.executeNonQuery();
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

std::optional<ReservedMountCandidate> MountDecisionDB::tryReserveNextMountCandidate(const std::string& logicalLibrary,
                                                                                    const std::string& host,
                                                                                    const std::string& drive,
                                                                                    std::optional<uint64_t> pid) {
  auto conn = m_connectionProvider.getConn();
  // Tape servers race on this single UPDATE. FOR UPDATE SKIP LOCKED lets
  // concurrent callers ignore rows another transaction is already reserving,
  // so each successful caller receives a distinct candidate.
  const char* const sql = R"SQL(
    UPDATE SCHEDULER_MOUNT_CANDIDATES SET
      STATE = 'Reserved',
      STATE_REASON = NULL,
      RESERVED_BY_HOST = :RESERVED_BY_HOST,
      RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE,
      RESERVED_BY_PID = :RESERVED_BY_PID,
      RESERVED_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      RESERVATION_HEARTBEAT_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER
    WHERE CANDIDATE_ID = (
      SELECT CANDIDATE_ID
      FROM SCHEDULER_MOUNT_CANDIDATES
      WHERE
        STATE = 'Available'
        AND LOGICAL_LIBRARY = :LOGICAL_LIBRARY
      ORDER BY CANDIDATE_RANK, CANDIDATE_ID
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING
      CANDIDATE_ID,
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
      CANDIDATE_RANK,
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
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.bindUint64(":RESERVED_BY_PID", pid);
  stmt.bindString(":LOGICAL_LIBRARY", logicalLibrary);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    return std::nullopt;
  }

  ReservedMountCandidate ret;
  ret.candidateId = rset.columnUint64("CANDIDATE_ID");
  ret.candidate = mountCandidateFromRset(rset);
  return ret;
}

void MountDecisionDB::releaseMountCandidate(const uint64_t candidateId,
                                            const std::string& host,
                                            const std::string& drive,
                                            std::optional<uint64_t> pid) {
  auto conn = m_connectionProvider.getConn();
  const char* const sql = R"SQL(
    DELETE FROM SCHEDULER_MOUNT_CANDIDATES
    WHERE
      CANDIDATE_ID = :CANDIDATE_ID
      AND STATE = 'Reserved'
      AND RESERVED_BY_HOST = :RESERVED_BY_HOST
      AND RESERVED_BY_DRIVE = :RESERVED_BY_DRIVE
      AND ((:RESERVED_BY_PID::bigint IS NULL AND RESERVED_BY_PID IS NULL) OR RESERVED_BY_PID = :RESERVED_BY_PID::bigint)
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":CANDIDATE_ID", candidateId);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.bindUint64(":RESERVED_BY_PID", pid);
  stmt.executeNonQuery();
}

void MountDecisionDB::heartbeatMountCandidate(const uint64_t candidateId,
                                              const std::string& host,
                                              const std::string& drive,
                                              std::optional<uint64_t> pid) {
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
      AND ((:RESERVED_BY_PID::bigint IS NULL AND RESERVED_BY_PID IS NULL) OR RESERVED_BY_PID = :RESERVED_BY_PID::bigint)
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":CANDIDATE_ID", candidateId);
  stmt.bindString(":RESERVED_BY_HOST", host);
  stmt.bindString(":RESERVED_BY_DRIVE", drive);
  stmt.bindUint64(":RESERVED_BY_PID", pid);
  stmt.executeNonQuery();
}

}  // namespace cta::mountdecision
