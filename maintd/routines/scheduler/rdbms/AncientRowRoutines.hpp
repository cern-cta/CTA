/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "rdbms/Conn.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

#include <unordered_map>

namespace cta::maintd {

/**
 * @brief Periodic routine that deletes stale jobs from failed queue tables.
 *
 * Removes jobs that have remained in archive/retrieve/repack failed queues
 * longer than the configured inactivity limit (e.g. two weeks).
 */
class DeleteOldFailedQueuesRoutine final : public IRoutine {
public:
  std::string getName() const final { return m_routineName; };

  void execute();

  virtual ~DeleteOldFailedQueuesRoutine() = default;

  DeleteOldFailedQueuesRoutine(log::LogContext& lc, RelationalDB& pgs, size_t batchSize, uint64_t inactiveTimeLimit);

private:
  cta::log::LogContext& m_lc;
  cta::RelationalDB& m_RelationalDB;
  size_t m_batchSize;
  const std::string m_routineName = "DeleteOldFailedQueuesRoutine";
  uint64_t m_inactiveTimeLimit;
};

/**
 * @brief Periodic routine that cleans stale mount last-fetch tracking entries.
 *
 * Deletes entries from the MOUNT_QUEUE_LAST_FETCH table for mounts that
 * have been inactive for a very long time (e.g. several weeks), exceeding
 * the inactivity limits used by inactive mount cleanup routines.
 */
class CleanMountLastFetchTimeRoutine : public IRoutine {
public:
  std::string getName() const final { return m_routineName; };

  void execute();

  virtual ~CleanMountLastFetchTimeRoutine() = default;

  CleanMountLastFetchTimeRoutine(log::LogContext& lc, RelationalDB& pgs, size_t batchSize, uint64_t inactiveTimeLimit);

private:
  cta::log::LogContext& m_lc;
  cta::RelationalDB& m_RelationalDB;
  size_t m_batchSize;
  const std::string m_routineName = "CleanMountLastFetchTimeRoutine";
  uint64_t m_inactiveTimeLimit;
};

}  // namespace cta::maintd
