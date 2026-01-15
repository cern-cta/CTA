/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "rdbms/Conn.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

#include <unordered_map>

namespace cta::maintd {

/**
 * @brief Base class for routines handling inactive (dead) mount queues.
 *
 * Provides common helpers and state for routines that clean up scheduler
 * queues associated with mounts that have been inactive for longer than
 * a configured time limit. Derived classes specialize by queue type
 * (archive/retrieve/repack_active/pending).
 *
 * The routine logic typically:
 *  - Identifies dead mounts using the scheduler DB and catalogue
 *  - Cleans up ACTIVE or PENDING queues for those mounts
 *  - Removes corresponding entries from the MOUNT_QUEUE_LAST_FETCH table
 *    if no more cleanup needs to be done
 */
class InactiveMountQueueRoutineBase : public IRoutine {
public:
  /**
   * @brief Returns the routine name.
   */
  std::string getName() const final { return m_routineName; };

  /**
   * @brief Selects the appropriate dead mount ID vector for a given queue type.
   *
   * Extracts and returns a reference to the vector of dead mount IDs matching
   * the requested archive/retrieve/repack_pending/active combination.
   *
   * @param deadMounts  Dead mount IDs grouped by queue type.
   * @param isArchive   True for archive queues, false for retrieve queues.
   * @param isRepack    True for repack queues.
   * @param isPending   True for pending queues, false for active queues.
   *
   * @return Reference to the corresponding vector of dead mount IDs.
   */
  std::vector<uint64_t>& getDeadMountVector(cta::common::dataStructures::DeadMountCandidateIDs& deadMounts,
                                            bool isArchive,
                                            bool isRepack,
                                            bool isPending);

  /**
   * @brief Handles cleanup of ACTIVE queues for inactive mounts.
   *
   * Requeues jobs owned by inactive mounts from ACTIVE queues back to the
   * corresponding PENDING queues. Only jobs that have not entered the
   * reporting phase are requeued.
   *
   * @param isArchive  True for archive queues, false for retrieve queues.
   * @param isRepack   True for repack queues.
   */
  void handleInactiveMountActiveQueueRoutine(bool isArchive, bool isRepack);

  /**
   * @brief Handles cleanup of PENDING queues for inactive mounts.
   *
   * Clears mount assignments for jobs in PENDING queues that still reference
   * inactive mounts, allowing them to be rescheduled to new mounts.
   *
   * @param isArchive  True for archive queues, false for retrieve queues.
   * @param isRepack   True for repack queues.
   */
  void handleInactiveMountPendingQueueRoutine(bool isArchive, bool isRepack);

  virtual ~InactiveMountQueueRoutineBase() = default;

protected:
  /**
   * @brief Constructs a base inactive-mount queue cleanup routine.
   *
   * @param lc                 Logging context.
   * @param catalogue          CTA catalogue interface.
   * @param pgs                Scheduler relational database interface.
   * @param batchSize          Maximum number of jobs processed per iteration.
   * @param routineName        Name of the routine.
   * @param inactiveTimeLimit  Inactivity threshold in seconds.
   */
  InactiveMountQueueRoutineBase(log::LogContext& lc,
                                catalogue::Catalogue& catalogue,
                                RelationalDB& pgs,
                                size_t batchSize,
                                const std::string& routineName,
                                uint64_t inactiveTimeLimit);

  cta::log::LogContext& m_lc;
  catalogue::Catalogue& m_catalogue;
  cta::RelationalDB& m_RelationalDB;
  size_t m_batchSize;
  std::string m_routineName;
  uint64_t m_inactiveTimeLimit;
};

class ArchiveInactiveMountActiveQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  ArchiveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                         catalogue::Catalogue& catalogue,
                                         RelationalDB& pgs,
                                         size_t batchSize,
                                         uint64_t inactiveTimeLimit);

  void execute();
};

class RetrieveInactiveMountActiveQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RetrieveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                          catalogue::Catalogue& catalogue,
                                          RelationalDB& pgs,
                                          size_t batchSize,
                                          uint64_t inactiveTimeLimit);

  void execute();
};

class RepackRetrieveInactiveMountActiveQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RepackRetrieveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                                catalogue::Catalogue& catalogue,
                                                RelationalDB& pgs,
                                                size_t batchSize,
                                                uint64_t inactiveTimeLimit);

  void execute();
};

class RepackArchiveInactiveMountActiveQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RepackArchiveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                               catalogue::Catalogue& catalogue,
                                               RelationalDB& pgs,
                                               size_t batchSize,
                                               uint64_t inactiveTimeLimit);

  void execute();
};

class ArchiveInactiveMountPendingQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  ArchiveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                          catalogue::Catalogue& catalogue,
                                          RelationalDB& pgs,
                                          size_t batchSize,
                                          uint64_t inactiveTimeLimit);

  void execute();
};

class RetrieveInactiveMountPendingQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RetrieveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                           catalogue::Catalogue& catalogue,
                                           RelationalDB& pgs,
                                           size_t batchSize,
                                           uint64_t inactiveTimeLimit);

  void execute();
};

class RepackRetrieveInactiveMountPendingQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RepackRetrieveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                 catalogue::Catalogue& catalogue,
                                                 RelationalDB& pgs,
                                                 size_t batchSize,
                                                 uint64_t inactiveTimeLimit);

  void execute();
};

class RepackArchiveInactiveMountPendingQueueRoutine : public InactiveMountQueueRoutineBase {
public:
  RepackArchiveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                catalogue::Catalogue& catalogue,
                                                RelationalDB& pgs,
                                                size_t batchSize,
                                                uint64_t inactiveTimeLimit);

  void execute();
};

}  // namespace cta::maintd
