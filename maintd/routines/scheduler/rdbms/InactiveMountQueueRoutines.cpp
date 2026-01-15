/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "InactiveMountQueueRoutines.hpp"

namespace cta::maintd {

InactiveMountQueueRoutineBase::InactiveMountQueueRoutineBase(log::LogContext& lc,
                                                             catalogue::Catalogue& catalogue,
                                                             RelationalDB& pgs,
                                                             size_t batchSize,
                                                             const std::string& routineName,
                                                             uint64_t inactiveTimeLimit)
    : m_lc(lc),
      m_catalogue(catalogue),
      m_RelationalDB(pgs),
      m_batchSize(batchSize),
      m_routineName(routineName),
      m_inactiveTimeLimit(inactiveTimeLimit) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", m_batchSize);
  params.add("inactiveTimeLimit", m_inactiveTimeLimit);
  m_lc.log(cta::log::INFO, "Created " + std::string(m_routineName));
};

std::vector<uint64_t>&
InactiveMountQueueRoutineBase::getDeadMountVector(cta::common::dataStructures::DeadMountCandidateIDs& deadMounts,
                                                  bool isArchive,
                                                  bool isRepack,
                                                  bool isPending) {
  if (isArchive) {
    if (isRepack) {
      return isPending ? deadMounts.archiveRepackPending : deadMounts.archiveRepackActive;
    } else {
      return isPending ? deadMounts.archivePending : deadMounts.archiveActive;
    }
  } else {  // Retrieve queue
    if (isRepack) {
      return isPending ? deadMounts.retrieveRepackPending : deadMounts.retrieveRepackActive;
    } else {
      return isPending ? deadMounts.retrievePending : deadMounts.retrieveActive;
    }
  }
}

void InactiveMountQueueRoutineBase::handleInactiveMountActiveQueueRoutine(bool isArchive, bool isRepack) {
  cta::common::dataStructures::DeadMountCandidateIDs deadCandidates =
    m_RelationalDB.getDeadMounts(m_inactiveTimeLimit, m_lc);
  std::vector<uint64_t> deadMountIds = getDeadMountVector(deadCandidates, isArchive, isRepack, false /* isPending */);
  uint64_t njobs = m_RelationalDB.handleInactiveMountActiveQueues(deadMountIds, m_batchSize, isArchive, isRepack, m_lc);
  if (njobs == 0 && !deadMountIds.empty()) {
    m_RelationalDB.cleanMountLastFetchTimes(deadMountIds, isArchive, isRepack, false /* isPending */, m_lc);
  }
}

void InactiveMountQueueRoutineBase::handleInactiveMountPendingQueueRoutine(bool isArchive, bool isRepack) {
  cta::common::dataStructures::DeadMountCandidateIDs deadCandidates =
    m_RelationalDB.getDeadMounts(m_inactiveTimeLimit, m_lc);
  std::vector<uint64_t> deadMountIds = getDeadMountVector(deadCandidates, isArchive, isRepack, true /* isPending */);
  uint64_t njobs =
    m_RelationalDB.handleInactiveMountPendingQueues(deadMountIds, m_batchSize, isArchive, isRepack, m_lc);
  if (njobs == 0 && !deadMountIds.empty()) {
    m_RelationalDB.cleanMountLastFetchTimes(deadMountIds, isArchive, isRepack, true /* isPending */, m_lc);
  }
}

ArchiveInactiveMountActiveQueueRoutine::ArchiveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                                                               catalogue::Catalogue& catalogue,
                                                                               RelationalDB& pgs,
                                                                               size_t batchSize,
                                                                               uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "ArchiveInactiveMountActiveQueueRoutine",
                                    inactiveTimeLimit) {}

void ArchiveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(true /* isArchive */, false /* isRepack */);
};

RetrieveInactiveMountActiveQueueRoutine::RetrieveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                                                                 catalogue::Catalogue& catalogue,
                                                                                 RelationalDB& pgs,
                                                                                 size_t batchSize,
                                                                                 uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RetrieveInactiveMountActiveQueueRoutine",
                                    inactiveTimeLimit) {}

void RetrieveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(false /* isArchive */, false /* isRepack */);
};

RepackRetrieveInactiveMountActiveQueueRoutine::RepackRetrieveInactiveMountActiveQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize,
  uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RepackRetrieveInactiveMountActiveQueueRoutine",
                                    inactiveTimeLimit) {}

void RepackRetrieveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(false /* isArchive */, true /* isRepack */);
};

RepackArchiveInactiveMountActiveQueueRoutine::RepackArchiveInactiveMountActiveQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize,
  uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RepackArchiveInactiveMountActiveQueueRoutine",
                                    inactiveTimeLimit) {}

void RepackArchiveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(true /* isArchive */, true /* isRepack */);
};

ArchiveInactiveMountPendingQueueRoutine::ArchiveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                                                 catalogue::Catalogue& catalogue,
                                                                                 RelationalDB& pgs,
                                                                                 size_t batchSize,
                                                                                 uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "ArchiveInactiveMountPendingQueueRoutine",
                                    inactiveTimeLimit) {}

void ArchiveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(true /* isArchive */, false /* isRepack */);
};

RetrieveInactiveMountPendingQueueRoutine::RetrieveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                                                   catalogue::Catalogue& catalogue,
                                                                                   RelationalDB& pgs,
                                                                                   size_t batchSize,
                                                                                   uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RetrieveInactiveMountPendingQueueRoutine",
                                    inactiveTimeLimit) {}

void RetrieveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(false /* isArchive */, false /* isRepack */);
};

RepackRetrieveInactiveMountPendingQueueRoutine::RepackRetrieveInactiveMountPendingQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize,
  uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RepackRetrieveInactiveMountPendingQueueRoutine",
                                    inactiveTimeLimit) {}

void RepackRetrieveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(false /* isArchive */, true /* isRepack */);
};

RepackArchiveInactiveMountPendingQueueRoutine::RepackArchiveInactiveMountPendingQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize,
  uint64_t inactiveTimeLimit)
    : InactiveMountQueueRoutineBase(lc,
                                    catalogue,
                                    pgs,
                                    batchSize,
                                    "RepackArchiveInactiveMountPendingQueueRoutine",
                                    inactiveTimeLimit) {}

void RepackArchiveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(true /* isArchive */, true /* isRepack */);
};

}  // namespace cta::maintd
