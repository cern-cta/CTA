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
                                                             uint64_t ageForCollection)
    : m_lc(lc),
      m_catalogue(catalogue),
      m_RelationalDB(pgs),
      m_batchSize(batchSize),
      m_routineName(routineName),
      m_ageForCollection(ageForCollection) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", m_batchSize);
  m_lc.log(cta::log::INFO, "Created " + std::string(m_routineName));
};

cta::common::dataStructures::DeadMountCandidateIDs InactiveMountQueueRoutineBase::getDeadMountCandicateIDs() {
  // Get all active mount IDs for drives which do have an active mount registered in the catalogue
  std::unordered_map<std::string, std::optional<uint64_t>> driveNameMountIdOpt =
    m_catalogue.DriveState()->getTapeDriveMountIDs();
  log::ScopedParamContainer params(m_lc);
  params.add("routineName", m_routineName);
  std::unordered_set<uint64_t> activeMountIds;
  activeMountIds.reserve(driveNameMountIdOpt.size());

  for (const auto& kv : driveNameMountIdOpt) {
    if (kv.second) {
      activeMountIds.insert(*kv.second);
    }
  }
  params.add("activeCatalogueMountIdCount", activeMountIds.size());
  m_lc.log(cta::log::INFO, "Fetched mounts registered in catalogue for active drives.");
  // Get all active mount IDs from the Scheduler DB
  cta::common::dataStructures::DeadMountCandidateIDs scheduledMountIDs =
    m_RelationalDB.getDeadMountCandidates(m_ageForCollection, m_lc);
  params.add("archivePendingDeadMountIdsCount", scheduledMountIDs.archivePending.size());
  params.add("archiveActiveDeadMountIdsCount", scheduledMountIDs.archiveActive.size());
  params.add("retrievePendingDeadMountIdsCount", scheduledMountIDs.retrievePending.size());
  params.add("retrieveActiveDeadMountIdsCount", scheduledMountIDs.retrieveActive.size());
  params.add("archiveRepackPendingDeadMountIdsCount", scheduledMountIDs.archiveRepackPending.size());
  params.add("archiveRepackActiveDeadMountIdsCount", scheduledMountIDs.archiveRepackActive.size());
  params.add("retrieveRepackPendingDeadMountIdsCount", scheduledMountIDs.retrieveRepackPending.size());
  params.add("retrieveRepackActiveDeadMountIdsCount", scheduledMountIDs.retrieveRepackActive.size());
  m_lc.log(cta::log::INFO, "Fetched dead mounts from scheduler DB.");

  /* We will now filter out all Mount IDs which are still reported as alive in the catalogue
   * in order to be sure no active processes from the mount will be changing the DB rows
   * (in case of wrong timeout input e.g.)
   */
  // Helper lambda to remove IDs present in activeSet
  auto removeActiveIds = [&activeMountIds](std::vector<uint64_t>& vec) {
    std::erase_if(vec, [&activeMountIds](uint64_t id) { return activeMountIds.count(id) > 0; });
  };
  removeActiveIds(scheduledMountIDs.archivePending);
  removeActiveIds(scheduledMountIDs.archiveActive);
  removeActiveIds(scheduledMountIDs.retrievePending);
  removeActiveIds(scheduledMountIDs.retrieveActive);
  removeActiveIds(scheduledMountIDs.archiveRepackPending);
  removeActiveIds(scheduledMountIDs.archiveRepackActive);
  removeActiveIds(scheduledMountIDs.retrieveRepackPending);
  removeActiveIds(scheduledMountIDs.retrieveRepackActive);
  params.add("archivePendingDeadMountIdsCount", scheduledMountIDs.archivePending.size());
  params.add("archiveActiveDeadMountIdsCount", scheduledMountIDs.archiveActive.size());
  params.add("retrievePendingDeadMountIdsCount", scheduledMountIDs.retrievePending.size());
  params.add("retrieveActiveDeadMountIdsCount", scheduledMountIDs.retrieveActive.size());
  params.add("archiveRepackPendingDeadMountIdsCount", scheduledMountIDs.archiveRepackPending.size());
  params.add("archiveRepackActiveDeadMountIdsCount", scheduledMountIDs.archiveRepackActive.size());
  params.add("retrieveRepackPendingDeadMountIdsCount", scheduledMountIDs.retrieveRepackPending.size());
  params.add("retrieveRepackActiveDeadMountIdsCount", scheduledMountIDs.retrieveRepackActive.size());
  m_lc.log(cta::log::INFO, "Found dead mounts which need job rescheduling.");
  return scheduledMountIDs;
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
  cta::common::dataStructures::DeadMountCandidateIDs deadCandidates = getDeadMountCandicateIDs();
  std::vector<uint64_t> deadMountIds = getDeadMountVector(deadCandidates, isArchive, isRepack, false /* isPending */);
  m_RelationalDB.handleInactiveMountActiveQueues(deadMountIds, m_batchSize, isArchive, isRepack, m_lc);
}

void InactiveMountQueueRoutineBase::handleInactiveMountPendingQueueRoutine(bool isArchive, bool isRepack) {
  cta::common::dataStructures::DeadMountCandidateIDs deadCandidates = getDeadMountCandicateIDs();
  std::vector<uint64_t> deadMountIds = getDeadMountVector(deadCandidates, isArchive, isRepack, true /* isPending */);
  m_RelationalDB.handleInactiveMountPendingQueues(deadMountIds, m_batchSize, isArchive, isRepack, m_lc);
}

ArchiveInactiveMountActiveQueueRoutine::ArchiveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                                                               catalogue::Catalogue& catalogue,
                                                                               RelationalDB& pgs,
                                                                               size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "ArchiveInactiveMountActiveQueueRoutine", 300 /* ageForCollection */) {}

void ArchiveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(true /* isArchive */, false /* isRepack */);
};

RetrieveInactiveMountActiveQueueRoutine::RetrieveInactiveMountActiveQueueRoutine(log::LogContext& lc,
                                                                                 catalogue::Catalogue& catalogue,
                                                                                 RelationalDB& pgs,
                                                                                 size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RetrieveInactiveMountActiveQueueRoutine", 300 /* ageForCollection */) {}

void RetrieveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(false /* isArchive */, false /* isRepack */);
};

RepackRetrieveInactiveMountActiveQueueRoutine::RepackRetrieveInactiveMountActiveQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackRetrieveInactiveMountActiveQueueRoutine", 300 /* ageForCollection */) {}

void RepackRetrieveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(false /* isArchive */, true /* isRepack */);
};

RepackArchiveInactiveMountActiveQueueRoutine::RepackArchiveInactiveMountActiveQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackArchiveInactiveMountActiveQueueRoutine", 300 /* ageForCollection */) {}

void RepackArchiveInactiveMountActiveQueueRoutine::execute() {
  handleInactiveMountActiveQueueRoutine(true /* isArchive */, true /* isRepack */);
};

ArchiveInactiveMountPendingQueueRoutine::ArchiveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                                                 catalogue::Catalogue& catalogue,
                                                                                 RelationalDB& pgs,
                                                                                 size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "ArchiveInactiveMountPendingQueueRoutine", 300 /* ageForCollection */) {}

void ArchiveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(true /* isArchive */, false /* isRepack */);
};

RetrieveInactiveMountPendingQueueRoutine::RetrieveInactiveMountPendingQueueRoutine(log::LogContext& lc,
                                                                                   catalogue::Catalogue& catalogue,
                                                                                   RelationalDB& pgs,
                                                                                   size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RetrieveInactiveMountPendingQueueRoutine", 300 /* ageForCollection */) {}

void RetrieveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(false /* isArchive */, false /* isRepack */);
};

RepackRetrieveInactiveMountPendingQueueRoutine::RepackRetrieveInactiveMountPendingQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackRetrieveInactiveMountPendingQueueRoutine", 300 /* ageForCollection */) {}

void RepackRetrieveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(false /* isArchive */, true /* isRepack */);
};

RepackArchiveInactiveMountPendingQueueRoutine::RepackArchiveInactiveMountPendingQueueRoutine(
  log::LogContext& lc,
  catalogue::Catalogue& catalogue,
  RelationalDB& pgs,
  size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackArchiveInactiveMountPendingQueueRoutine", 300 /* ageForCollection */) {}

void RepackArchiveInactiveMountPendingQueueRoutine::execute() {
  handleInactiveMountPendingQueueRoutine(true /* isArchive */, true /* isRepack */);
};

DeleteOldFailedQueuesRoutine::DeleteOldFailedQueuesRoutine(log::LogContext& lc,
                                                           catalogue::Catalogue& catalogue,
                                                           RelationalDB& pgs,
                                                           size_t batchSize)
    : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "DeleteOldFailedQueuesRoutine", 1209600 /* ageForCollection */) {}

void DeleteOldFailedQueuesRoutine::execute() {
  // only rows older than 2 weeks will be deleted from the FAILED tables
  m_RelationalDB.deleteOldFailedQueues(1209600, m_batchSize, m_lc);
};

}  // namespace cta::maintd
