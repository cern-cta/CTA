/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "InactiveMountQueueRoutines.hpp"

namespace cta::maintd {

  InactiveMountQueueRoutineBase::InactiveMountQueueRoutineBase(log::LogContext &lc, catalogue::Catalogue &catalogue,
                                                               RelationalDB &pgs,
                                                               size_t batchSize, std::string routineName) : m_lc(lc),
                                                                                                            m_catalogue(
                                                                                                                    catalogue),
                                                                                                            m_RelationalDB(
                                                                                                                    pgs),
                                                                                                            m_batchSize(
                                                                                                                    batchSize),
                                                                                                            m_routineName(
                                                                                                                    routineName) {
    log::ScopedParamContainer params(m_lc);
    params.add("batchSize", m_batchSize);
    m_lc.log(cta::log::INFO, "Created " + std::string(routineName));
  }

  void InactiveMountQueueRoutineBase::handleInactiveMountQueueRoutine(const std::string &queueTypePrefix) {
    // Get all active mount IDs for drives which do have an active mount registered in the catalogue
    std::unordered_map <std::string, std::optional<uint64_t>> driveNameMountIdOpt = m_catalogue.getTapeDriveMountIDs();

    std::unordered_set <uint64_t> activeMountIds;
    activeMountIds.reserve(driveNameMountIdOpt.size());

    for (const auto &kv: driveNameMountIdOpt) {
      if (kv.second) {
        activeMountIds.insert(*kv.second);
      }
    }
    if (activeMountIds.empty()) return;
    // Get all active mount IDs from the Scheduler DB
    std::vector <uint64_t> scheduledMountIds = m_RelationalDB.getScheduledMountIDs(queueTypePrefix);
    if (scheduledMountIds.empty()) return;

    std::vector <uint64_t> deadMountIds;
    deadMountIds.reserve(scheduledMountIds.size());

    for (uint64_t id: scheduledMountIds) {
      if (activeMountIds.find(id) == activeMountIds.end()) {
        deadMountIds.push_back(id);
      }
    }
    if (deadMountIds.empty()) return;

    m_RelationalDB.handleInactiveMountQueues(deadMountIds, queueTypePrefix, m_batchSize);

  }

  ArchiveInactiveMountQueueRoutine::ArchiveInactiveMountQueueRoutine(log::LogContext &lc,
                                                                     catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                                                     size_t batchSize) : InactiveMountQueueRoutineBase(
          lc, catalogue, pgs, batchSize, "ArchiveInactiveMountQueueRoutine") {}

  void ArchiveInactiveMountQueueRoutine::execute() {
    handleInactiveMountQueueRoutine("ARCHIVE_");
  }

  RetrieveInactiveMountQueueRoutine::RetrieveInactiveMountQueueRoutine(log::LogContext &lc,
                                                                       catalogue::Catalogue &catalogue,
                                                                       RelationalDB &pgs,
                                                                       size_t batchSize)
          : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RetrieveInactiveMountQueueRoutine") {}

  void RetrieveInactiveMountQueueRoutine::execute() {
    handleInactiveMountQueueRoutine("RETRIEVE_");
  }

  RepackRetrieveInactiveMountQueueRoutine::RepackRetrieveInactiveMountQueueRoutine(log::LogContext &lc,
                                                                                   catalogue::Catalogue &catalogue,
                                                                                   RelationalDB &pgs,
                                                                                   size_t batchSize)
          : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackRetrieveInactiveMountQueueRoutine") {}

  void RepackRetrieveInactiveMountQueueRoutine::execute() {
    handleInactiveMountQueueRoutine("REPACK_RETRIEVE_");
  }

  RepackArchiveInactiveMountQueueRoutine::RepackArchiveInactiveMountQueueRoutine(log::LogContext &lc,
                                                                                 catalogue::Catalogue &catalogue,
                                                                                 RelationalDB &pgs,
                                                                                 size_t batchSize)
          : InactiveMountQueueRoutineBase(lc, catalogue, pgs, batchSize, "RepackArchiveInactiveMountQueueRoutine") {}

  void RepackArchiveInactiveMountQueueRoutine::execute() {
    handleInactiveMountQueueRoutine("REPACK_ARCHIVE_");
  }
}  // namespace cta::maintd
