/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <time.h>

#ifndef TAPEMOUNTDECISIONINFO_H
#define TAPEMOUNTDECISIONINFO_H
#endif /* TAPEMOUNTDECISIONINFO_H */

namespace cta::schedulerdb {

class ArchiveMount;

class TapeMountDecisionInfo : public SchedulerDatabase::TapeMountDecisionInfo {
  friend class cta::RelationalDB;

public:
  explicit TapeMountDecisionInfo(RelationalDB& pdb,
                                 const std::string& ownerId,
                                 TapeDrivesCatalogueState* drivesState,
                                 log::LogContext& lc);

  std::unique_ptr<SchedulerDatabase::ArchiveMount>
  createArchiveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                     const catalogue::TapeForWriting& tape,
                     const std::string& driveName,
                     const std::string& logicalLibrary,
                     const std::string& hostName) override;

  std::unique_ptr<SchedulerDatabase::RetrieveMount>
  createRetrieveMount(const cta::SchedulerDatabase::PotentialMount& mount,
                      const std::string& driveName,
                      const std::string& logicalLibrary,
                      const std::string& hostName) override;

private:
  /** Acquire Scheduler DB advisory lock per tape pool */
  void lock(std::string_view tapePool);
  /** Commit decision and release scheduler global lock */
  void commit();

  cta::RelationalDB& m_RelationalDB;
  log::LogContext& m_lc;
  std::unique_ptr<schedulerdb::Transaction> m_txn;
  std::string m_ownerId;
  bool m_lockTaken = false;
  TapeDrivesCatalogueState* m_tapeDrivesState = nullptr;
};

}  // namespace cta::schedulerdb
