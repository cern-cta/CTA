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

class InactiveMountQueueRoutineBase : public IRoutine {
public:
  std::string getName() const final { return m_routineName; };

  cta::common::dataStructures::DeadMountCandidateIDs getDeadMountCandicateIDs();
  std::vector<uint64_t>& getDeadMountVector(cta::common::dataStructures::DeadMountCandidateIDs& deadMounts,
                                            bool isArchive,
                                            bool isRepack,
                                            bool isPending);
  void handleInactiveMountActiveQueueRoutine(bool isArchive, bool isRepack);
  void handleInactiveMountPendingQueueRoutine(bool isArchive, bool isRepack);

  virtual ~InactiveMountQueueRoutineBase() = default;

protected:
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

class DeleteOldFailedQueuesRoutine : public InactiveMountQueueRoutineBase {
public:
  DeleteOldFailedQueuesRoutine(log::LogContext& lc,
                               catalogue::Catalogue& catalogue,
                               RelationalDB& pgs,
                               size_t batchSize,
                               uint64_t inactiveTimeLimit);

  void execute();
};

}  // namespace cta::maintd
