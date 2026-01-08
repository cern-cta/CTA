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

namespace cta::maintd {

  class InactiveMountQueueRoutineBase : public IRoutine {
  public:

    std::string getName() const final { return m_routineName; };

    void handleInactiveMountQueueRoutine(const std::string &queueTypePrefix) final;

    virtual ~InactiveMountQueueRoutineBase() = default;

  protected:

    InactiveMountQueueRoutineBase(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                  size_t batchSize, std::string routineName);

    cta::log::LogContext &m_lc;
    cta::RelationalDB &m_RelationalDB;
    catalogue::Catalogue &m_catalogue;
    size_t m_batchSize;
    std::string m_routineName;
  };

  class ArchiveInactiveMountQueueRoutine : public InactiveMountQueueRoutineBase {
  public:
    ArchiveInactiveMountQueueRoutine(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                     size_t batchSize);

    void execute();

  };

  class RetrieveInactiveMountQueueRoutine : public InactiveMountQueueRoutineBase {
  public:
    RetrieveInactiveMountQueueRoutine(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                      size_t batchSize);

    void execute();

  };

  class RepackRetrieveInactiveMountQueueRoutine : public InactiveMountQueueRoutineBase {
  public:
    RepackRetrieveInactiveMountQueueRoutine(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                            size_t batchSize);

    void execute();

  };

  class RepackArchiveInactiveMountQueueRoutine : public InactiveMountQueueRoutineBase {
  public:
    RepackArchiveInactiveMountQueueRoutine(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs,
                                           size_t batchSize);

    void execute();

  };

}  // namespace cta::maintd
