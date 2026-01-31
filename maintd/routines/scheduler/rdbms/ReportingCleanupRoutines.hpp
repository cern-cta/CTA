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
 * @brief Periodic routine that reactivated for reporting rows which are stale
 *
 * Updated IS_REPORTING state back to false to make rows eligible
 * to be picked up by reporting again in case of dead reporting process.
 */
class ResubmitInactiveReportingRoutine : public IRoutine {
public:
  std::string getName() const final { return m_routineName; };

  void execute();

  virtual ~ResubmitInactiveReportingRoutine() = default;

  ResubmitInactiveReportingRoutine(log::LogContext& lc,
                                   RelationalDB& pgs,
                                   size_t batchSize,
                                   uint64_t inactiveTimeLimit);

private:
  cta::log::LogContext& m_lc;
  cta::RelationalDB& m_RelationalDB;
  size_t m_batchSize;
  const std::string m_routineName = "ResubmitInactiveReportingRoutine";
  uint64_t m_inactiveTimeLimit;
};

}  // namespace cta::maintd
