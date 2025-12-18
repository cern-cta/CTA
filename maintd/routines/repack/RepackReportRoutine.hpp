/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

class Scheduler;

class RepackReportRoutine : public IRoutine {
public:
  RepackReportRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int timeout);

  void execute() final;
  std::string getName() const final;

private:
  template<typename GetBatchFunc>
  void reportBatch(const std::string& reportingType, GetBatchFunc getBatchFunc) const;

  cta::log::LogContext& m_lc;
  cta::Scheduler& m_scheduler;
  int m_softTimeout;
};
}  // namespace cta::maintd
