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

class RepackExpandRoutine final : public IRoutine {
public:
  RepackExpandRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int maxRequestsToToExpand);

  void execute() final;
  std::string getName() const final;

private:
  cta::log::LogContext& m_lc;
  cta::Scheduler& m_scheduler;
  int m_repackMaxRequestsToToExpand;
};
}  // namespace cta::maintd
