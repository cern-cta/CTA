/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "disk/DiskReporterFactory.hpp"
#include "maintd/IRoutine.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

class DiskReportRetrieveRoutine final : public IRoutine {
public:
  DiskReportRetrieveRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int batchSize, int softTimeout);
  void execute() final;
  std::string getName() const final;

private:
  cta::log::LogContext& m_lc;
  cta::Scheduler& m_scheduler;
  cta::disk::DiskReporterFactory m_reporterFactory;

  int m_batchSize = 500;
  int m_softTimeout = 30;
};

}  // namespace cta::maintd
