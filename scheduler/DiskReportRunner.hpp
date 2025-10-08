/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/LogContext.hpp"
#include "disk/DiskReporterFactory.hpp"

namespace cta {

class Scheduler;

class DiskReportRunner {
public:
  explicit DiskReportRunner(Scheduler& scheduler) : m_scheduler(scheduler) {}
  void runOnePass(log::LogContext& lc);

private:
  Scheduler& m_scheduler;
  disk::DiskReporterFactory m_reporterFactory;
};

} // namespace cta
