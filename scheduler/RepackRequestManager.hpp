/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/LogContext.hpp"

namespace cta {

class Scheduler;

class RepackRequestManager {
public:
  explicit RepackRequestManager(Scheduler &scheduler) : m_scheduler(scheduler) {}

  void runOnePass(log::LogContext &lc, const size_t repackMaxRequestsToExpand);

private:
  Scheduler & m_scheduler;
};
} // namespace cta
