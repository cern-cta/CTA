/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "mountdecision/MountDecisionDB.hpp"
#include "scheduler/rdbms/ConnProvider.hpp"

#include <memory>
#include <string>
#include <string_view>

namespace cta::mountdecision {

/**
 * High-level Mount Decision component interface.
 *
 * This class owns the Mount Decision backend wrapper and uses the supplied
 * connection provider to access the scheduler DB.
 */
class MountDecision final {
public:
  MountDecision(ConnProvider& connectionProvider, log::LogContext& lc);

  bool isAvailable() const;

  bool incrementCounter(const std::string& key, log::LogContext& lc, std::string_view operation);

private:
  std::unique_ptr<MountDecisionDB> m_db;
};

}  // namespace cta::mountdecision
