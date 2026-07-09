/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "mountdecision/MountDecisionDB.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace cta::mountdecision {

/**
 * High-level Mount Decision component interface.
 *
 * This class owns the backend connection setup and exposes Mount Decision
 * operations without forcing callers to know how the backend is initialized.
 */
class MountDecision final {
public:
  using ConnectionProvider = MountDecisionDB::ConnectionProvider;

  MountDecision(ConnectionProvider connectionProvider, log::LogContext& lc);

  bool isAvailable() const;

  bool incrementCounter(const std::string& key, log::LogContext& lc, std::string_view operation);

private:
  std::unique_ptr<MountDecisionDB> m_db;
};

}  // namespace cta::mountdecision
