/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/Conn.hpp"

#include <functional>
#include <optional>
#include <string>

namespace cta::mountdecision {

/**
 * Relational backend for the Mount Decision DB.
 */
class MountDecisionDB {
public:
  using ConnectionProvider = std::function<rdbms::Conn()>;

  explicit MountDecisionDB(ConnectionProvider connectionProvider);

  void ping();

  void setValue(const std::string& key, const std::string& value);

  std::optional<std::string> getValue(const std::string& key);

  void incrementCounter(const std::string& key);

private:
  ConnectionProvider m_connectionProvider;
};

}  // namespace cta::mountdecision
