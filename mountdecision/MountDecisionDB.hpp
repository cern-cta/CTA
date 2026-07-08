/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace cta::mountdecision {

/**
 * Relational backend for the Mount Decision DB.
 */
class MountDecisionDB {
public:
  MountDecisionDB(const std::string& ownerId, log::Logger&, const rdbms::Login& login, uint64_t nbConns);

  void ping();

  void setValue(const std::string& key, const std::string& value);

  std::optional<std::string> getValue(const std::string& key);

  void incrementCounter(const std::string& key);

private:
  std::string m_ownerId;
  rdbms::ConnPool m_connPool;
};

}  // namespace cta::mountdecision
