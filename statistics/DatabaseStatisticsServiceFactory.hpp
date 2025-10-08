/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "DatabaseStatisticsService.hpp"
namespace cta::statistics {

class DatabaseStatisticsServiceFactory {
 public:
  static std::unique_ptr<DatabaseStatisticsService> create(cta::rdbms::Conn* databaseConnection,
    cta::rdbms::Login::DbType dbType);
};

} // namespace cta::statistics
