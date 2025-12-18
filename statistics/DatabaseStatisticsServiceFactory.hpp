/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DatabaseStatisticsService.hpp"

#include <memory>

namespace cta::statistics {

class DatabaseStatisticsServiceFactory {
public:
  static std::unique_ptr<DatabaseStatisticsService> create(cta::rdbms::Conn* databaseConnection,
                                                           cta::rdbms::Login::DbType dbType);
};

}  // namespace cta::statistics
