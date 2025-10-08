/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "scheduler/rdbms/schema/SchedulerSchema.hpp"

#include <string>

namespace cta::schedulerdb {

/**
 * Structure containing the SQL to create the schema of the CTA scheduler
 * database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * PostgresSchedulerSchema.cpp from:
 *   - PostgresSchedulerSchema.before_SQL.cpp
 *   - postgres_scheduler_schema.sql
 *
 * The PostgresSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct PostgresSchedulerSchema: public SchedulerSchema {
  /**
   * Constructor.
   */
  PostgresSchedulerSchema();
};

} // namespace cta::schedulerdb
