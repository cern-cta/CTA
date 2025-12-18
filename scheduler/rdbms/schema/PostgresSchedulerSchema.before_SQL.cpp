/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresSchedulerSchema::PostgresSchedulerSchema()
    : SchedulerSchema(
        // CTA_SQL_SCHEMA - The contents of postgres_scheduler_schema.cpp go here
      ) {}

}  // namespace cta::schedulerdb
