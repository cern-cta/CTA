/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
