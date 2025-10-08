/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <list>
#include <map>

namespace cta::schedulerdb {

/**
 * Structure containing the common schema procedures of the CTA schedulerdb
 * database.
 */
struct SchedulerSchema {

  /**
   * Constructor.
   *
   */
  SchedulerSchema();

  /**
   * Constructor.
   *
   * @param sqlSchema The sql for the scheduler schema provided at compilation
   *                  time.
   */
  explicit SchedulerSchema(const std::string &sqlSchema);

  /**
   * The schema.
   */
  std::string sql;

  /**
   * Returns the map of strings to uint64 for the scheduler SCHEMA_VERSION_MAJOR
   * and SCHEMA_VERSION_MINOR values.
   *
   * @return The map for SCHEMA_VERSION_MAJOR and SCHEMA_VERSION_MINOR  values.
   */
  std::map<std::string, uint64_t, std::less<>> getSchemaVersion() const;
};

} // namespace cta::schedulerdb
