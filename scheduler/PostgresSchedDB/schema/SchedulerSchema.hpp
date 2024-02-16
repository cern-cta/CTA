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

#include <string>
#include <list>
#include <map>

namespace cta::postgresscheddb {

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
  std::map<std::string, uint64_t> getSchemaVersion() const;
};

} // namespace cta::postgresscheddb
