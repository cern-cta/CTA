/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/CatalogueSchema.hpp"

#include <string>

namespace cta {
namespace statistics {

/**
 * Structure containing the SQL to create the schema of the in Statistics Database
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * MySQLStatisticsSchema.cpp from:
 *   - MySQLStatisticsSchema.before_SQL.cpp
 *   - common_statistics_schema.sql
 *
 * The MySQLStatisticsSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct StatisticsSchema: public cta::catalogue::CatalogueSchema {
  /**
   * Constructor.
   */
  StatisticsSchema(const std::string &sqlSchema);
};

} // namespace statistics
} // namespace cta
