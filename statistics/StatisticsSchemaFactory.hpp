/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <memory>

#include "StatisticsSchema.hpp"
#include "rdbms/Login.hpp"

namespace cta {
namespace statistics{
  
  /**
   * Factory of Statistics Schema
   * Note: Only MySQL is supported for Statistics database
   */
  class StatisticsSchemaFactory {
    public:
      /**
       * Creates a StatisticsSchema
       * @param dbType the dbType to deduce which StatisticsSchema to instanciate
       * @return a unique_ptr containing the StatisticsSchema
       * @throws and exception if the dbType != MYSQL, statistics database works for MySQL only
       * Other database Schema can be implemented.
       */
      static std::unique_ptr<StatisticsSchema> create(cta::rdbms::Login::DbType dbType);
    };
  }
}

