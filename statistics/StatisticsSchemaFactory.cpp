/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "StatisticsSchemaFactory.hpp"
#include "MysqlStatisticsSchema.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
  namespace statistics {
    
    std::unique_ptr<StatisticsSchema> StatisticsSchemaFactory::create(cta::rdbms::Login::DbType dbType){
      switch(dbType){
        case cta::rdbms::Login::DbType::DBTYPE_MYSQL:
          return std::unique_ptr<MysqlStatisticsSchema>(new MysqlStatisticsSchema());
        default:
          throw cta::exception::Exception("Only Mysql statistics schema is supported.");
      }
    }
  }
}

