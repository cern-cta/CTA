/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#pragma once

#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

#include <string>
#include <list>

// TODO
#include <iostream>

namespace cta {
namespace catalogue {

/**
 * Structure containing the common schema procedures of the CTA catalogue
 * database.
 */
struct CatalogueSchema {
  /**
   * Constructors.
   */
  CatalogueSchema(const std::string &sqlSchema);
  CatalogueSchema(const std::string &sqlSchema, const std::string &sqlTriggerSchema);
  
  /**
   * The schema.
   */
  const std::string sql;
  
  /**
   * The trigger.
   */
  const std::string sql_trigger;

  /**
   * TODO
   * 
   * @return 
   */
  std::list<std::string> getSchemaTableNames() const;
  /**
   * TODO
   * 
   * @return 
   */
  std::list<std::string> getSchemaIndexNames() const;
  /**
   * TODO
   * 
   * @return 
   */
  std::list<std::string> getSchemaSequenceNames() const;
  /**
   * TODO
   * 
   * @return 
   */
  std::list<std::string> getSchemaTriggerNames() const;
};

} // namespace catalogue
} // namespace cta
