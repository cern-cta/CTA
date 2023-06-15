/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

namespace cta {
namespace catalogue {

/**
 * Structure containing the common schema procedures of the CTA catalogue
 * database.
 */
struct CatalogueSchema {
  /**
   * Constructor.
   *
   */
  CatalogueSchema();

  /**
   * Constructor.
   *
   * @param sqlSchema The sql for the catalogue schema provided at compilation
   *                  time.
   */
  CatalogueSchema(const std::string& sqlSchema);

  /**
   * The schema.
   */
  std::string sql;

  /**
   * Returns the names of all the tables in the catalogue schema.
   *
   * @return The names of all the tables in the catalogue schema.
   */
  std::list<std::string> getSchemaTableNames() const;

  /**
   * Returns the names of all the column and their type as a map for the given 
   * table in the catalogue schema.
   *
   * @param tableName The table name to get the columns.
   * @return The map of types by name of all the columns for the given table in the catalogue schema.
   */
  std::map<std::string, std::string> getSchemaColumns(const std::string& tableName) const;

  /**
   * Returns the names of all the indexes in the catalogue schema.
   *
   * @return The names of all the indexes in the catalogue schema.
   */
  std::list<std::string> getSchemaIndexNames() const;

  /**
   * Returns the names of all the sequences in the catalogue schema.
   * 
   * if the underlying database technologies does not supported sequences then
   * this method simply returns an empty list.
   *
   * @return The names of all the sequences in the catalogue schema.
   */
  std::list<std::string> getSchemaSequenceNames() const;

  /**
   * Returns the map of strings to uint64 for the catalogue SCHEMA_VERSION_MAJOR
   * and SCHEMA_VERSION_MINOR values.
   * 
   * @return The map for SCHEMA_VERSION_MAJOR and SCHEMA_VERSION_MINOR  values.
   */
  std::map<std::string, uint64_t> getSchemaVersion() const;
};

}  // namespace catalogue
}  // namespace cta
