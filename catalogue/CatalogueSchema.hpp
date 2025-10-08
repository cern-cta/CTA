/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <list>
#include <map>

namespace cta::catalogue {

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
   * Constructor
   *
   * @param sqlSchema The sql for the catalogue schema provided at compilation time
   */
  explicit CatalogueSchema(const std::string &sqlSchema);

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
  std::map<std::string, std::string, std::less<>> getSchemaColumns(const std::string &tableName) const;

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
  std::map<std::string, uint64_t, std::less<>> getSchemaVersion() const;
};

} // namespace cta::catalogue
