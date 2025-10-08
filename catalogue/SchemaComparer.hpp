/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
/*
 * File:   SchemaComparer.hpp
 * Author: cedric
 *
 * Created on December 10, 2019, 10:58 AM
 */
#pragma once

#include "rdbms/ConnPool.hpp"
#include "SchemaCheckerResult.hpp"
#include "DatabaseMetadataGetter.hpp"
#include "SchemaSqlStatementsReader.hpp"

namespace cta::catalogue {

/**
 * This class is used to compare the schema that is running against the database accessible
 * via the connection given in the constructor
 */
class SchemaComparer {
public:
  /**
   * Constructs a SchemaComparer
   * @param catalogueMetadataGetter the catalogue metadata getter to compare the catalogue schema content
   */
  SchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &databaseMetadataGetter);
  /**
   * Destructor
   */
  virtual ~SchemaComparer() = default;
  /**
   * Compare the schema to compare against the database
   * @return a SchemaComparerResult object that will contain the differences if there are some
   */
  virtual SchemaCheckerResult compareAll() = 0;
  /**
   * Compare the tables of the schema against the catalogue database
   * @return a SchemaComparerResult that will contain the differences if there are some
   */
  virtual SchemaCheckerResult compareTables() = 0;
  /**
   * Compare the indexes of the schema against the catalogue database
   * @return a SchemaComparerResult that will contain the differences if there are some
   */
  virtual SchemaCheckerResult compareIndexes() = 0;

  /**
   * Compare only the tables that are located in the schema
   * This is useful when want to compare ONLY tables that are defined in a schema
   * @return a SchemaComparerResult that will contain the differences if there are some
   */
  virtual SchemaCheckerResult compareTablesLocatedInSchema() = 0;
  /**
   * Sets the way the schema sql statements will be read to do the schemas comparison
   * @param schemaSqlStatementsReader the reader used to get the schema sql statements in order to do schema comparison
   */
  void setSchemaSqlStatementsReader(std::unique_ptr<SchemaSqlStatementsReader> schemaSqlStatementsReader);

protected:
  const std::string  m_databaseToCheckName;
  cta::catalogue::DatabaseMetadataGetter & m_databaseMetadataGetter;
  std::unique_ptr<SchemaSqlStatementsReader> m_schemaSqlStatementsReader;
  bool m_compareTableConstraints = true;
};

} // namespace cta::catalogue
