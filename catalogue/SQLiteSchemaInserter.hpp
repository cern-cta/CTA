/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <vector>

namespace cta::catalogue {

/**
   * This class is used to create a InMemory SQLiteSchema from sql statements
   *
   * @param sqliteConn the connection of the InMemory SQLite schema
   */
class SQLiteSchemaInserter {
public:
  /**
     * Constructor
     * @param sqliteConn the connection of the InMemory SQLite schema
     */
  explicit SQLiteSchemaInserter(rdbms::Conn& sqliteConn);
  /**
     * Transform and insert the schema statements passed in parameter into the
     * InMemory SQLite database
     */
  void insert(const std::vector<std::string>& schemaStatements);
  virtual ~SQLiteSchemaInserter() = default;

private:
  cta::rdbms::Conn& m_sqliteCatalogueConn;
  /**
     * Execute all the statements passed in parameter against the InMemory SQLite database
     * @param statements the statements to execute in the InMemory SQLite database
     */
  void executeStatements(const std::vector<std::string>& statements);
};

}  // namespace cta::catalogue
