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
#include "rdbms/Login.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {
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
    SQLiteSchemaInserter(rdbms::Conn &sqliteConn);
    /**
     * Transform and insert the schema statements passed in parameter into the
     * InMemory SQLite database
     */
    void insert(const std::list<std::string> &schemaStatements);    
    virtual ~SQLiteSchemaInserter();
    
  private:
    cta::rdbms::Conn & m_sqliteCatalogueConn;
    /**
     * Execute all the statements passed in parameter against the InMemory SQLite database 
     * @param statements the statements to execute in the InMemory SQLite database
     */
    void executeStatements(const std::list<std::string> &statements);
  };
  
}}


