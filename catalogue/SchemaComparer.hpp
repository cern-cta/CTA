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
/* 
 * File:   SchemaComparer.hpp
 * Author: cedric
 *
 * Created on December 10, 2019, 10:58 AM
 */

#pragma once

#include "rdbms/ConnPool.hpp"
#include "SchemaComparerResult.hpp"
#include "CatalogueMetadataGetter.hpp"

namespace cta {
namespace catalogue {

/**
 * This class is used to compare the schema that is running against the database accessible
 * via the connection given in the constructor
 */
class SchemaComparer {
public:
  /**
   * Constructs a SchemaComparer
   * @param catalogueDbType the database type of the database to compare
   * @param conn the connection of the database to compare the schema
   */
  SchemaComparer(const cta::rdbms::Login::DbType &catalogueDbType,cta::rdbms::Conn &conn);
  /**
   * Destructor
   */
  virtual ~SchemaComparer();
  /**
   * Compare the schema to compare against the database 
   * @return a SchemaComparerResult object that will contain the differences if there are some
   */
  virtual SchemaComparerResult compare() = 0;
  /**
   * Return the catalogue version of the schema located in the database to compare
   * @return the catalogue version of the schema located in the database to compare
   */
  std::string getCatalogueVersion();
protected:
  const cta::rdbms::Login::DbType &m_dbType;
  std::string m_catalogueSchemaVersion;
  cta::rdbms::Conn &m_catalogueConn;
  std::unique_ptr<cta::catalogue::CatalogueMetadataGetter> m_catalogueMetadataGetter;
  
private:
  /**
   * Compare the tables of the schema against the catalogue database
   * @return a SchemaComparerResult that will contain the differences if there are some
   */
  virtual SchemaComparerResult compareTables() = 0;
  /**
   * Compare the indexes of the schema against the catalogue database
   * @return a SchemaComparerResult that will contain the differences if there are some
   */
  virtual SchemaComparerResult compareIndexes() = 0;
};

}}