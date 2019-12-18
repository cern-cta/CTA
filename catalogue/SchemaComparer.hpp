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

class SchemaComparer {
public:
  SchemaComparer(const cta::rdbms::Login::DbType &catalogueDbType,cta::rdbms::Conn &conn);
  virtual ~SchemaComparer();
  /**
   * Compare the schema
   * @return 
   */
  virtual SchemaComparerResult compare() = 0;
  std::string getCatalogueVersion();
protected:
  const cta::rdbms::Login::DbType &m_dbType;
  std::string m_catalogueSchemaVersion;
  cta::rdbms::Conn &m_catalogueConn;
  std::unique_ptr<cta::catalogue::CatalogueMetadataGetter> m_catalogueMetadataGetter;
  
private:
  virtual SchemaComparerResult compareTables() = 0;
  virtual SchemaComparerResult compareIndexes() = 0;
};

}}