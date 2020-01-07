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
 * File:   SchemaComparer.cpp
 * Author: cedric
 * 
 * Created on December 10, 2019, 10:58 AM
 */

#include "SchemaComparer.hpp"

namespace cta {
namespace catalogue {
SchemaComparer::SchemaComparer(const cta::rdbms::Login::DbType &dbType, rdbms::Conn &conn): m_dbType(dbType),m_catalogueConn(conn),m_compareTableConstraints((m_dbType != cta::rdbms::Login::DBTYPE_MYSQL)){
  m_catalogueMetadataGetter.reset(CatalogueMetadataGetterFactory::create(dbType,m_catalogueConn));
  m_catalogueSchemaVersion = m_catalogueMetadataGetter->getCatalogueVersion();
}

std::string SchemaComparer::getCatalogueVersion(){
  return m_catalogueSchemaVersion;
}

SchemaComparer::~SchemaComparer() {
}

}}
