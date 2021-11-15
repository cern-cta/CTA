/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <memory>
#include <string>
#include <utility>

#include "SchemaComparer.hpp"

namespace cta {
namespace catalogue {
SchemaComparer::SchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &catalogueMetadataGetter)
  : m_databaseToCheckName(databaseToCheckName),
    m_databaseMetadataGetter(catalogueMetadataGetter),
    m_compareTableConstraints((m_databaseMetadataGetter.getDbType() != cta::rdbms::Login::DBTYPE_MYSQL)) {}

void SchemaComparer::setSchemaSqlStatementsReader(std::unique_ptr<SchemaSqlStatementsReader> schemaSqlStatementsReader) {
  m_schemaSqlStatementsReader = std::move(schemaSqlStatementsReader);
}

SchemaComparer::~SchemaComparer() {
}

}  // namespace catalogue
}  // namespace cta
