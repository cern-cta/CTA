/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>
#include <string>
#include <utility>

#include "SchemaComparer.hpp"

namespace cta::catalogue {

SchemaComparer::SchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &catalogueMetadataGetter)
  : m_databaseToCheckName(databaseToCheckName),
    m_databaseMetadataGetter(catalogueMetadataGetter) {}

void SchemaComparer::setSchemaSqlStatementsReader(std::unique_ptr<SchemaSqlStatementsReader> schemaSqlStatementsReader) {
  m_schemaSqlStatementsReader = std::move(schemaSqlStatementsReader);
}

} // namespace cta::catalogue
