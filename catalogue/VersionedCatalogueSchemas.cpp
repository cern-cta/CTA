/*
 * SPDX-FileCopyrightText: 2015 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/VersionedCatalogueSchemas.hpp"
#include "catalogue/AllCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleVersionedCatalogueSchema::OracleVersionedCatalogueSchema(const std::string& schemaVersion) {
    try {
      sql = AllCatalogueSchema::mapSchema.at(schemaVersion).at("oracle");
    } catch(std::out_of_range &) {
        throw exception::Exception(std::string("No such catalogue version ") + schemaVersion);
    }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresVersionedCatalogueSchema::PostgresVersionedCatalogueSchema(const std::string& schemaVersion) {
    try {
      sql = AllCatalogueSchema::mapSchema.at(schemaVersion).at("postgres");
    } catch(std::out_of_range &) {
        throw exception::Exception(std::string("No such catalogue version ") + schemaVersion);
    }
}


} // namespace cta::catalogue
