/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/OracleCatalogueSchema.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogueSchema::OracleCatalogueSchema()
    : CatalogueSchema(
        // CTA_SQL_SCHEMA - The contents of oracle_catalogue_schema.cpp go here
      ) {}

}  // namespace cta::catalogue
