/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/SqliteCatalogueSchema.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogueSchema::SqliteCatalogueSchema(): CatalogueSchema(
  // CTA_SQL_SCHEMA - The contents of sqlite_catalogue_schema.cpp go here
  ) {
}

} // namespace cta::catalogue
