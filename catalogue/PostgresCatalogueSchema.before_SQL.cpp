/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/PostgresCatalogueSchema.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
PostgresCatalogueSchema::PostgresCatalogueSchema(): CatalogueSchema(
  // CTA_SQL_SCHEMA - The contents of postgres_catalogue_schema.cpp go here
  ) {
}

} // namespace cta::catalogue
