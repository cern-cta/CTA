/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CatalogueSchema.hpp"

namespace cta::catalogue {

/**
 * Structure containing the SQL to create the schema of the CTA catalogue
 * database in an SQLite database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * SqliteCatalogueSchema.cpp from:
 *   - SqliteCatalogueSchema.before_SQL.cpp
 *   - sqlite_catalogue_schema.sql
 *
 * The SqliteCatalogueSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct SqliteCatalogueSchema: public CatalogueSchema {
  /**
   * Constructor.
   */
  SqliteCatalogueSchema();
};

} // namespace cta::catalogue
