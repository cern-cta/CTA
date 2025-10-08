/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CatalogueSchema.hpp"

#include <string>

namespace cta::catalogue {

/**
 * Structure containing the SQL to create the schema of the in memory CTA
 * database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * PostgresCatalogueSchema.cpp from:
 *   - PostgresCatalogueSchema.before_SQL.cpp
 *   - postgres_catalogue_schema.sql
 *
 * The PostgresSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct PostgresCatalogueSchema: public CatalogueSchema {
  /**
   * Constructor.
   */
  PostgresCatalogueSchema();
};

} // namespace cta::catalogue
