/*
 * SPDX-FileCopyrightText: 2015 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CatalogueSchema.hpp"

#include <string>

namespace cta::catalogue {

struct OracleVersionedCatalogueSchema : public CatalogueSchema {
  /**
   * Constructor
   */
  explicit OracleVersionedCatalogueSchema(const std::string& catalogueVersion);
};

struct PostgresVersionedCatalogueSchema : public CatalogueSchema {
  /**
   * Constructor
   */
  explicit PostgresVersionedCatalogueSchema(const std::string& catalogueVersion);
};

}  // namespace cta::catalogue
