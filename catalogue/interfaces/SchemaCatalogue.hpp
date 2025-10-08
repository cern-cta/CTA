/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::catalogue {

class SchemaVersion;

CTA_GENERATE_EXCEPTION_CLASS(WrongSchemaVersionException);

class SchemaCatalogue {
public:
  virtual ~SchemaCatalogue() = default;

  /**
   * Returns the SchemaVersion object corresponding to the catalogue schema version:
   * - SCHEMA_VERSION_MAJOR
   * - SCHEMA_VERSION_MINOR
   * - SCHEMA_VERSION_MAJOR_NEXT (future major version number of the schema in case of upgrade)
   * - SCHEMA_VERSION_MINOR_NEXT (future minor version number of the schema in case of upgrade)
   * - STATUS (UPGRADING or PRODUCTION)
   *
   * @return The SchemaVersion object corresponding to the catalogue schema version
   */
  virtual SchemaVersion getSchemaVersion() const = 0;

  /**
   * Checks that the online database schema MAJOR version number matches
   * the schema MAJOR version number defined in version.h
   */
  virtual void verifySchemaVersion() = 0;

  /**
   * Checks that the most trivial query goes through. Throws an exception if not.
   */
  virtual void ping() = 0;
};

} // namespace cta::catalogue
