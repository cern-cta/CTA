/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "CatalogueSchema.hpp"

#include <string>

namespace cta {
namespace catalogue {

/**
 * Structure containing the SQL to create the schema of the in memory CTA
 * database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * OracleCatalogueSchema.cpp from:
 *   - OracleCatalogueSchema.before_SQL.cpp
 *   - oracle_catalogue_schema.sql
 *
 * The OracleSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct OracleCatalogueSchema : public CatalogueSchema {
  /**
   * Constructor.
   */
  OracleCatalogueSchema();
};

}  // namespace catalogue
}  // namespace cta
