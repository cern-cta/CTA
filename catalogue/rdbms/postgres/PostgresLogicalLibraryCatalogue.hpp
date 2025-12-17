/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsLogicalLibraryCatalogue.hpp"

#include <string>

namespace cta::catalogue {

class RdbmsCatalogue;

class PostgresLogicalLibraryCatalogue : public RdbmsLogicalLibraryCatalogue {
public:
  PostgresLogicalLibraryCatalogue(log::Logger& log,
                                  std::shared_ptr<rdbms::ConnPool> connPool,
                                  RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresLogicalLibraryCatalogue() override = default;

private:
  uint64_t getNextLogicalLibraryId(rdbms::Conn& conn) const override;
};  // class SqliteFileRecycleLogCatalogue

}  // namespace cta::catalogue
