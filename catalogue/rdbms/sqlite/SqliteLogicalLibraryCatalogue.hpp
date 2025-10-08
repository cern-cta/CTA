/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsLogicalLibraryCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteLogicalLibraryCatalogue : public RdbmsLogicalLibraryCatalogue {
public:
  SqliteLogicalLibraryCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteLogicalLibraryCatalogue() override = default;

private:
  uint64_t getNextLogicalLibraryId(rdbms::Conn &conn) const override;
};  // class SqliteFileRecycleLogCatalogue

} // namespace cta::catalogue
