/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsPhysicalLibraryCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class OraclePhysicalLibraryCatalogue : public RdbmsPhysicalLibraryCatalogue {
public:
  OraclePhysicalLibraryCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~OraclePhysicalLibraryCatalogue() override = default;

private:
  uint64_t getNextPhysicalLibraryId(rdbms::Conn &conn) const override;
};  // class SqliteFileRecycleLogCatalogue

} // namespace cta::catalogue
