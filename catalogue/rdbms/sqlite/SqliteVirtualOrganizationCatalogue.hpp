/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsVirtualOrganizationCatalogue.hpp"

#include <string>

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteVirtualOrganizationCatalogue : public RdbmsVirtualOrganizationCatalogue {
public:
  SqliteVirtualOrganizationCatalogue(log::Logger& log,
                                     std::shared_ptr<rdbms::ConnPool> connPool,
                                     RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteVirtualOrganizationCatalogue() override = default;

private:
  uint64_t getNextVirtualOrganizationId(rdbms::Conn& conn) override;
};  // class SqliteFileRecycleLogCatalogue

}  // namespace cta::catalogue
