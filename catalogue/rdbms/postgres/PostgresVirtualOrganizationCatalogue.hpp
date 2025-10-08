/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsVirtualOrganizationCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class PostgresVirtualOrganizationCatalogue : public RdbmsVirtualOrganizationCatalogue {
public:
  PostgresVirtualOrganizationCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresVirtualOrganizationCatalogue() override = default;

protected:
  uint64_t getNextVirtualOrganizationId(rdbms::Conn &conn) override;
};  // class PostgresVirtualOrganizationCatalogue

} // namespace cta::catalogue
