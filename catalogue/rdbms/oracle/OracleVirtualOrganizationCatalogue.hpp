/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsVirtualOrganizationCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class OracleVirtualOrganizationCatalogue : public RdbmsVirtualOrganizationCatalogue {
public:
  OracleVirtualOrganizationCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~OracleVirtualOrganizationCatalogue() override = default;

private:
  uint64_t getNextVirtualOrganizationId(rdbms::Conn &conn) override;
};  // class OracleFileRecycleLogCatalogue

} // namespace cta::catalogue
