/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class OracleMediaTypeCatalogue : public RdbmsMediaTypeCatalogue {
public:
  OracleMediaTypeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~OracleMediaTypeCatalogue() override = default;

private:
  uint64_t getNextMediaTypeId(rdbms::Conn &conn) const override;
};  // class PostgresMediaTypeCatalogue

} // namespace cta::catalogue
