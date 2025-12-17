/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"

#include <string>

namespace cta::catalogue {

class RdbmsCatalogue;

class PostgresMediaTypeCatalogue : public RdbmsMediaTypeCatalogue {
public:
  PostgresMediaTypeCatalogue(log::Logger& log,
                             std::shared_ptr<rdbms::ConnPool> connPool,
                             RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresMediaTypeCatalogue() override = default;

private:
  uint64_t getNextMediaTypeId(rdbms::Conn& conn) const override;
};  // class PostgresMediaTypeCatalogue

}  // namespace cta::catalogue
