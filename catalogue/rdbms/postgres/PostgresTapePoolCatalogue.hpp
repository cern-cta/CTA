/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsTapePoolCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class PostgresTapePoolCatalogue : public RdbmsTapePoolCatalogue {
public:
  PostgresTapePoolCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresTapePoolCatalogue() override = default;

private:
  uint64_t getNextTapePoolId(rdbms::Conn &conn) const override;
};  // class PostgresTapePoolCatalogue

} // namespace cta::catalogue
