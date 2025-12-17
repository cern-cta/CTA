/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsTapePoolCatalogue.hpp"

#include <string>

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteTapePoolCatalogue : public RdbmsTapePoolCatalogue {
public:
  SqliteTapePoolCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteTapePoolCatalogue() override = default;

private:
  uint64_t getNextTapePoolId(rdbms::Conn& conn) const override;
};  // class SqliteTapePoolCatalogue

}  // namespace cta::catalogue
