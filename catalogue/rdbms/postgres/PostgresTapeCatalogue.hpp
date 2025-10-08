/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsTapeCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class PostgresTapeCatalogue : public RdbmsTapeCatalogue {
public:
  PostgresTapeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresTapeCatalogue() override = default;

private:
  uint64_t getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) const override;
};  // class PostgresTapeCatalogue

} // namespace cta::catalogue
