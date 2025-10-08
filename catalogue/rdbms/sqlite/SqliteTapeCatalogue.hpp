/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsTapeCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteTapeCatalogue : public RdbmsTapeCatalogue {
public:
  SqliteTapeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteTapeCatalogue() override = default;

private:
  friend class SqliteTapeFileCatalogue;
  uint64_t getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) const override;
};  // class SqliteTapeCatalogue

} // namespace cta::catalogue
