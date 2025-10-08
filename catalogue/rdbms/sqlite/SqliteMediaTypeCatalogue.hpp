/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteMediaTypeCatalogue : public RdbmsMediaTypeCatalogue {
public:
  SqliteMediaTypeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteMediaTypeCatalogue() override = default;

private:
  uint64_t getNextMediaTypeId(rdbms::Conn &conn) const override;
};  // class SqliteMediaTypeCatalogue

} // namespace cta::catalogue
