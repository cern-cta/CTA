/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <string>

#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class OracleStorageClassCatalogue : public RdbmsStorageClassCatalogue {
public:
  OracleStorageClassCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~OracleStorageClassCatalogue() override = default;

private:
  uint64_t getNextStorageClassId(rdbms::Conn &conn) override;
};  // class PostgresStorageClassCatalogue

} // namespace cta::catalogue
