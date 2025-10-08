/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteStorageClassCatalogue : public RdbmsStorageClassCatalogue {
public:
  SqliteStorageClassCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteStorageClassCatalogue() override = default;

private:
  uint64_t getNextStorageClassId(rdbms::Conn &conn) override;
};  // class SqliteStorageClassCatalogue

} // namespace cta::catalogue
