/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/SchemaCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class ConnPool;
}

namespace catalogue {

class RdbmsSchemaCatalogue : public SchemaCatalogue {
public:
  RdbmsSchemaCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsSchemaCatalogue() override = default;

  SchemaVersion getSchemaVersion() const override;

  void verifySchemaVersion() override;

  void ping() override;

  // This method is for unit tests only in InMemoryCatalogueTest.cpp, it's not defined in the interface
  /**
   * Returns the names of all the tables in the database schema in alphabetical
   * order.
   *
   * @return The names of all the tables in the database schema in alphabetical
   * order.
   */
  std::list<std::string> getTableNames() const;

private:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
};

}} // namespace cta::catalogue
