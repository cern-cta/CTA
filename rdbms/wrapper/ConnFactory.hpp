/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "rdbms/wrapper/ConnWrapper.hpp"

#include <memory>

namespace cta::rdbms::wrapper {

/**
 * Abstract class that specifies the interface of a factory of Conn objects.
 */
class ConnFactory {
public:

  /**
   * Destructor.
   */
  virtual ~ConnFactory() = default;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  virtual std::unique_ptr<ConnWrapper> create() = 0;

  /**
   * @brief Get the database system identifier.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.system`. It identifies which kind of database management system
   * (DBMS) is in use. Typical values include:
   *   - "postgresql"
   *   - "mysql"
   *   - "oracle"
   *   - "other_sql"
   *
   * @return A string identifying the DBMS product.
   */
  virtual std::string getDbSystemName() const = 0;

  /**
   * @brief Get the logical database namespace.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.namespace`. It identifies the logical database, schema, or
   * namespace where the operation is executed.
   *
   * @return A string representing the database namespace.
   */
  virtual std::string getDbNamespace() const = 0;

}; // class ConnFactory

} // namespace cta::rdbms::wrapper
