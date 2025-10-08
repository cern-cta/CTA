/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/wrapper/ConnFactory.hpp"
#include "rdbms/Login.hpp"

#include <vector>

namespace cta::rdbms::wrapper {

/**
 * A concrete factory of Conn objects.
 */
class PostgresConnFactory: public ConnFactory {
public:
  /**
   * Constructor
   *
   * @param login The database login information.
   */
  explicit PostgresConnFactory(const rdbms::Login& login);

  /**
   * Destructor.
   */
  ~PostgresConnFactory() override = default;

  /**
   * Returns a newly created database connection.
   *
   * @return A newly created database connection.
   */
  std::unique_ptr<ConnWrapper> create() override;

  /**
   * @brief Get the database system identifier.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.system`. It identifies which kind of database management system
   * (DBMS) is in use.
   *
   * @return The string "oracle".
   */
  std::string getDbSystemName() const override;

  /**
   * @brief Get the logical database namespace.
   *
   * This should return the OpenTelemetry semantic convention attribute
   * `db.namespace`. It identifies the logical database, schema, or
   * namespace where the operation is executed.
   *
   * @return A string representing the database namespace.
   */
  std::string getDbNamespace() const override;

private:
  /**
   * The database login information.
   */
  const rdbms::Login m_login;

}; // class PostgresConnFactory

} // namespace cta::rdbms::wrapper
