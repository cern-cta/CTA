/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "rdbms/Login.hpp"
#include "rdbms/wrapper/ConnFactory.hpp"

#include <vector>

namespace cta::rdbms::wrapper {

/**
 * A concrete factory of Conn objects.
 */
class PostgresConnFactory : public ConnFactory {
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

};  // class PostgresConnFactory

}  // namespace cta::rdbms::wrapper
