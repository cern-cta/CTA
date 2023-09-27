/*
 * @project	 The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license	 This program is free software, distributed under the terms of the GNU General Public
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

#include <memory>
#include <string>

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/PostgresSchedDB/PostgresSchedDB.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "rdbms/Login.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

class PostgresSchedDBWrapper: public SchedulerDatabaseDecorator {

private:
    std::unique_ptr <cta::log::Logger> m_logger;
    cta::catalogue::Catalogue& m_catalogue;
    PostgresSchedDB m_PostgresSchedDB;

public:
  PostgresSchedDBWrapper(const std::string &ownerId,
                         std::unique_ptr<cta::log::Logger> logger,
                         catalogue::Catalogue &catalogue,
                         const rdbms::Login &login,
                         const uint64_t nbConns) :
      SchedulerDatabaseDecorator(m_PostgresSchedDB),
      m_logger(std::move(logger)), m_catalogue(catalogue),
      m_PostgresSchedDB(ownerId, *logger, catalogue, login, nbConns)
   {
     // empty
   }

  ~PostgresSchedDBWrapper() throw() {}

};

/**
 * A concrete implementation of a scheduler database factory that creates mock
 * scheduler database objects.
 */
class PostgresSchedDBFactory: public SchedulerDatabaseFactory {
public:
  /**
   * Constructor
   */
  explicit PostgresSchedDBFactory(const std::string & URL = ""): m_URL(URL) {}

  /**
   * Destructor.
   */
  ~PostgresSchedDBFactory() throw() {}

  /**
   * Returns a newly created scheduler database object.
   *
   * @return A newly created scheduler database object.
   */
  std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue> &catalogue) const {
    auto dummylogger = std::make_unique<cta::log::DummyLogger>("","");
    auto logger = std::unique_ptr<cta::log::Logger>(std::move(dummylogger));

    cta::rdbms::Login login(cta::rdbms::Login::DBTYPE_POSTGRESQL, "user", "password", "dbname", "host", 0);

    auto pgwrapper = std::make_unique<PostgresSchedDBWrapper>("UnitTest", std::move(logger), *catalogue, login, 2);
    return std::unique_ptr<SchedulerDatabase>(std::move(pgwrapper));
  }

  private:
    std::string m_URL;
};  // class PostgresSchedDBFactory

} // namespace cta
