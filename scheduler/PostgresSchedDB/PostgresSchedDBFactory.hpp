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

namespace cta {

namespace catalogue {
class Catalogue;
}

class PostgresSchedDBWrapper: public SchedulerDatabaseDecorator {
public:
  PostgresSchedDBWrapper(const std::string &context, std::unique_ptr<cta::catalogue::Catalogue>& catalogue, const std::string &URL = "") :
    SchedulerDatabaseDecorator(m_PostgresSchedDB), m_logger(new cta::log::DummyLogger("", "")), m_catalogue(catalogue),
    m_PostgresSchedDB(nullptr, *m_catalogue, *m_logger) {}

  ~PostgresSchedDBWrapper() throw() {}

private:
  std::unique_ptr <cta::log::Logger> m_logger;
  std::unique_ptr <cta::catalogue::Catalogue> & m_catalogue;
  cta::PostgresSchedDB m_PostgresSchedDB;
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
  std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue>& catalogue) const {
    return std::unique_ptr<SchedulerDatabase>(new PostgresSchedDBWrapper("UnitTest", catalogue, m_URL));
  }

  private:
    std::string m_URL;
};  // class PostgresSchedDBFactory

}  // namespace cta
