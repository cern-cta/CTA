/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <memory>
#include <optional>
#include <string>

#include "catalogue/interfaces/PhysicalLibraryCatalogue.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace log {
class Logger;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsPhysicalLibraryCatalogue: public PhysicalLibraryCatalogue {
public:
  ~RdbmsPhysicalLibraryCatalogue() override = default;

  void createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::PhysicalLibrary& pl) override;

  void deletePhysicalLibrary(const std::string& name) override;

  std::list<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const override;

  void modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::UpdatePhysicalLibrary& pl) override;

protected:
  RdbmsPhysicalLibraryCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);

  /**
   * Returns a unique physical library ID that can be used by a new physical
   * library within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique logical library ID that can be used by a new physical
   * library within the catalogue.
   */
  virtual uint64_t getNextPhysicalLibraryId(rdbms::Conn& conn) const = 0;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue *m_rdbmsCatalogue;

  friend class RdbmsLogicalLibraryCatalogue;
  std::optional<uint64_t> getPhysicalLibraryId(rdbms::Conn& conn, const std::string& name) const;
};

} // namespace catalogue
} // namespace cta