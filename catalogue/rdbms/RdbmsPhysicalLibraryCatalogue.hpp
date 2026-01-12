/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/PhysicalLibraryCatalogue.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
class Stmt;
}  // namespace rdbms

namespace log {
class Logger;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsPhysicalLibraryCatalogue : public PhysicalLibraryCatalogue {
public:
  ~RdbmsPhysicalLibraryCatalogue() override = default;

  void createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                             const common::dataStructures::PhysicalLibrary& pl) override;

  void deletePhysicalLibrary(const std::string& name) override;

  std::vector<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const override;

  void modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                             const common::dataStructures::UpdatePhysicalLibrary& pl) override;

protected:
  RdbmsPhysicalLibraryCatalogue(log::Logger& log,
                                std::shared_ptr<rdbms::ConnPool> connPool,
                                RdbmsCatalogue* rdbmsCatalogue);

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
  RdbmsCatalogue* m_rdbmsCatalogue;

  friend class RdbmsLogicalLibraryCatalogue;
  std::optional<uint64_t> getPhysicalLibraryId(rdbms::Conn& conn, const std::string& name) const;

  std::string buildUpdateStmtStr(const common::dataStructures::UpdatePhysicalLibrary& pl) const;
  void bindUpdateParams(cta::rdbms::Stmt& stmt,
                        const common::dataStructures::UpdatePhysicalLibrary& pl,
                        const common::dataStructures::SecurityIdentity& admin,
                        const time_t now) const;
};

}  // namespace catalogue
}  // namespace cta
