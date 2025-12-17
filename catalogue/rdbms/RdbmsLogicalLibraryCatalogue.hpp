/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/LogicalLibraryCatalogue.hpp"

#include <list>
#include <memory>
#include <optional>
#include <string>

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace log {
class Logger;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsLogicalLibraryCatalogue : public LogicalLibraryCatalogue {
public:
  ~RdbmsLogicalLibraryCatalogue() override = default;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const bool isDisabled,
                            const std::optional<std::string>& physicalLibraryName,
                            const std::string& comment) override;

  void deleteLogicalLibrary(const std::string& name) override;

  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;

  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                const std::string& currentName,
                                const std::string& newName) override;

  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin,
                                   const std::string& name,
                                   const std::string& comment) override;

  void modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& name,
                                           const std::string& physicalLibraryName) override;

  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity& admin,
                                          const std::string& name,
                                          const std::string& disabledReason) override;

  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const bool disabledValue) override;

protected:
  RdbmsLogicalLibraryCatalogue(log::Logger& log,
                               std::shared_ptr<rdbms::ConnPool> connPool,
                               RdbmsCatalogue* rdbmsCatalogue);

  /**
   * Returns a unique logical library ID that can be used by a new logical
   * library within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique logical library ID that can be used by a new logical
   * library storage class within the catalogue.
   */
  virtual uint64_t getNextLogicalLibraryId(rdbms::Conn& conn) const = 0;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  friend class RdbmsTapeCatalogue;
  std::optional<uint64_t> getLogicalLibraryId(rdbms::Conn& conn, const std::string& name) const;
};

}  // namespace catalogue
}  // namespace cta
