/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/ArchiveRouteCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

class RdbmsArchiveRouteCatalogue : public ArchiveRouteCatalogue {
public:
  RdbmsArchiveRouteCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsArchiveRouteCatalogue() override = default;

  void createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName,
    const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName, const std::string &comment) override;

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName,
    const std::string &tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType & archiveRouteType,
    const std::string &comment) override;

private:

  bool archiveRouteExists(rdbms::Conn &conn, const std::string &storageClassName, const uint32_t copyNb,
                          const common::dataStructures::ArchiveRouteType & archiveRouteType);

  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;

  /**
   * @return the archive routes of the given storage class and destination tape
   * pool.
   *
   * Under normal circumstances this method should return either 0 or 1 route.
   * For a given storage class there should be no more than one route to any
   * given tape pool.
   *
   * @param conn The database connection.
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @param tapePoolName The name of the tape pool.
   */
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(rdbms::Conn &conn,
    const std::string &storageClassName, const std::string &tapePoolName) const;
};

}} // namespace cta::catalogue
