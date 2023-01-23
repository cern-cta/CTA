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
    const uint32_t copyNb, const std::string &tapePoolName, const std::string &comment) override;

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName,
    const std::string &tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) override;

private:
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

} // namespace catalogue
} // namespace cta