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
#include <string>

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

class RdbmsDiskInstanceCatalogue : public DiskInstanceCatalogue {
public:
  RdbmsDiskInstanceCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsDiskInstanceCatalogue() override = default;

  void createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                          const std::string& name,
                          const std::string& comment) override;

  void deleteDiskInstance(const std::string& name) override;

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const std::string& comment) override;

  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
};

}  // namespace catalogue
}  // namespace cta