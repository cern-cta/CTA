/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <string>

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
