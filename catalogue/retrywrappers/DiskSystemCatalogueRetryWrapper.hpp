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

#include "catalogue/interfaces/DiskSystemCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class DiskSystemCatalogueRetryWrapper : public DiskSystemCatalogue {
public:
  DiskSystemCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~DiskSystemCatalogueRetryWrapper() override = default;

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp,
    const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment) override;

  void deleteDiskSystem(const std::string &name) override;

  disk::DiskSystemList getAllDiskSystems() const override;

  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &fileRegexp) override;

  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t targetedFreeSpace) override;

  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
    const std::string& name, const uint64_t sleepTime) override;

  void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceName) override;

  void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceSpaceName) override;

  bool diskSystemExists(const std::string &name) const override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class DiskSystemCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta