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

#include "catalogue/interfaces/PhysicalLibraryCatalogue.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct PhysicalLibrary;
struct SecurityIdentity;
}
}

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class PhysicalLibraryCatalogueRetryWrapper: public PhysicalLibraryCatalogue {
public:
  PhysicalLibraryCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~PhysicalLibraryCatalogueRetryWrapper() override = default;

  void createPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::PhysicalLibrary& pl) override;

  void deletePhysicalLibrary(const std::string &name) override;

  std::list<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const override;

  void modifyPhysicalLibraryGuiUrl   (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const std::string &guiUrl) override;

  void modifyPhysicalLibraryWebcamUrl(const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const std::string &webcamUrl) override;

  void modifyPhysicalLibraryLocation (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const std::string &location) override;

  void modifyPhysicalLibraryNbPhysicalCartridgeSlots  (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const uint64_t &nbPhysicalCartridgeSlots) override;

  void modifyPhysicalLibraryNbAvailableCartridgeSlots (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const uint64_t &nbAvailableCartridgeSlots) override;

  void modifyPhysicalLibraryNbPhysicalDriveSlots (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const uint64_t &nbPhysicalDriveSlots) override;

  void modifyPhysicalLibraryComment (const common::dataStructures::SecurityIdentity &admin,
   const std::string &name, const std::string &comment) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class PhysicalLibraryCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta