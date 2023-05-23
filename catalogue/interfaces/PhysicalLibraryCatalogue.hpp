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
#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common::dataStructures {
struct PhysicalLibrary;
struct SecurityIdentity;
} // namespace common::dataStructures

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringPhysicalLibraryName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyPhysicalLibrary);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentPhysicalLibrary);


class PhysicalLibraryCatalogue {
public:
  virtual ~PhysicalLibraryCatalogue() = default;

  virtual void createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::PhysicalLibrary& pl) = 0;

  virtual void deletePhysicalLibrary(const std::string& name) = 0;

  virtual std::list<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const = 0;

  virtual void modifyPhysicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
   const std::string& currentName, const std::string& newName) = 0;

  virtual void modifyPhysicalLibraryGuiUrl(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const std::string& guiUrl) = 0;

  virtual void modifyPhysicalLibraryWebcamUrl(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const std::string& webcamUrl) = 0;

  virtual void modifyPhysicalLibraryLocation(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const std::string& location) = 0;

  virtual void modifyPhysicalLibraryNbPhysicalCartridgeSlots(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const uint64_t& nbPhysicalCartridgeSlots) = 0;

  virtual void modifyPhysicalLibraryNbAvailableCartridgeSlots(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const uint64_t& nbAvailableCartridgeSlots) = 0;

  virtual void modifyPhysicalLibraryNbPhysicalDriveSlots(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const uint64_t& nbPhysicalDriveSlots) = 0;

  virtual void modifyPhysicalLibraryComment(const common::dataStructures::SecurityIdentity& admin,
   const std::string& name, const std::string& comment) = 0;
};

} // namespace catalogue
} // namespace cta