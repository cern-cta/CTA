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
struct UpdatePhysicalLibrary;
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

  virtual void modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
   common::dataStructures::UpdatePhysicalLibrary& pl) = 0;
};

}} // namespace cta::catalogue
