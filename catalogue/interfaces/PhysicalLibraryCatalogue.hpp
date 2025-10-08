/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
   const common::dataStructures::UpdatePhysicalLibrary& pl) = 0;
};

}} // namespace cta::catalogue
