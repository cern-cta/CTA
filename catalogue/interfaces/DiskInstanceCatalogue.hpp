/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <string>
#include <vector>

namespace cta {

namespace common::dataStructures {
struct DiskInstance;
struct SecurityIdentity;
}  // namespace common::dataStructures

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringDiskInstanceName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyDiskInstanceAfterDelete);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentDiskInstance);

class DiskInstanceCatalogue {
public:
  virtual ~DiskInstanceCatalogue() = default;

  virtual void createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                                  const std::string& name,
                                  const std::string& comment) = 0;

  virtual void deleteDiskInstance(const std::string& name) = 0;

  virtual void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                         const std::string& name,
                                         const std::string& comment) = 0;

  virtual std::vector<common::dataStructures::DiskInstance> getAllDiskInstances() const = 0;
};

}  // namespace catalogue
}  // namespace cta
