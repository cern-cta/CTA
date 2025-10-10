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
struct SecurityIdentity;
struct StorageClass;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringStorageClassName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVo);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByArchiveFiles);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByArchiveRoutes);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByFileRecycleLogs);

class StorageClassCatalogue {
public:
  virtual ~StorageClassCatalogue() = default;

  /**
   * Creates the specified storage class.
   *
   * @param admin The administrator.
   * @param storageClass The storage class.
   */
  virtual void createStorageClass(
    const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::StorageClass &storageClass) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   */
  virtual void deleteStorageClass(const std::string &storageClassName) = 0;

  virtual std::list<common::dataStructures::StorageClass> getStorageClasses() const = 0;

  virtual common::dataStructures::StorageClass getStorageClass(const std::string &name) const = 0;

  virtual void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t nbCopies) = 0;

  virtual void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) = 0;

  virtual void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) = 0;

  /**
   * Modifies the name of the specified storage class.
   *
   * @param currentName The current name of the storage class.
   * @param newName The new name of the storage class.
   */
  virtual void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) = 0;
};

}} // namespace cta::catalogue
