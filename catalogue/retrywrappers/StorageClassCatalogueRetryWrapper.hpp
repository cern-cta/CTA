/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <string>

namespace cta::catalogue {

class Catalogue;

class StorageClassCatalogueRetryWrapper : public StorageClassCatalogue {
public:
  StorageClassCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~StorageClassCatalogueRetryWrapper() override = default;

  void createStorageClass(const common::dataStructures::SecurityIdentity& admin,
                          const common::dataStructures::StorageClass& storageClass) override;

  void deleteStorageClass(const std::string& storageClassName) override;

  std::vector<common::dataStructures::StorageClass> getStorageClasses() const override;

  common::dataStructures::StorageClass getStorageClass(const std::string& name) const override;

  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin,
                                  const std::string& name,
                                  const uint64_t nbCopies) override;

  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const std::string& comment) override;

  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const std::string& vo) override;

  void modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& currentName,
                              const std::string& newName) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class StorageClassCatalogueRetryWrapper

}  // namespace cta::catalogue
