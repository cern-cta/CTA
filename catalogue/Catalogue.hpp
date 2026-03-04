/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/AdminUserCatalogue.hpp"
#include "catalogue/interfaces/ArchiveFileCatalogue.hpp"
#include "catalogue/interfaces/ArchiveRouteCatalogue.hpp"
#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"
#include "catalogue/interfaces/DiskSystemCatalogue.hpp"
#include "catalogue/interfaces/DriveConfigCatalogue.hpp"
#include "catalogue/interfaces/DriveStateCatalogue.hpp"
#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "catalogue/interfaces/LogicalLibraryCatalogue.hpp"
#include "catalogue/interfaces/MediaTypeCatalogue.hpp"
#include "catalogue/interfaces/MountPolicyCatalogue.hpp"
#include "catalogue/interfaces/PhysicalLibraryCatalogue.hpp"
#include "catalogue/interfaces/RequesterActivityMountRuleCatalogue.hpp"
#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"
#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"
#include "catalogue/interfaces/SchemaCatalogue.hpp"
#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "catalogue/interfaces/TapeCatalogue.hpp"
#include "catalogue/interfaces/TapeFileCatalogue.hpp"
#include "catalogue/interfaces/TapePoolCatalogue.hpp"
#include "catalogue/interfaces/VirtualOrganizationCatalogue.hpp"

#include <memory>

namespace cta::catalogue {

/**
 * Abstract class defining the interface to the CTA catalogue responsible for
 * storing critical information about archive files, tapes and tape files.
 */
class Catalogue {
public:
  /**
   * Destructor.
   */
  virtual ~Catalogue() = default;

  // Table Methods
  virtual const std::unique_ptr<SchemaCatalogue>& Schema() const = 0;
  virtual const std::unique_ptr<AdminUserCatalogue>& AdminUser() const = 0;
  virtual const std::unique_ptr<DiskSystemCatalogue>& DiskSystem() const = 0;
  virtual const std::unique_ptr<DiskInstanceCatalogue>& DiskInstance() const = 0;
  virtual const std::unique_ptr<DiskInstanceSpaceCatalogue>& DiskInstanceSpace() const = 0;
  virtual const std::unique_ptr<VirtualOrganizationCatalogue>& VO() const = 0;
  virtual const std::unique_ptr<ArchiveRouteCatalogue>& ArchiveRoute() const = 0;
  virtual const std::unique_ptr<MediaTypeCatalogue>& MediaType() const = 0;
  virtual const std::unique_ptr<StorageClassCatalogue>& StorageClass() const = 0;
  virtual const std::unique_ptr<TapePoolCatalogue>& TapePool() const = 0;
  virtual const std::unique_ptr<TapeCatalogue>& Tape() const = 0;
  virtual const std::unique_ptr<MountPolicyCatalogue>& MountPolicy() const = 0;
  virtual const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RequesterActivityMountRule() const = 0;
  virtual const std::unique_ptr<RequesterMountRuleCatalogue>& RequesterMountRule() const = 0;
  virtual const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RequesterGroupMountRule() const = 0;
  virtual const std::unique_ptr<LogicalLibraryCatalogue>& LogicalLibrary() const = 0;
  virtual const std::unique_ptr<PhysicalLibraryCatalogue>& PhysicalLibrary() const = 0;
  virtual const std::unique_ptr<TapeFileCatalogue>& TapeFile() const = 0;
  virtual const std::unique_ptr<FileRecycleLogCatalogue>& FileRecycleLog() const = 0;
  virtual const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() const = 0;
  virtual const std::unique_ptr<DriveStateCatalogue>& DriveState() const = 0;
  virtual const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() const = 0;
};  // class Catalogue

}  // namespace cta::catalogue
