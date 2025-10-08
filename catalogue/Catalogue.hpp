/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

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
#include "catalogue/interfaces/RequesterGroupMountRuleCatalogue.hpp"
#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"
#include "catalogue/interfaces/RequesterMountRuleCatalogue.hpp"
#include "catalogue/interfaces/SchemaCatalogue.hpp"
#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "catalogue/interfaces/TapeCatalogue.hpp"
#include "catalogue/interfaces/TapeFileCatalogue.hpp"
#include "catalogue/interfaces/TapePoolCatalogue.hpp"
#include "catalogue/interfaces/VirtualOrganizationCatalogue.hpp"

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
  virtual const std::unique_ptr<SchemaCatalogue>& Schema() = 0;
  virtual const std::unique_ptr<AdminUserCatalogue>& AdminUser() = 0;
  virtual const std::unique_ptr<DiskSystemCatalogue>& DiskSystem() = 0;
  virtual const std::unique_ptr<DiskInstanceCatalogue>& DiskInstance() = 0;
  virtual const std::unique_ptr<DiskInstanceSpaceCatalogue>& DiskInstanceSpace() = 0;
  virtual const std::unique_ptr<VirtualOrganizationCatalogue>& VO() = 0;
  virtual const std::unique_ptr<ArchiveRouteCatalogue>& ArchiveRoute() = 0;
  virtual const std::unique_ptr<MediaTypeCatalogue>& MediaType() = 0;
  virtual const std::unique_ptr<StorageClassCatalogue>& StorageClass() = 0;
  virtual const std::unique_ptr<TapePoolCatalogue>& TapePool() = 0;
  virtual const std::unique_ptr<TapeCatalogue>& Tape() = 0;
  virtual const std::unique_ptr<MountPolicyCatalogue>& MountPolicy() = 0;
  virtual const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RequesterActivityMountRule() = 0;
  virtual const std::unique_ptr<RequesterMountRuleCatalogue>& RequesterMountRule() = 0;
  virtual const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RequesterGroupMountRule() = 0;
  virtual const std::unique_ptr<LogicalLibraryCatalogue>& LogicalLibrary() = 0;
  virtual const std::unique_ptr<PhysicalLibraryCatalogue>& PhysicalLibrary() = 0;
  virtual const std::unique_ptr<TapeFileCatalogue>& TapeFile() = 0;
  virtual const std::unique_ptr<FileRecycleLogCatalogue>& FileRecycleLog() = 0;
  virtual const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() = 0;
  virtual const std::unique_ptr<DriveStateCatalogue>& DriveState() = 0;
  virtual const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() = 0;
}; // class Catalogue

} // namespace cta::catalogue

