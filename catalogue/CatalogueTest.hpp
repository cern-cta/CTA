/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <set>

namespace unitTests {

class cta_catalogue_CatalogueTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory **> {
public:

  cta_catalogue_CatalogueTest();

protected:

  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  const cta::common::dataStructures::SecurityIdentity m_localAdmin;
  const cta::common::dataStructures::SecurityIdentity m_admin;
  const cta::common::dataStructures::VirtualOrganization m_vo;
  const cta::common::dataStructures::VirtualOrganization m_anotherVo;
  const cta::common::dataStructures::StorageClass m_storageClassSingleCopy;
  const cta::common::dataStructures::StorageClass m_anotherStorageClass;
  const cta::common::dataStructures::StorageClass m_storageClassDualCopy;
  const cta::common::dataStructures::StorageClass m_storageClassTripleCopy;
  const cta::common::dataStructures::DiskInstance m_diskInstance;
  const cta::catalogue::MediaType m_mediaType;
  const cta::catalogue::CreateTapeAttributes m_tape1;
  const cta::catalogue::CreateTapeAttributes m_tape2;
  const cta::catalogue::CreateTapeAttributes m_tape3;

  virtual void SetUp();

  virtual void TearDown();

  /**
   * Creates a map from logical library name to logical library given the
   * specified list of logical libraries.
   *
   * @param listOfLibs The list of logical libraries from which the map shall
   * be created.
   * @return The map from logical library name to logical library.
   */
  std::map<std::string, cta::common::dataStructures::LogicalLibrary> logicalLibraryListToMap(
    const std::list<cta::common::dataStructures::LogicalLibrary> &listOfLibs);

  /**
   * Creates a map from VID to tape given the specified list of tapes.
   *
   * @param listOfTapes The list of tapes from which the map shall be created.
   * @return The map from VID to tape.
   */
  std::map<std::string, cta::common::dataStructures::Tape> tapeListToMap(
    const std::list<cta::common::dataStructures::Tape> &listOfTapes);

  /**
   * Creates a map from archive file ID to archive file from the specified
   * iterator.
   *
   * @param itor Iterator over archive files.
   * @return Map from archive file ID to archive file.
   */
  std::map<uint64_t, cta::common::dataStructures::ArchiveFile> archiveFileItorToMap(
    cta::catalogue::Catalogue::ArchiveFileItor &itor);

  /**
   * Creates a map from archive file ID to archive file from the specified
   * list of archive files.
   *
   * @param listOfArchiveFiles The list of archive files from which the map
   * shall be created.
   * @return Map from archive file ID to archive file.
   */
  std::map<uint64_t, cta::common::dataStructures::ArchiveFile> archiveFileListToMap(
    const std::list<cta::common::dataStructures::ArchiveFile> &listOfArchiveFiles);

  /**
   * Creates a map from admin username to admin user from the specified list of
   * admin users.
   *
   * @param listOfAdminUsers The list of admin users.
   * @return Map from username to admin user.
   */
  std::map<std::string, cta::common::dataStructures::AdminUser> adminUserListToMap(
    const std::list<cta::common::dataStructures::AdminUser> &listOfAdminUsers);

  /**
   * Creates a map from tape meida type name to tape media type from the
   * specified list of tape media types.
   *
   * @param listOfMediaTypes The list of tape media types.
   * @return Map from tape media type name to tape media type.
   */
  std::map<std::string, cta::catalogue::MediaTypeWithLogs> mediaTypeWithLogsListToMap(
    const std::list<cta::catalogue::MediaTypeWithLogs> &listOfMediaTypes);

  /**
   * Creates a map from tape pool name to tape pool from the specified list of
   * tape pools.
   *
   * @param listOfTapePools The list of tape pools.
   * @return Map from tape pool name to tape pool.
   */
  std::map<std::string, cta::catalogue::TapePool> tapePoolListToMap(
    const std::list<cta::catalogue::TapePool> &listOfTapePools);

}; // cta_catalogue_CatalogueTest

} // namespace unitTests
