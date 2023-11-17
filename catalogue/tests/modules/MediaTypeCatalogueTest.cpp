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

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/MediaTypeCatalogueTest.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_MediaTypeTest::cta_catalogue_MediaTypeTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_tape1(CatalogueTestUtils::getTape1()) {
}

void cta_catalogue_MediaTypeTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_MediaTypeTest::TearDown() {
  m_catalogue.reset();
}

std::map<std::string, cta::catalogue::MediaTypeWithLogs> cta_catalogue_MediaTypeTest::mediaTypeWithLogsListToMap(
  const std::list<cta::catalogue::MediaTypeWithLogs> &listOfMediaTypes) {
  try {
    std::map<std::string, cta::catalogue::MediaTypeWithLogs> m;

    for(auto &mediaType: listOfMediaTypes) {
      if(m.end() != m.find(mediaType.name)) {
        cta::exception::Exception ex;
        ex.getMessage() << "Media type " << mediaType.name << " is a duplicate";
        throw ex;
      }
      m[mediaType.name] = mediaType;
    }
    return m;
  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

  ASSERT_EQ(1, mediaTypes.size());

  ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
  ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
  ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
  ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
  ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
  ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
  ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
  ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

  const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType_same_twice) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  ASSERT_THROW(m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType_emptyStringMediaTypeName) {
  auto mediaType = m_mediaType;
  mediaType.name = "";
  ASSERT_THROW(m_catalogue->MediaType()->createMediaType(m_admin, mediaType),
    cta::catalogue::UserSpecifiedAnEmptyStringMediaTypeName);
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType_emptyStringComment) {
  auto mediaType = m_mediaType;
  mediaType.comment = "";
  ASSERT_THROW(m_catalogue->MediaType()->createMediaType(m_admin, mediaType),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType_emptyStringCartridge) {
  auto mediaType = m_mediaType;
  mediaType.cartridge = "";
  ASSERT_THROW(m_catalogue->MediaType()->createMediaType(m_admin, mediaType),
    cta::catalogue::UserSpecifiedAnEmptyStringCartridge);
}

TEST_P(cta_catalogue_MediaTypeTest, createMediaType_zeroCapacity) {
  auto mediaType = m_mediaType;
  mediaType.capacityInBytes = 0;
  ASSERT_THROW(m_catalogue->MediaType()->createMediaType(m_admin, mediaType),
    cta::catalogue::UserSpecifiedAZeroCapacity);
}

TEST_P(cta_catalogue_MediaTypeTest, deleteMediaType) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

  ASSERT_EQ(1, mediaTypes.size());

  ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
  ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
  ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
  ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
  ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
  ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
  ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
  ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

  const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->MediaType()->deleteMediaType(m_mediaType.name);

  ASSERT_TRUE(m_catalogue->MediaType()->getMediaTypes().empty());
}

TEST_P(cta_catalogue_MediaTypeTest, deleteMediaType_nonExistentMediaType) {
  ASSERT_THROW(m_catalogue->MediaType()->deleteMediaType("media_type"), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, deleteMediaType_usedByTapes) {
  cta::log::LogContext dummyLc(m_dummyLog);
  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  //Media type is used by at least one tape, deleting it should throw an exception
  ASSERT_THROW(m_catalogue->MediaType()->deleteMediaType(m_tape1.mediaType), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeName) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newMediaTypeName = "new_media_type";
  m_catalogue->MediaType()->modifyMediaTypeName(m_admin, m_mediaType.name, newMediaTypeName);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(newMediaTypeName, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeName_nonExistentMediaType) {
  const std::string currentName = "media_type";
  const std::string newName = "new_media_type";
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeName(m_admin, currentName, newName), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeName_newNameAlreadyExists) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  auto mediaType2 = m_mediaType;
  mediaType2.name = "media_type_2";

  m_catalogue->MediaType()->createMediaType(m_admin, mediaType2);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(2, mediaTypes.size());

    const auto mediaTypeMap = mediaTypeWithLogsListToMap(mediaTypes);

    ASSERT_EQ(2, mediaTypeMap.size());

    auto mediaType1Itor = mediaTypeMap.find(m_mediaType.name);
    ASSERT_TRUE(mediaType1Itor != mediaTypeMap.end());

    ASSERT_EQ(m_mediaType.name, mediaType1Itor->second.name);
    ASSERT_EQ(m_mediaType.cartridge, mediaType1Itor->second.cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaType1Itor->second.capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaType1Itor->second.primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaType1Itor->second.secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaType1Itor->second.nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaType1Itor->second.minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaType1Itor->second.maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaType1Itor->second.comment);

    const cta::common::dataStructures::EntryLog creationLog1 = mediaType1Itor->second.creationLog;
    ASSERT_EQ(m_admin.username, creationLog1.username);
    ASSERT_EQ(m_admin.host, creationLog1.host);

    const cta::common::dataStructures::EntryLog lastModificationLog1 = mediaType1Itor->second.lastModificationLog;
    ASSERT_EQ(creationLog1, lastModificationLog1);

    auto mediaType2Itor = mediaTypeMap.find(mediaType2.name);
    ASSERT_TRUE(mediaType2Itor != mediaTypeMap.end());

    ASSERT_EQ(mediaType2.name, mediaType2Itor->second.name);
    ASSERT_EQ(mediaType2.cartridge, mediaType2Itor->second.cartridge);
    ASSERT_EQ(mediaType2.capacityInBytes, mediaType2Itor->second.capacityInBytes);
    ASSERT_EQ(mediaType2.primaryDensityCode, mediaType2Itor->second.primaryDensityCode);
    ASSERT_EQ(mediaType2.secondaryDensityCode, mediaType2Itor->second.secondaryDensityCode);
    ASSERT_EQ(mediaType2.nbWraps, mediaType2Itor->second.nbWraps);
    ASSERT_EQ(mediaType2.minLPos, mediaType2Itor->second.minLPos);
    ASSERT_EQ(mediaType2.maxLPos, mediaType2Itor->second.maxLPos);
    ASSERT_EQ(mediaType2.comment, mediaType2Itor->second.comment);

    const cta::common::dataStructures::EntryLog creationLog2 = mediaType2Itor->second.creationLog;
    ASSERT_EQ(m_admin.username, creationLog2.username);
    ASSERT_EQ(m_admin.host, creationLog2.host);

    const cta::common::dataStructures::EntryLog lastModificationLog2 = mediaType2Itor->second.lastModificationLog;
    ASSERT_EQ(creationLog2, lastModificationLog2);
  }

  // Try to rename the first media type with the name of the second one
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeName(m_admin, m_mediaType.name, mediaType2.name),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeCartridge) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedCartridge = "new_cartridge";
  m_catalogue->MediaType()->modifyMediaTypeCartridge(m_admin, m_mediaType.name, modifiedCartridge);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(modifiedCartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeCartridge_nonExistentMediaType) {
  const std::string name = "media_type";
  const std::string cartridge = "cartride";
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeCartridge(m_admin, name, cartridge), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeCapacityInBytes) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedCapacityInBytes = m_mediaType.capacityInBytes + 7;
  m_catalogue->MediaType()->modifyMediaTypeCapacityInBytes(m_admin, m_mediaType.name, modifiedCapacityInBytes);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(modifiedCapacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeCapacityInBytes_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint64_t capacityInBytes = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeCapacityInBytes(m_admin, name, capacityInBytes),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypePrimaryDensityCode) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint8_t modifiedPrimaryDensityCode = 7;
  m_catalogue->MediaType()->modifyMediaTypePrimaryDensityCode(m_admin, m_mediaType.name, modifiedPrimaryDensityCode);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(modifiedPrimaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypePrimaryDensityCode_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint8_t primaryDensityCode = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypePrimaryDensityCode(m_admin, name, primaryDensityCode),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeSecondaryDensityCode) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint8_t modifiedSecondaryDensityCode = 7;
  m_catalogue->MediaType()->modifyMediaTypeSecondaryDensityCode(m_admin, m_mediaType.name, modifiedSecondaryDensityCode);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(modifiedSecondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeSecondaryDensityCode_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint8_t secondaryDensityCode = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeSecondaryDensityCode(m_admin, name, secondaryDensityCode),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeNbWraps) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint32_t modifiedNbWraps = 7;
  m_catalogue->MediaType()->modifyMediaTypeNbWraps(m_admin, m_mediaType.name, modifiedNbWraps);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(modifiedNbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeNbWraps_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint32_t nbWraps = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeNbWraps(m_admin, name, nbWraps), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeMinLPos) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinLPos = 7;
  m_catalogue->MediaType()->modifyMediaTypeMinLPos(m_admin, m_mediaType.name, modifiedMinLPos);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(modifiedMinLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeMinLPos_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint64_t minLPos = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeMinLPos(m_admin, name, minLPos), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeMaxLPos) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMaxLPos = 7;
  m_catalogue->MediaType()->modifyMediaTypeMaxLPos(m_admin, m_mediaType.name, modifiedMaxLPos);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(modifiedMaxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeMaxLPos_nonExistentMediaType) {
  const std::string name = "media_type";
  const uint64_t maxLPos = 1;
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeMaxLPos(m_admin, name, maxLPos), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeComment) {
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->MediaType()->modifyMediaTypeComment(m_admin, m_mediaType.name, modifiedComment);

  {
    const auto mediaTypes = m_catalogue->MediaType()->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(modifiedComment, mediaTypes.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_MediaTypeTest, modifyMediaTypeComment_nonExistentMediaType) {
  const std::string name = "media_type";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->MediaType()->modifyMediaTypeComment(m_admin, name, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_MediaTypeTest, getMediaTypeByVid_nonExistentTape) {
  ASSERT_THROW(m_catalogue->MediaType()->getMediaTypeByVid("DOES_NOT_EXIST"), cta::exception::Exception);
}

TEST_P(cta_catalogue_MediaTypeTest, getMediaTypeByVid) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  auto tapeMediaType = m_catalogue->MediaType()->getMediaTypeByVid(m_tape1.vid);
  ASSERT_EQ(m_mediaType.name, tapeMediaType.name);
  ASSERT_EQ(m_mediaType.capacityInBytes, tapeMediaType.capacityInBytes);
  ASSERT_EQ(m_mediaType.cartridge, tapeMediaType.cartridge);
  ASSERT_EQ(m_mediaType.comment, tapeMediaType.comment);
  ASSERT_EQ(m_mediaType.maxLPos, tapeMediaType.maxLPos);
  ASSERT_EQ(m_mediaType.minLPos, tapeMediaType.minLPos);
  ASSERT_EQ(m_mediaType.nbWraps, tapeMediaType.nbWraps);
  ASSERT_EQ(m_mediaType.primaryDensityCode, tapeMediaType.primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, tapeMediaType.secondaryDensityCode);
}

}  // namespace unitTests