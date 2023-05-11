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

#include "catalogue/Catalogue.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/PhysicalLibraryCatalogueTest.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_PhysicalLibraryTest::cta_catalogue_PhysicalLibraryTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin("admin", "admin", "admin", ""),
    m_physicalLibrary1(CatalogueTestUtils::getPhysicalLibrary1()),
    m_physicalLibrary2(CatalogueTestUtils::getPhysicalLibrary2()) {
}

void cta_catalogue_PhysicalLibraryTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_PhysicalLibraryTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_PhysicalLibraryTest, createPhysicalLibraryMandatoryFields) {
  ASSERT_TRUE(m_catalogue->PhysicalLibrary()->getPhysicalLibraries().empty());

  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary1);

  const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

  ASSERT_EQ(1, libs.size());

  const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
  ASSERT_EQ(m_physicalLibrary1.name, lib.name);
  ASSERT_EQ(m_physicalLibrary1.manufacturer, lib.manufacturer);
  ASSERT_EQ(m_physicalLibrary1.model, lib.model);
  ASSERT_EQ(m_physicalLibrary1.nbPhysicalCartridgeSlots, lib.nbPhysicalCartridgeSlots);
  ASSERT_EQ(m_physicalLibrary1.nbPhysicalDriveSlots, lib.nbPhysicalDriveSlots);

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_PhysicalLibraryTest, createPhysicalLibraryAllFields) {
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary2);

  const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

  ASSERT_EQ(1, libs.size());

  const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
  ASSERT_EQ(m_physicalLibrary2.name, lib.name);
  ASSERT_EQ(m_physicalLibrary2.manufacturer, lib.manufacturer);
  ASSERT_EQ(m_physicalLibrary2.model, lib.model);
  ASSERT_EQ(m_physicalLibrary2.type.value(), lib.type.value());
  ASSERT_EQ(m_physicalLibrary2.guiUrl.value(), lib.guiUrl.value());
  ASSERT_EQ(m_physicalLibrary2.webcamUrl.value(), lib.webcamUrl.value());
  ASSERT_EQ(m_physicalLibrary2.location.value(), lib.location.value());
  ASSERT_EQ(m_physicalLibrary2.nbPhysicalCartridgeSlots, lib.nbPhysicalCartridgeSlots);
  ASSERT_EQ(m_physicalLibrary2.nbAvailableCartridgeSlots.value(), lib.nbAvailableCartridgeSlots.value());
  ASSERT_EQ(m_physicalLibrary2.nbPhysicalDriveSlots, lib.nbPhysicalDriveSlots);

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

}  // namespace unitTests
