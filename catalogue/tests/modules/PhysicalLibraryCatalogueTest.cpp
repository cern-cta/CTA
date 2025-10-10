/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
#include "rdbms/ConstraintError.hpp"

namespace unitTests {

cta_catalogue_PhysicalLibraryTest::cta_catalogue_PhysicalLibraryTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin("admin", "admin", "admin", ""),
    m_physicalLibrary1(CatalogueTestUtils::getPhysicalLibrary1()),
    m_physicalLibrary2(CatalogueTestUtils::getPhysicalLibrary2()),
    m_physicalLibrary3(CatalogueTestUtils::getPhysicalLibrary3()) {
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
  ASSERT_EQ(m_physicalLibrary2.comment.value(), lib.comment.value());

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_PhysicalLibraryTest, modifyPhysicalLibrary) {
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary2);

  {
    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

    ASSERT_EQ(1, libs.size());
    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name                      = m_physicalLibrary2.name;
    pl.guiUrl                    = m_physicalLibrary3.guiUrl.value();
    pl.webcamUrl                 = m_physicalLibrary3.webcamUrl.value();
    pl.location                  = m_physicalLibrary3.location.value();
    pl.nbPhysicalCartridgeSlots  = m_physicalLibrary3.nbPhysicalCartridgeSlots;
    pl.nbAvailableCartridgeSlots = m_physicalLibrary3.nbAvailableCartridgeSlots.value();
    pl.nbPhysicalDriveSlots      = m_physicalLibrary3.nbPhysicalDriveSlots;
    pl.comment                   = m_physicalLibrary3.comment.value();

    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);
  }

  {
    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    ASSERT_EQ(m_physicalLibrary3.guiUrl.value(), lib.guiUrl.value());
    ASSERT_EQ(m_physicalLibrary3.webcamUrl.value(), lib.webcamUrl.value());
    ASSERT_EQ(m_physicalLibrary3.location.value(), lib.location.value());
    ASSERT_EQ(m_physicalLibrary3.nbPhysicalCartridgeSlots, lib.nbPhysicalCartridgeSlots);
    ASSERT_EQ(m_physicalLibrary3.nbAvailableCartridgeSlots.value(), lib.nbAvailableCartridgeSlots.value());
    ASSERT_EQ(m_physicalLibrary3.nbPhysicalDriveSlots, lib.nbPhysicalDriveSlots);
    ASSERT_EQ(m_physicalLibrary3.comment.value(), lib.comment.value());
  }
  // add a test here for disabled, disabled_reason
  {
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name = m_physicalLibrary2.name;
    pl.isDisabled = std::optional<bool>(true);
    pl.disabledReason = "physical library disabled reason";
    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);
  }

  {
    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    ASSERT_TRUE(lib.isDisabled);
    ASSERT_TRUE(lib.disabledReason.has_value());
    ASSERT_EQ(lib.disabledReason.value(), "physical library disabled reason");
  }
  // modify the disabled reason only
  {
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name = m_physicalLibrary2.name;
    pl.disabledReason = std::optional<std::string>("physical library disabled reason updated");
    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);

    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    ASSERT_TRUE(lib.isDisabled);
    ASSERT_TRUE(lib.disabledReason.has_value());
    ASSERT_EQ(lib.disabledReason.value(), "physical library disabled reason updated");
  }
  // now enable it again, the reason should not have been wiped
  {
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name = m_physicalLibrary2.name;
    pl.disabledReason = "physical library disabled reason updated";
    pl.isDisabled = false;
    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);

    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    ASSERT_FALSE(lib.isDisabled);
    ASSERT_TRUE(lib.disabledReason.has_value());
  }
  // try to disable without providing a reason
  {
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name = m_physicalLibrary2.name;
    pl.isDisabled = true;
    ASSERT_THROW(m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl), cta::exception::UserError);
  }
}

TEST_P(cta_catalogue_PhysicalLibraryTest, deletePhysicalLibrary) {
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary2);

  const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

  ASSERT_EQ(1, libs.size());

  m_catalogue->PhysicalLibrary()->deletePhysicalLibrary(m_physicalLibrary2.name);
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_PhysicalLibraryTest, addingSameNamePhysicalLibrary) {
  ASSERT_TRUE(m_catalogue->PhysicalLibrary()->getPhysicalLibraries().empty());

  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary1);

  auto shouldThrow = [this]() -> void {
    m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary1);
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

TEST_P(cta_catalogue_PhysicalLibraryTest, modifyNonExistentPhysicalLibrary) {
  auto shouldThrow = [this]() -> void {
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name                      = "doesNotExist";
    pl.guiUrl                    = m_physicalLibrary3.guiUrl.value();
    pl.webcamUrl                 = m_physicalLibrary3.webcamUrl.value();
    pl.location                  = m_physicalLibrary3.location.value();
    pl.nbPhysicalCartridgeSlots  = m_physicalLibrary3.nbPhysicalCartridgeSlots;
    pl.nbAvailableCartridgeSlots = m_physicalLibrary3.nbAvailableCartridgeSlots.value();
    pl.nbPhysicalDriveSlots      = m_physicalLibrary3.nbPhysicalDriveSlots;
    pl.comment                   = m_physicalLibrary3.comment.value();

    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

TEST_P(cta_catalogue_PhysicalLibraryTest, noParameterSetForModifyPhysicalLibrary) {
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, m_physicalLibrary2);

  auto shouldThrow = [this]() -> void {
    const auto libs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();

    ASSERT_EQ(1, libs.size());
    const cta::common::dataStructures::PhysicalLibrary lib = libs.front();
    cta::common::dataStructures::UpdatePhysicalLibrary pl;
    pl.name = "name";

    m_catalogue->PhysicalLibrary()->modifyPhysicalLibrary(m_admin, pl);
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

}  // namespace unitTests
