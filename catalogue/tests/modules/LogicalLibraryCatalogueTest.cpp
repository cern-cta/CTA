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
#include "catalogue/tests/modules/LogicalLibraryCatalogueTest.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"
#include "rdbms/UniqueConstraintError.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_LogicalLibraryTest::cta_catalogue_LogicalLibraryTest()
  : m_dummyLog("dummy", "dummy", "configFilename"),
    m_admin("admin", "admin", "admin", ""),
    m_vo(CatalogueTestUtils::getVo()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_tape1(CatalogueTestUtils::getTape1()) {
}

void cta_catalogue_LogicalLibraryTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_LogicalLibraryTest::TearDown() {
  m_catalogue.reset();
}

std::map<std::string, cta::common::dataStructures::LogicalLibrary>
  cta_catalogue_LogicalLibraryTest::logicalLibraryListToMap(
  const std::list<cta::common::dataStructures::LogicalLibrary> &listOfLibs) const {
  try {
    std::map<std::string, cta::common::dataStructures::LogicalLibrary> nameToLib;

    for (auto &lib: listOfLibs) {
      if(nameToLib.end() != nameToLib.find(lib.name)) {
        throw cta::exception::Exception(std::string("Duplicate logical library: value=") + lib.name);
      }
      nameToLib[lib.name] = lib;
    }

    return nameToLib;
  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, createLogicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;

  const auto physicalLibrary = CatalogueTestUtils::getPhysicalLibrary1();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary);
  const auto physLibs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
  ASSERT_EQ(1, physLibs.size());

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibrary.name,
    comment);

  const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const cta::common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_FALSE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_LogicalLibraryTest, createLogicalLibrary_disabled_true) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(true);
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);

  const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const cta::common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_TRUE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_LogicalLibraryTest, createLogicalLibrary_disabled_false) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(false);
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);

  const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const cta::common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_FALSE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_LogicalLibraryTest, createLogicalLibrary_same_twice) {
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);
  ASSERT_THROW(m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName,
    logicalLibraryIsDisabled, physicalLibraryName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_LogicalLibraryTest, removePhysicalLibraryAssociatedWithLogicalLibrary) {
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  const auto physicalLibrary = CatalogueTestUtils::getPhysicalLibrary1();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibrary.name,
                                                      comment);

  auto shouldThrow = [this, physicalLibrary]() -> void {
    m_catalogue->PhysicalLibrary()->deletePhysicalLibrary(physicalLibrary.name);
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

TEST_P(cta_catalogue_LogicalLibraryTest, setLogicalLibraryDisabled_true) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);

  {
    const std::list<cta::common::dataStructures::LogicalLibrary> libs =
      m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_FALSE(lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog =
      lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedLogicalLibraryIsDisabled= true;
  m_catalogue->LogicalLibrary()->setLogicalLibraryDisabled(m_admin, logicalLibraryName,
    modifiedLogicalLibraryIsDisabled);

  {
    const std::list<cta::common::dataStructures::LogicalLibrary> libs =
      m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedLogicalLibraryIsDisabled, lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, setLogicalLibraryDisabled_false) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);

  {
    const std::list<cta::common::dataStructures::LogicalLibrary> libs =
      m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_FALSE(lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog =
      lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedLogicalLibraryIsDisabled= false;
  m_catalogue->LogicalLibrary()->setLogicalLibraryDisabled(m_admin, logicalLibraryName,
    modifiedLogicalLibraryIsDisabled);

  {
    const std::list<cta::common::dataStructures::LogicalLibrary> libs =
      m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedLogicalLibraryIsDisabled, lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, deleteLogicalLibrary) {
  const bool libNotToDeleteIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string libNotToDeleteComment = "Create logical library to NOT be deleted";
  std::optional<std::string> physicalLibraryName;

  // Create a tape and a logical library that are not the ones to be deleted
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, libNotToDeleteIsDisabled, physicalLibraryName,
    libNotToDeleteComment);
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libs.size());
    const auto lib = libs.front();
    ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
    ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
    ASSERT_EQ(libNotToDeleteComment, lib.comment);
    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  ASSERT_TRUE(m_catalogue->Tape()->tapeExists(m_tape1.vid));
  {
    const auto tapes = m_catalogue->Tape()->getTapes();
    ASSERT_EQ(1, tapes.size());

    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  // Create the logical library to be deleted
  const std::string libToDeleteName = "lib_to_delete";
  const bool libToDeleteIsDisabled = false;
  const std::string libToDeleteComment = "Create logical library to be deleted";
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, libToDeleteName, libToDeleteIsDisabled, physicalLibraryName,
    libToDeleteComment);
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(2, libs.size());
    const auto nameToLib = logicalLibraryListToMap(libs);
    ASSERT_EQ(2, nameToLib.size());

    {
      const auto nameToLibItor = nameToLib.find(m_tape1.logicalLibraryName);
      ASSERT_NE(nameToLib.end(), nameToLibItor);
      const auto &lib = nameToLibItor->second;
      ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
      ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
      ASSERT_EQ(libNotToDeleteComment, lib.comment);
      const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);
      const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }

    {
      const auto nameToLibItor = nameToLib.find(libToDeleteName);
      ASSERT_NE(nameToLib.end(), nameToLibItor);
      const auto &lib = nameToLibItor->second;
      ASSERT_EQ(libToDeleteName, lib.name);
      ASSERT_EQ(libToDeleteIsDisabled, lib.isDisabled);
      ASSERT_EQ(libToDeleteComment, lib.comment);
      const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);
      const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->LogicalLibrary()->deleteLogicalLibrary(libToDeleteName);
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libs.size());
    const auto lib = libs.front();
    ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
    ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
    ASSERT_EQ(libNotToDeleteComment, lib.comment);
    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, deleteLogicalLibrary_non_existent) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());
  ASSERT_THROW(m_catalogue->LogicalLibrary()->deleteLogicalLibrary("non_existent_logical_library"),
    cta::catalogue::UserSpecifiedANonExistentLogicalLibrary);
}

TEST_P(cta_catalogue_LogicalLibraryTest, deleteLogicalLibrary_non_empty) {
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(1, tapes.size());

  const cta::common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(m_tape1.state,tape.state);
  ASSERT_EQ(m_tape1.full, tape.full);

  ASSERT_FALSE(tape.isFromCastor);
  ASSERT_EQ(m_tape1.comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->LogicalLibrary()->deleteLogicalLibrary(m_tape1.logicalLibraryName),
    cta::catalogue::UserSpecifiedANonEmptyLogicalLibrary);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryName) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool libraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, physicalLibraryName, comment);

  {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto &library = libraries.front();
    ASSERT_EQ(libraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const cta::common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = library.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newLibraryName = "new_logical_library";
  m_catalogue->LogicalLibrary()->modifyLogicalLibraryName(m_admin, libraryName, newLibraryName);

  {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto &library = libraries.front();
    ASSERT_EQ(newLibraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const cta::common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryName_emptyStringCurrentLogicalLibraryName) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const bool libraryIsDisabled = false;
  const std::string comment = "Create logical library";
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, physicalLibraryName, comment);

  const std::string newLibraryName = "new_logical_library";
  ASSERT_THROW(m_catalogue->LogicalLibrary()->modifyLogicalLibraryName(m_admin, "", newLibraryName),
    cta::catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryName_emptyStringNewLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const bool libraryIsDisabled = false;
  const std::string comment = "Create logical library";
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, physicalLibraryName, comment);

  {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto library = libraries.front();
    ASSERT_EQ(libraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const cta::common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = library.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newLibraryName = "";
  ASSERT_THROW(m_catalogue->LogicalLibrary()->modifyLogicalLibraryName(m_admin, libraryName, newLibraryName),
    cta::catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryComment) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->LogicalLibrary()->modifyLogicalLibraryComment(m_admin, logicalLibraryName, modifiedComment);

  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedComment, lib.comment);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryPhysicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;

  const auto physicalLibrary1 = CatalogueTestUtils::getPhysicalLibrary1();
  const auto physicalLibrary2 = CatalogueTestUtils::getPhysicalLibrary2();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary1);
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary2);
  const auto physLibs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
  ASSERT_EQ(2, physLibs.size());

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibrary1.name,
    comment);

  m_catalogue->LogicalLibrary()->modifyLogicalLibraryPhysicalLibrary(m_admin, logicalLibraryName, physicalLibrary2.name);

  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(physicalLibrary2.name, lib.physicalLibraryName);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryEmptyPhysicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;

  const auto physicalLibrary1 = CatalogueTestUtils::getPhysicalLibrary1();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary1);
  const auto physLibs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
  ASSERT_EQ(1, physLibs.size());

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibrary1.name,
    comment);

  m_catalogue->LogicalLibrary()->modifyLogicalLibraryPhysicalLibrary(m_admin, logicalLibraryName, "");

  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(std::nullopt, lib.physicalLibraryName);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryNonExistentPhysicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;

  auto shouldThrow = [this, logicalLibraryName, comment, logicalLibraryIsDisabled]() -> void {
    m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "doesNotExist", comment);
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryModifyNonExistentPhysicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;

  const auto physicalLibrary1 = CatalogueTestUtils::getPhysicalLibrary1();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary1);
  const auto physLibs = m_catalogue->PhysicalLibrary()->getPhysicalLibraries();
  ASSERT_EQ(1, physLibs.size());

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibrary1.name,
    comment);

  auto shouldThrow = [this]() -> void {
    m_catalogue->LogicalLibrary()->modifyLogicalLibraryPhysicalLibrary(m_admin, m_tape1.logicalLibraryName, "doeNotExist");
  };

  ASSERT_THROW(shouldThrow(), cta::exception::UserError);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryComment_nonExisentLogicalLibrary) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  ASSERT_THROW(m_catalogue->LogicalLibrary()->modifyLogicalLibraryComment(m_admin, logicalLibraryName, comment),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryDisabledReason) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    comment);
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);
    ASSERT_FALSE(lib.disabledReason);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedDisabledReason = "Modified disabled reason";
  m_catalogue->LogicalLibrary()->modifyLogicalLibraryDisabledReason(m_admin, logicalLibraryName,
    modifiedDisabledReason);

  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);
    ASSERT_EQ(modifiedDisabledReason, lib.disabledReason.value());

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }

  //setting empty reason should delete it from the DB
  m_catalogue->LogicalLibrary()->modifyLogicalLibraryDisabledReason(m_admin, logicalLibraryName, "");
  {
    const auto libs = m_catalogue->LogicalLibrary()->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const cta::common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);
    ASSERT_FALSE(lib.disabledReason);

    const cta::common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_LogicalLibraryTest, modifyLogicalLibraryDisabledReason_nonExisentLogicalLibrary) {
  ASSERT_TRUE(m_catalogue->LogicalLibrary()->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string disabledReason = "Create logical library";
  ASSERT_THROW(m_catalogue->LogicalLibrary()->modifyLogicalLibraryDisabledReason(m_admin, logicalLibraryName,
    disabledReason), cta::exception::UserError);
}


TEST_P(cta_catalogue_LogicalLibraryTest, tapeExists_emptyString) {
  const std::string vid = "";
  ASSERT_THROW(m_catalogue->Tape()->tapeExists(vid), cta::exception::Exception);
}

TEST_P(cta_catalogue_LogicalLibraryTest, createTape) {
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  ASSERT_TRUE(m_catalogue->Tape()->tapeExists(m_tape1.vid));

  const auto tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

}  // namespace unitTests
