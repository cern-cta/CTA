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

#include <gtest/gtest.h>

#include <list>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/DriveStateCatalogueTest.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/SourcedParameter.hpp"

namespace unitTests {

namespace {
cta::common::dataStructures::SecurityIdentity getAdmin() {
  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";
  return admin;
}

cta::common::dataStructures::TapeDrive getTapeDriveWithMandatoryElements(const std::string &driveName) {
  cta::common::dataStructures::TapeDrive tapeDrive;
  tapeDrive.driveName = driveName;
  tapeDrive.host = "admin_host";
  tapeDrive.logicalLibrary = "VLSTK10";
  tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
  tapeDrive.desiredUp = false;
  tapeDrive.desiredForceDown = false;
  return tapeDrive;
}

cta::common::dataStructures::TapeDrive getTapeDriveWithAllElements(const std::string &driveName) {
  cta::common::dataStructures::TapeDrive tapeDrive;
  tapeDrive.driveName = driveName;
  tapeDrive.host = "admin_host";
  tapeDrive.logicalLibrary = "VLSTK10";
  tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
  tapeDrive.desiredUp = false;
  tapeDrive.desiredForceDown = false;
  tapeDrive.diskSystemName = "dummyDiskSystemName";
  tapeDrive.reservedBytes = 694498291384;
  tapeDrive.reservationSessionId = 0;

  tapeDrive.sessionStartTime = 1001;
  tapeDrive.mountStartTime = 1002;
  tapeDrive.transferStartTime = 1003;
  tapeDrive.unloadStartTime = 1004;
  tapeDrive.unmountStartTime = 1005;
  tapeDrive.drainingStartTime = 1006;
  tapeDrive.downOrUpStartTime = 1007;
  tapeDrive.probeStartTime = 1008;
  tapeDrive.cleanupStartTime = 1009;
  tapeDrive.startStartTime = 1010;
  tapeDrive.shutdownTime = 1011;

  tapeDrive.reasonUpDown = "Random Reason";

  tapeDrive.currentVid = "VIDONE";
  tapeDrive.ctaVersion = "v1.0.0";
  tapeDrive.currentPriority = 3;
  tapeDrive.currentActivity = "Activity1";
  tapeDrive.currentTapePool = "tape_pool_0";
  tapeDrive.nextMountType = cta::common::dataStructures::MountType::Retrieve;
  tapeDrive.nextVid = "VIDTWO";
  tapeDrive.nextTapePool = "tape_pool_1";
  tapeDrive.nextPriority = 1;
  tapeDrive.nextActivity = "Activity2";

  tapeDrive.devFileName = "fileName";
  tapeDrive.rawLibrarySlot = "librarySlot1";

  tapeDrive.currentVo = "VO_ONE";
  tapeDrive.nextVo = "VO_TWO";

  tapeDrive.userComment = "Random comment";
  tapeDrive.creationLog = cta::common::dataStructures::EntryLog("user_name_1", "host_1", 100002);
  tapeDrive.lastModificationLog = cta::common::dataStructures::EntryLog("user_name_2", "host_2", 10032131);

  return tapeDrive;
}
}  // namespace

cta_catalogue_DriveStateTest::cta_catalogue_DriveStateTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(getAdmin()) {
}

void cta_catalogue_DriveStateTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_DriveStateTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveNames) {
  const std::list<std::string> tapeDriveNames = {"VDSTK11", "VDSTK12", "VDSTK21", "VDSTK22"};
  for (const auto& name : tapeDriveNames) {
    const auto tapeDrive = getTapeDriveWithMandatoryElements(name);
    m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  }
  const auto storedTapeDriveNames = m_catalogue->DriveState()->getTapeDriveNames();
  ASSERT_EQ(tapeDriveNames, storedTapeDriveNames);
  for (const auto& name : tapeDriveNames) {
    m_catalogue->DriveState()->deleteTapeDrive(name);
  }
}

TEST_P(cta_catalogue_DriveStateTest, getAllTapeDrives) {
  std::list<std::string> tapeDriveNames;
  // Create 100 tape drives
  for (size_t i = 0; i < 100; i++) {
    std::stringstream ss;
    ss << "VDSTK" << std::setw(5) << std::setfill('0') << i;
    tapeDriveNames.push_back(ss.str());
  }
  std::list<cta::common::dataStructures::TapeDrive> tapeDrives;
  for (const auto& name : tapeDriveNames) {
    const auto tapeDrive = getTapeDriveWithMandatoryElements(name);
    m_catalogue->DriveState()->createTapeDrive(tapeDrive);
    tapeDrives.push_back(tapeDrive);
  }
  auto storedTapeDrives = m_catalogue->DriveState()->getTapeDrives();
  ASSERT_EQ(tapeDriveNames.size(), storedTapeDrives.size());
  while (!storedTapeDrives.empty()) {
    const auto storedTapeDrive = storedTapeDrives.front();
    const auto tapeDrive = tapeDrives.front();
    storedTapeDrives.pop_front();
    tapeDrives.pop_front();
    ASSERT_EQ(tapeDrive, storedTapeDrive);
  }
  for (const auto& name : tapeDriveNames) {
    m_catalogue->DriveState()->deleteTapeDrive(name);
  }
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDrive) {
  const std::string tapeDriveName = "VDSTK11";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithEmptyEntryLog) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.creationLog = cta::common::dataStructures::EntryLog("", "", 0);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().creationLog);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}


TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithNonExistingLogicalLibrary) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().logicalLibraryDisabled);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithDisabledLogicalLibrary) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  std::optional<std::string> physicalLibraryName;

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, tapeDrive.logicalLibrary, true, physicalLibraryName, "comment");
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(storedTapeDrive.value().logicalLibraryDisabled);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
  m_catalogue->LogicalLibrary()->deleteLogicalLibrary(tapeDrive.logicalLibrary);
}


TEST_P(cta_catalogue_DriveStateTest, failToGetTapeDrive) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(wrongName);
  ASSERT_FALSE(storedTapeDrive);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDriveName);
}

TEST_P(cta_catalogue_DriveStateTest, failToDeleteTapeDrive) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  m_catalogue->DriveState()->deleteTapeDrive(wrongName);
  auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  m_catalogue->DriveState()->deleteTapeDrive(tapeDriveName);
  storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive);
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithAllElements) {
  const std::string tapeDriveName = "VDSTK11";
  const auto tapeDrive = getTapeDriveWithAllElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, multipleTapeDrives) {
  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";
  const auto tapeDrive1 = getTapeDriveWithMandatoryElements(tapeDriveName1);
  const auto tapeDrive2 = getTapeDriveWithAllElements(tapeDriveName2);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive1);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive2);
  const auto storedTapeDrive1 = m_catalogue->DriveState()->getTapeDrive(tapeDrive1.driveName);
  const auto storedTapeDrive2 = m_catalogue->DriveState()->getTapeDrive(tapeDrive2.driveName);
  ASSERT_EQ(tapeDrive1, storedTapeDrive1);
  ASSERT_EQ(tapeDrive2, storedTapeDrive2);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive1.driveName);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive2.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setDesiredStateEmpty) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.reasonUpDown = "Previous reason";
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  {
    cta::common::dataStructures::DesiredDriveState desiredState;
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->setDesiredDriveState(tapeDriveName, desiredState, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reasonUpDown));
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value(), tapeDrive.reasonUpDown.value());
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setDesiredStateWithEmptyReason) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  {
    cta::common::dataStructures::DesiredDriveState desiredState;
    desiredState.reason = "";
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->setDesiredDriveState(tapeDriveName, desiredState, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  // SqlLite (InMemory) returns an empty string and Oracle returns a std::nullopt
  if (storedTapeDrive.value().reasonUpDown) {
    ASSERT_TRUE(storedTapeDrive.value().reasonUpDown.value().empty());
  } else {
    ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().reasonUpDown));
  }
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setDesiredState) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  cta::common::dataStructures::DesiredDriveState desiredState;
  desiredState.up = false;
  desiredState.forceDown = true;
  desiredState.reason = "reason";
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->setDesiredDriveState(tapeDriveName, desiredState, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().desiredUp , desiredState.up);
  ASSERT_EQ(storedTapeDrive.value().desiredForceDown , desiredState.forceDown);
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value() , desiredState.reason);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setDesiredStateComment) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  // It should keep this Desired Status
  tapeDrive.desiredUp = true;
  tapeDrive.desiredForceDown = false;
  tapeDrive.reasonUpDown = "reason";
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  cta::common::dataStructures::DesiredDriveState desiredState;
  // It should update only the comment
  const std::string comment = "New Comment";
  desiredState.up = false;
  desiredState.forceDown = true;
  desiredState.reason = "reason2";
  desiredState.comment = comment;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->setDesiredDriveState(tapeDriveName, desiredState, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().desiredUp , tapeDrive.desiredUp);
  ASSERT_EQ(storedTapeDrive.value().desiredForceDown , tapeDrive.desiredForceDown);
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value() , tapeDrive.reasonUpDown);
  ASSERT_EQ(storedTapeDrive.value().userComment.value() , comment);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setDesiredStateEmptyComment) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  // It should keep this Desired Status
  tapeDrive.desiredUp = true;
  tapeDrive.desiredForceDown = false;
  tapeDrive.reasonUpDown = "reason";
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  cta::common::dataStructures::DesiredDriveState desiredState;
  // It should update only the comment
  const std::string comment = "";
  desiredState.up = false;
  desiredState.forceDown = true;
  desiredState.reason = "reason2";
  desiredState.comment = comment;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->setDesiredDriveState(tapeDriveName, desiredState, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().desiredUp , tapeDrive.desiredUp);
  ASSERT_EQ(storedTapeDrive.value().desiredForceDown , tapeDrive.desiredForceDown);
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value() , tapeDrive.reasonUpDown);
  // SqlLite (InMemory) returns an empty string and Oracle returns a std::nullopt
  if (storedTapeDrive.value().userComment) {
    ASSERT_TRUE(storedTapeDrive.value().userComment.value().empty());
  } else {
    ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().userComment));
  }
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setTapeDriveStatistics) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Transferring;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = 123456789;
  inputs.filesTransferred = 987654321;
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatistics(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().bytesTransferedInSession.value(), inputs.bytesTransferred);
  ASSERT_EQ(storedTapeDrive.value().filesTransferedInSession.value(), inputs.filesTransferred);
  const auto lastModificationLog = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host,
    inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value() , lastModificationLog);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, setTapeDriveStatisticsInNoTransferingStatus) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  cta::ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = 123456789;
  inputs.filesTransferred = 987654321;
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatistics(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().bytesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().filesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().lastModificationLog);
  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusSameAsPrevious) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  // We update keeping the same status, so it has to update only the lastModificationLog
  cta::ReportDriveStatusInputs inputs;
  inputs.status = tapeDrive.driveStatus;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 123456;
  inputs.filesTransferred = 987654;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(driveInfo.driveName, storedTapeDrive.value().driveName);
  ASSERT_EQ(inputs.status, storedTapeDrive.value().driveStatus);
  ASSERT_NE(inputs.mountType, storedTapeDrive.value().mountType);  // Not update this value
  ASSERT_EQ(driveInfo.host, storedTapeDrive.value().host);
  ASSERT_EQ(driveInfo.logicalLibrary, storedTapeDrive.value().logicalLibrary);
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(log, storedTapeDrive.value().lastModificationLog.value());
  ASSERT_FALSE(storedTapeDrive.value().bytesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().filesTransferedInSession);
  // Disk reservations are not updated by updateTapeDriveStatus()
  ASSERT_FALSE(storedTapeDrive.value().diskSystemName);
  ASSERT_FALSE(storedTapeDrive.value().reservedBytes);
  ASSERT_FALSE(storedTapeDrive.value().reservationSessionId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusSameTransferingAsPrevious) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Transferring;
  tapeDrive.sessionStartTime = time(nullptr);
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto test = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive.sessionStartTime, test.value().sessionStartTime.value());
  // We update keeping the same status, so it has to update only the lastModificationLog
  const uint64_t elapsedTime = 1000;
  cta::ReportDriveStatusInputs inputs;
  inputs.status = tapeDrive.driveStatus;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = tapeDrive.sessionStartTime.value() + elapsedTime;
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 123456;
  inputs.filesTransferred = 987654;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(driveInfo.driveName, storedTapeDrive.value().driveName);
  ASSERT_EQ(inputs.status, storedTapeDrive.value().driveStatus);
  ASSERT_NE(inputs.mountType, storedTapeDrive.value().mountType);  // Not update this value
  ASSERT_EQ(driveInfo.host, storedTapeDrive.value().host);
  ASSERT_EQ(driveInfo.logicalLibrary, storedTapeDrive.value().logicalLibrary);
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(log, storedTapeDrive.value().lastModificationLog.value());
  ASSERT_EQ(inputs.byteTransferred, storedTapeDrive.value().bytesTransferedInSession.value());
  ASSERT_EQ(inputs.filesTransferred, storedTapeDrive.value().filesTransferedInSession.value());
  // It will keep names and bytes, because it isn't in state UP
  ASSERT_FALSE(storedTapeDrive.value().reservedBytes);
  ASSERT_FALSE(storedTapeDrive.value().reservationSessionId);
  ASSERT_FALSE(storedTapeDrive.value().diskSystemName);
  // Check elapsed time
  ASSERT_EQ(storedTapeDrive.value().sessionElapsedTime.value(), inputs.reportTime - tapeDrive.sessionStartTime.value());

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusDown) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Down;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.reason = "testing";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionId));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_EQ(storedTapeDrive.value().downOrUpStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, cta::common::dataStructures::MountType::NoMount);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, cta::common::dataStructures::DriveStatus::Down);
  ASSERT_EQ(storedTapeDrive.value().desiredUp, false);
  ASSERT_EQ(storedTapeDrive.value().desiredForceDown, false);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVid));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentTapePool));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVo));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value(), inputs.reason);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUp) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.desiredUp = true;
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Up;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionId));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_EQ(storedTapeDrive.value().downOrUpStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, cta::common::dataStructures::MountType::NoMount);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, cta::common::dataStructures::DriveStatus::Up);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVid));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentTapePool));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVo));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().reasonUpDown));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUpButDesiredIsDown) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::DrainingToDisk;  // To force a change of state
  tapeDrive.desiredUp = false;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Up;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.reason = "testing";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().driveStatus, cta::common::dataStructures::DriveStatus::Down);
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value(), inputs.reason);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUpCleanSpaceReservation) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  tapeDrive.diskSystemName = "DISK_SYSTEM_NAME";
  tapeDrive.reservedBytes = 123456789;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Up;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.reason = "testing";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().diskSystemName);
  ASSERT_FALSE(storedTapeDrive.value().reservedBytes);
  ASSERT_FALSE(storedTapeDrive.value().reservationSessionId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUpDontCleanSpaceReservation) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  tapeDrive.diskSystemName = std::nullopt;
  tapeDrive.reservedBytes = std::nullopt;
  tapeDrive.reservationSessionId = std::nullopt;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Up;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.reason = "testing";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().diskSystemName);
  ASSERT_FALSE(storedTapeDrive.value().reservedBytes);
  ASSERT_FALSE(storedTapeDrive.value().reservationSessionId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusProbing) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Probing;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::NoMount;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 0;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionId));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_EQ(storedTapeDrive.value().probeStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVid));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentTapePool));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentVo));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusStarting) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Starting;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_EQ(storedTapeDrive.value().sessionStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_EQ(storedTapeDrive.value().currentActivity.value(), inputs.activity);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusMounting) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Mounting;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_EQ(storedTapeDrive.value().mountStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusTransfering) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Transferring;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_EQ(storedTapeDrive.value().bytesTransferedInSession.value(), inputs.byteTransferred);
  ASSERT_EQ(storedTapeDrive.value().filesTransferedInSession.value(), inputs.filesTransferred);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_EQ(storedTapeDrive.value().sessionElapsedTime.value(), 0);  // Because it's starting
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_EQ(storedTapeDrive.value().transferStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUnloading) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Unloading;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_EQ(storedTapeDrive.value().unloadStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusUnmounting) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Unmounting;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_EQ(storedTapeDrive.value().unmountStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusDrainingToDisk) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::DrainingToDisk;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_EQ(storedTapeDrive.value().drainingStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusCleaningUp) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::CleaningUp;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(storedTapeDrive.value().sessionId.value(), inputs.mountSessionId);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_EQ(storedTapeDrive.value().cleanupStartTime.value(), inputs.reportTime);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().shutdownTime));
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, updateTapeDriveStatusShutdown) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::ReportDriveStatusInputs inputs;
  inputs.status = cta::common::dataStructures::DriveStatus::Shutdown;
  // We use a different MountType to check it doesn't update this value in the database
  inputs.mountType = cta::common::dataStructures::MountType::ArchiveForUser;
  inputs.reportTime = time(nullptr);
  inputs.mountSessionId = 123456;
  inputs.byteTransferred = 987654;
  inputs.filesTransferred = 456;
  inputs.vid = "vid";
  inputs.tapepool = "tapepool";
  inputs.vo = "vo";
  inputs.activity = "activity";
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = tapeDrive.driveName;
  driveInfo.host = tapeDrive.host;
  driveInfo.logicalLibrary = tapeDrive.logicalLibrary;
  {
    cta::log::LogContext dummyLc(m_dummyLog);
    auto tapeDrivesState = std::make_unique<cta::TapeDrivesCatalogueState>(*m_catalogue);
    tapeDrivesState->updateDriveStatus(driveInfo, inputs, dummyLc);
  }
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionId));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().bytesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().filesTransferedInSession));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().sessionElapsedTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().mountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().transferStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unloadStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().unmountStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().drainingStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().downOrUpStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().probeStartTime));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().cleanupStartTime));
  ASSERT_EQ(storedTapeDrive.value().shutdownTime.value(), inputs.reportTime);
  const auto log = cta::common::dataStructures::EntryLog("NO_USER", driveInfo.host, inputs.reportTime);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value(), log);
  ASSERT_EQ(storedTapeDrive.value().mountType, inputs.mountType);
  ASSERT_EQ(storedTapeDrive.value().driveStatus, inputs.status);
  ASSERT_EQ(storedTapeDrive.value().currentVid.value(), inputs.vid);
  ASSERT_EQ(storedTapeDrive.value().currentTapePool.value(), inputs.tapepool);
  ASSERT_EQ(storedTapeDrive.value().currentVo.value(), inputs.vo);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().currentActivity));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}


TEST_P(cta_catalogue_DriveStateTest, addDiskSpaceReservationWhenItsNull) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = std::nullopt;
  tapeDrive.reservedBytes = std::nullopt;
  tapeDrive.reservationSessionId = std::nullopt;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::DiskSpaceReservationRequest request;
  const std::string spaceName = "space1";
  const uint64_t reservedBytes = 987654;
  request.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = 123;

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, request, dummyLc);

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, incrementAnExistingDiskSpaceReservation) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = "existing_space";
  tapeDrive.reservedBytes = 1234;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::DiskSpaceReservationRequest request;
  const std::string spaceName = tapeDrive.diskSystemName.value();
  const uint64_t reservedBytes = 852;
  request.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = tapeDrive.reservationSessionId.value();

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, request, dummyLc);

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes + tapeDrive.reservedBytes.value());
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, decrementANonExistingDiskSpaceReservation) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = std::nullopt;
  tapeDrive.reservedBytes = std::nullopt;
  tapeDrive.reservationSessionId = std::nullopt;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::DiskSpaceReservationRequest request;
  const std::string spaceName = "space1";
  const uint64_t reservedBytes = 852;
  request.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = 123;

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, mountId, request, dummyLc);

  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, decrementAExistingDiskSpaceReservation) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = "existing_space";
  tapeDrive.reservedBytes = 1234;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::DiskSpaceReservationRequest request1;
  const std::string spaceName = tapeDrive.diskSystemName.value();
  const uint64_t reservedBytes = 852;
  request1.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = tapeDrive.reservationSessionId.value();

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, mountId, request1, dummyLc);

  const auto storedTapeDrive1 = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive1.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive1.value().reservedBytes.value(), tapeDrive.reservedBytes.value() - reservedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive1.value().reservationSessionId.value(), mountId);

  cta::DiskSpaceReservationRequest request2;
  request2.addRequest(tapeDrive.diskSystemName.value(), tapeDrive.reservedBytes.value() - reservedBytes);

  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, mountId, request2, dummyLc);

  const auto storedTapeDrive2 = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive2.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive2.value().reservedBytes.value(), 0);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive2.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, incrementAnExistingDiskSpaceReservationAndThenLargerDecrement) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = "existing_space";
  tapeDrive.reservedBytes = 10;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  cta::DiskSpaceReservationRequest increaseRequest;
  const std::string spaceName = tapeDrive.diskSystemName.value();
  const uint64_t reservedBytes = 20;
  increaseRequest.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = tapeDrive.reservationSessionId.value();

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, increaseRequest, dummyLc);

  const auto storedTapeDrive1 = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive1.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive1.value().reservedBytes.value(), reservedBytes + tapeDrive.reservedBytes.value());
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive1.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive1.value().reservationSessionId.value(), mountId);

  cta::DiskSpaceReservationRequest decreaseRequest;
  decreaseRequest.addRequest(tapeDrive.diskSystemName.value(), 100000);  // Decrease a bigger number of reserved bytes

  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, mountId, decreaseRequest, dummyLc);

  const auto storedTapeDrive2 = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive2.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive2.value().reservedBytes.value(), 0);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive2.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive2.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, failToIncrementAnOldDiskSystem) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = "old_space";
  tapeDrive.reservedBytes = 1234;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  // New Disk Space
  cta::DiskSpaceReservationRequest newRequest;
  const std::string spaceName = "new_space";
  const uint64_t reservedBytes = 345;
  newRequest.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = 3;

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, newRequest, dummyLc);

  // Decrease Old Space
  cta::DiskSpaceReservationRequest oldRequest;
  oldRequest.addRequest(tapeDrive.diskSystemName.value(), tapeDrive.reservedBytes.value());

  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, tapeDrive.reservationSessionId.value(), oldRequest,
    dummyLc);

  // Check it keeps the new disk space system values
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), spaceName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, sameSystemNameButDifferentMountID) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string diskSystemName = "space_name";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = diskSystemName;
  tapeDrive.reservedBytes = 1234;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  // New Disk Space
  cta::DiskSpaceReservationRequest request;
  const std::string spaceName = diskSystemName;
  const uint64_t reservedBytes = 345;
  request.addRequest(spaceName, reservedBytes);
  const uint64_t mountId = 3;

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, request, dummyLc);

  // Check it keeps the new disk space system values
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), diskSystemName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, failToDecrementAnOldMountIDAndDecrementNewAgain) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string diskSystemName = "space_name";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.diskSystemName = diskSystemName;
  tapeDrive.reservedBytes = 1234;
  tapeDrive.reservationSessionId = 9;
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);

  // New Disk Space
  cta::DiskSpaceReservationRequest newRequest;
  const uint64_t reservedBytes = 345;
  newRequest.addRequest(diskSystemName, reservedBytes);
  const uint64_t mountId = 3;

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DriveState()->reserveDiskSpace(tapeDriveName, mountId, newRequest, dummyLc);

  // Decrease Old Space
  cta::DiskSpaceReservationRequest oldRequest;
  oldRequest.addRequest(diskSystemName, tapeDrive.reservedBytes.value());

  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, tapeDrive.reservationSessionId.value(), oldRequest, dummyLc);

  // Check it keeps the new disk space system values
  auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), diskSystemName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  // Decrease New Space
  cta::DiskSpaceReservationRequest decreaseRequest;
  const uint64_t decreasedBytes = 10;
  decreaseRequest.addRequest(diskSystemName, decreasedBytes);
  m_catalogue->DriveState()->releaseDiskSpace(tapeDriveName, mountId, decreaseRequest, dummyLc);

  // Check it keeps the new disk space system values
  storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().diskSystemName));
  ASSERT_EQ(storedTapeDrive.value().diskSystemName.value(), diskSystemName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservedBytes));
  ASSERT_EQ(storedTapeDrive.value().reservedBytes.value(), reservedBytes - decreasedBytes);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reservationSessionId));
  ASSERT_EQ(storedTapeDrive.value().reservationSessionId.value(), mountId);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithPhysicalLibrary) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  const auto physicalLibrary1 = CatalogueTestUtils::getPhysicalLibrary1();
  m_catalogue->PhysicalLibrary()->createPhysicalLibrary(m_admin, physicalLibrary1);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, tapeDrive.logicalLibrary, true, physicalLibrary1.name, "comment");
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().physicalLibraryName, physicalLibrary1.name);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
  m_catalogue->LogicalLibrary()->deleteLogicalLibrary(tapeDrive.logicalLibrary);
}

TEST_P(cta_catalogue_DriveStateTest, getTapeDriveWithoutPhysicalLibrary) {
  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  std::optional<std::string> physicalLibraryName;
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, tapeDrive.logicalLibrary, true, physicalLibraryName, "comment");
  m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->DriveState()->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().physicalLibraryName, std::nullopt);

  m_catalogue->DriveState()->deleteTapeDrive(tapeDrive.driveName);
  m_catalogue->LogicalLibrary()->deleteLogicalLibrary(tapeDrive.logicalLibrary);
}

}  // namespace unitTests
