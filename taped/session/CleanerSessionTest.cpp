/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CleanerSession.hpp"

#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "taped/drive/FakeDrive.hpp"
#include "taped/file/LabelSession.hpp"
#include "taped/system/Wrapper.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

namespace unitTests {

namespace {

struct CleanerSessionTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;
};

class CleanerSessionTest : public ::testing::TestWithParam<CleanerSessionTestParam> {
protected:
  void SetUp() override {
    constexpr uint64_t nbConns = 1;
    constexpr uint64_t nbArchiveFileListingConns = 1;
    m_catalogue = std::make_unique<cta::catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
    m_db = GetParam().dbFactory.create(m_catalogue);
    m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_db, "schedulerBackendName");

    setupCatalogue();

    m_systemWrapper.delegateToFake();
    m_systemWrapper.disableGMockCallsCounting();
    m_systemWrapper.fake.setupForVirtualDriveSLC6();
  }

  void setupCatalogue() {
    cta::common::dataStructures::DiskInstance diskInstance;
    diskInstance.name = m_diskInstance;
    diskInstance.comment = "Comment";
    m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance.name, diskInstance.comment);

    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = m_vo;
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.comment = "Comment";
    vo.diskInstanceName = diskInstance.name;
    vo.isRepackVo = false;
    m_catalogue->VO()->createVirtualOrganization(m_admin, vo);

    m_catalogue->TapePool()->createTapePool(m_admin, m_tapePool, vo.name, 1, std::nullopt, {}, "Comment");

    cta::catalogue::MediaType mediaType;
    mediaType.name = m_mediaType;
    mediaType.capacityInBytes = 12345678;
    mediaType.cartridge = "cartridge";
    mediaType.minLPos = 2696;
    mediaType.maxLPos = 171097;
    mediaType.nbWraps = 112;
    mediaType.comment = "Comment";
    m_catalogue->MediaType()->createMediaType(m_admin, mediaType);

    m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_library, false, std::nullopt, "Comment");

    cta::catalogue::CreateTapeAttributes tape;
    tape.vid = m_vid;
    tape.mediaType = mediaType.name;
    tape.vendor = "TestVendor";
    tape.logicalLibraryName = m_library;
    tape.tapePoolName = m_tapePool;
    tape.full = false;
    tape.state = cta::common::dataStructures::Tape::ACTIVE;
    tape.comment = "Comment";
    m_catalogue->Tape()->createTape(m_admin, tape);

    cta::common::dataStructures::TapeDrive tapeDrive;
    tapeDrive.driveName = m_driveInfo.driveName;
    tapeDrive.host = m_driveInfo.host;
    tapeDrive.logicalLibrary = m_driveInfo.logicalLibrary;
    tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
    tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
    tapeDrive.desiredUp = true;
    tapeDrive.desiredForceDown = false;
    m_catalogue->DriveState()->createTapeDrive(tapeDrive);
  }

  cta::tape::drive::FakeDrive* installDrive(bool tapeInPlace = true) {
    auto* drive = new cta::tape::drive::FakeDrive(5000, cta::tape::drive::FakeDrive::OnFlush);
    drive->setTapeInPlace(tapeInPlace);
    m_systemWrapper.fake.m_pathToDrive["/dev/nst0"] = drive;
    return drive;
  }

  void assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint failurePoint) {
    cta::mediachanger::RmcProxy rmcProxy;
    cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

    auto* drive = installDrive();
    drive->setFailurePoint(failurePoint);
    cta::tape::daemon::CleanerSession
      cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_systemWrapper, m_vid, false, 0, *m_catalogue, *m_scheduler);

    ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, cleaner.execute());
    ASSERT_NE(std::string::npos,
              m_sessionLog.getLog().find("Cleaner failed to prepare the drive or read the volume label"));
    ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
    ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  }

  cta::log::DummyLogger m_dummyLog {"dummy", "dummy"};
  cta::log::StringLogger m_sessionLog {"dummy", "tapedUnitTest", cta::log::DEBUG};
  cta::log::StringLogger m_changerLog {"dummy", "mediaChangerUnitTest", cta::log::DEBUG};
  cta::tape::System::mockWrapper m_systemWrapper;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::Scheduler> m_scheduler;

  const cta::common::dataStructures::SecurityIdentity m_admin {"admin1", "host1"};
  const std::string m_diskInstance = "disk_instance";
  const std::string m_vo = "vo";
  const std::string m_tapePool = "TestTapePool";
  const std::string m_mediaType = "LTO7M";
  const std::string m_library = "TestLogicalLibrary";
  const std::string m_vid = "TSTVID";
  const cta::common::dataStructures::DriveInfo m_driveInfo {"T10D6116",
                                                            "host",
                                                            m_library,
                                                            "/dev/tape_T10D6116",
                                                            "dummy"};
};

TEST_P(CleanerSessionTest, EjectsBlankTape) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive();
  cta::tape::daemon::CleanerSession
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_systemWrapper, m_vid, false, 0, *m_catalogue, *m_scheduler);

  ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, cleaner.execute());
  ASSERT_NE(std::string::npos,
            m_sessionLog.getLog().find("Cleaner failed to prepare the drive or read the volume label"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));

  const auto firstDismount = m_changerLog.getLog().find("Dummy dismount");
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount", firstDismount + 1));

  cta::log::LogContext logContext(m_sessionLog);
  const auto driveState = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, logContext);
  ASSERT_TRUE(driveState.up);
}

TEST_P(CleanerSessionTest, EjectsLabeledTape) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  auto* drive = installDrive();
  cta::tape::tapeFile::LabelSession::label(drive, m_vid, false);
  cta::tape::daemon::CleanerSession
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_systemWrapper, m_vid, false, 0, *m_catalogue, *m_scheduler);

  ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, cleaner.execute());
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner read the VSN from the volume label"));
  ASSERT_EQ(std::string::npos,
            m_sessionLog.getLog().find("Cleaner failed to prepare the drive or read the volume label"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(CleanerSessionTest, EjectsTapeAfterEncryptionClearFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::ClearEncryptionKey);
}

TEST_P(CleanerSessionTest, EjectsTapeAfterRewindFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::Rewind);
}

TEST_P(CleanerSessionTest, EjectsTapeAfterLbpDisableFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
}

TEST_P(CleanerSessionTest, DismountsTapeAfterUnloadFailure) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  auto* drive = installDrive();
  drive->setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::UnloadTape);
  cta::tape::daemon::CleanerSession
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_systemWrapper, m_vid, false, 0, *m_catalogue, *m_scheduler);

  ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, cleaner.execute());
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unload command failed"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(CleanerSessionTest, AcceptsEmptyDriveWithoutDismounting) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive(false);
  cta::tape::daemon::CleanerSession
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_systemWrapper, m_vid, false, 0, *m_catalogue, *m_scheduler);

  ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, cleaner.execute());
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(CleanerSessionTest, EjectsTapeWithUnknownVid) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive();
  cta::tape::daemon::CleanerSession unknownVidCleaner(mediaChanger,
                                                      m_sessionLog,
                                                      m_driveInfo,
                                                      m_systemWrapper,
                                                      "",
                                                      false,
                                                      0,
                                                      *m_catalogue,
                                                      *m_scheduler);
  ASSERT_EQ(cta::tape::daemon::Session::MARK_DRIVE_AS_UP, unknownVidCleaner.execute());
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

#ifdef CTA_PGSCHED
cta::RelationalDBTestFactory relationalDbTestFactory;
INSTANTIATE_TEST_SUITE_P(RelationalDB,
                         CleanerSessionTest,
                         ::testing::Values(CleanerSessionTestParam {relationalDbTestFactory}));
#else
cta::OStoreDBFactory<cta::objectstore::BackendVFS> objectStoreDbFactory;
INSTANTIATE_TEST_SUITE_P(ObjectStore,
                         CleanerSessionTest,
                         ::testing::Values(CleanerSessionTestParam {objectStoreDbFactory}));
#endif

}  // namespace

}  // namespace unitTests
