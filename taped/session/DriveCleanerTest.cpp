/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveCleaner.hpp"

#include "TapeSessionTracker.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyDriveStateCatalogue.hpp"
#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "mediachanger/CommonMarshal.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/RmcMarshal.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "taped/drive/FakeDrive.hpp"
#include "taped/file/LabelSession.hpp"
#include "taped/system/Wrapper.hpp"

#include <exception>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

namespace unitTests {

namespace {

// Exercise cartridge-name fallback with real RMC requests and a bounded local robot simulation.
class ScriptedRobot {
public:
  explicit ScriptedRobot(std::vector<uint32_t> replies) : m_listener(socket(AF_INET, SOCK_STREAM, 0)) {
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (m_listener.get() < 0 || bind(m_listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0
        || listen(m_listener.get(), 2) < 0) {
      throw std::runtime_error("Cannot create test robot listener");
    }
    socklen_t size = sizeof(address);
    if (getsockname(m_listener.get(), reinterpret_cast<sockaddr*>(&address), &size) < 0) {
      throw std::runtime_error("Cannot get test robot port");
    }
    m_port = ntohs(address.sin_port);
    m_requests = std::async(std::launch::async, [this, replies] {
      using namespace cta::mediachanger;
      std::vector<std::string> vids;
      for (const auto status : replies) {
        cta::SmartFd connection(acceptConnection(m_listener.get(), 2));
        char headerBuffer[12];
        readBytes(connection.get(), 2, sizeof(headerBuffer), headerBuffer);
        const char* source = headerBuffer;
        size_t length = sizeof(headerBuffer);
        MessageHeader header;
        unmarshal(source, length, header);
        if (header.magic != RMC_MAGIC || header.reqType != RMC_UNMOUNT || header.lenOrStatus < 12
            || header.lenOrStatus > 256) {
          throw std::runtime_error("Unexpected test robot request");
        }
        std::vector<char> body(header.lenOrStatus - 12);
        readBytes(connection.get(), 2, body.size(), body.data());
        source = body.data();
        length = body.size();
        RmcUnmountMsgBody request;
        unmarshal(source, length, request);
        vids.emplace_back(request.vid);
        MessageHeader response;
        response.magic = RMC_MAGIC;
        response.reqType = RMC_RC;
        response.lenOrStatus = status;
        char responseBuffer[12];
        const auto bytes = marshal(responseBuffer, response);
        writeBytes(connection.get(), 2, bytes, responseBuffer);
      }
      return vids;
    });
  }

  uint16_t port() const { return m_port; }

  std::vector<std::string> requests() { return m_requests.get(); }

private:
  cta::SmartFd m_listener;
  uint16_t m_port;
  std::future<std::vector<std::string>> m_requests;
};

using cta::tape::daemon::DriveUsability;
using cta::tape::daemon::TapeSessionError;
using FailurePoint = cta::tape::drive::FakeDrive::FailurePoint;
using Tape = cta::common::dataStructures::Tape;

// Delegate normal tape operations so each test can fail just the catalogue operation under review.
class FailingTapeCatalogue : public cta::catalogue::DummyTapeCatalogue {
public:
  explicit FailingTapeCatalogue(cta::catalogue::TapeCatalogue& delegate) : m_delegate(delegate) {}

  std::exception_ptr lookupFailure;
  std::exception_ptr modificationFailure;
  std::exception_ptr labelFailure;
  unsigned int modifications = 0;

  cta::common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override {
    if (lookupFailure) {
      std::rethrow_exception(lookupFailure);
    }
    return m_delegate.getTapesByVid(vid);
  }

  cta::common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const override {
    if (labelFailure) {
      std::rethrow_exception(labelFailure);
    }
    return m_delegate.getTapeLabelFormat(vid);
  }

  void modifyTapeState(const cta::common::dataStructures::SecurityIdentity& admin,
                       const std::string& vid,
                       const Tape::State& state,
                       const std::optional<Tape::State>& previous,
                       const std::optional<std::string>& reason) override {
    ++modifications;
    if (modificationFailure) {
      std::rethrow_exception(modificationFailure);
    }
    m_delegate.modifyTapeState(admin, vid, state, previous, reason);
  }

private:
  cta::catalogue::TapeCatalogue& m_delegate;
};

class FailingDriveCatalogue : public cta::catalogue::DummyDriveStateCatalogue {
public:
  explicit FailingDriveCatalogue(cta::catalogue::DriveStateCatalogue& delegate) : m_delegate(delegate) {}

  std::exception_ptr reportedFailure;
  std::exception_ptr desiredFailure;
  unsigned int reportedCalls = 0;
  unsigned int desiredCalls = 0;

  bool updateTapeDriveStatus(const cta::common::dataStructures::TapeDrive& drive) override {
    ++reportedCalls;
    if (reportedFailure) {
      std::rethrow_exception(reportedFailure);
    }
    return m_delegate.updateTapeDriveStatus(drive);
  }

  void setDesiredTapeDriveState(const std::string& name,
                                const cta::common::dataStructures::DesiredDriveState& state) override {
    ++desiredCalls;
    if (desiredFailure) {
      std::rethrow_exception(desiredFailure);
    }
    m_delegate.setDesiredTapeDriveState(name, state);
  }

private:
  cta::catalogue::DriveStateCatalogue& m_delegate;
};

class CleanerCatalogue : public cta::catalogue::DummyCatalogue {
public:
  explicit CleanerCatalogue(cta::catalogue::Catalogue& delegate)
      : m_tape(std::make_unique<FailingTapeCatalogue>(*delegate.Tape())),
        m_drive(std::make_unique<FailingDriveCatalogue>(*delegate.DriveState())) {}

  const std::unique_ptr<cta::catalogue::TapeCatalogue>& Tape() const override { return m_tape; }

  const std::unique_ptr<cta::catalogue::DriveStateCatalogue>& DriveState() const override { return m_drive; }

  FailingTapeCatalogue& failures() { return static_cast<FailingTapeCatalogue&>(*m_tape); }

  FailingDriveCatalogue& driveFailures() { return static_cast<FailingDriveCatalogue&>(*m_drive); }

private:
  std::unique_ptr<cta::catalogue::TapeCatalogue> m_tape;
  std::unique_ptr<cta::catalogue::DriveStateCatalogue> m_drive;
};

struct DriveCleanerTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;
};

class DriveCleanerTest : public ::testing::TestWithParam<DriveCleanerTestParam> {
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

  DriveUsability runCleaner(const std::string& vid,
                            bool robotFails = false,
                            bool wait = false,
                            cta::catalogue::Catalogue* catalogue = nullptr) {
    cta::mediachanger::RmcProxy rmcProxy("localhost", 0, 1, 1);
    cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);
    auto info = m_driveInfo;
    if (robotFails) {
      info.rawLibrarySlot = "smc0";
    }
    cta::tape::daemon::DriveCleaner
      cleaner(mediaChanger, m_sessionLog, info, vid, wait, 0, catalogue ? *catalogue : *m_catalogue, m_tracker);
    return cleaner.execute(m_systemWrapper);
  }

  void assertDriveDown() {
    cta::log::LogContext lc(m_sessionLog);
    const auto desired = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, lc);
    ASSERT_FALSE(desired.up);
    ASSERT_TRUE(desired.reason);
    ASSERT_NE(std::string::npos, desired.reason->find("[cta-taped] ERROR Drive cleanup failed: "));
    const auto drive = m_catalogue->DriveState()->getTapeDrive(m_driveInfo.driveName);
    ASSERT_TRUE(drive);
    ASSERT_EQ(cta::common::dataStructures::DriveStatus::Down, drive->driveStatus);
  }

  void setTapeState(Tape::State state) {
    m_catalogue->Tape()->modifyTapeState(m_admin, m_vid, state, std::nullopt, "Test setup");
  }

  void assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint failurePoint) {
    cta::mediachanger::RmcProxy rmcProxy;
    cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

    auto* drive = installDrive();
    drive->setFailurePoint(failurePoint);
    cta::tape::daemon::DriveCleaner
      cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

    const bool resetFailed = failurePoint != cta::tape::drive::FakeDrive::FailurePoint::Rewind;
    ASSERT_EQ(resetFailed ? cta::tape::daemon::DriveUsability::MustRemainDown :
                            cta::tape::daemon::DriveUsability::Reusable,
              cleaner.execute(m_systemWrapper));
    cta::log::LogContext logContext(m_sessionLog);
    ASSERT_EQ(!resetFailed, m_scheduler->getDesiredDriveState(m_driveInfo.driveName, logContext).up);
    ASSERT_EQ(cta::common::dataStructures::Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
    ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
    ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
    const auto stats = m_tracker.stats().cleanup;
    ASSERT_GT(stats.cleanupTime, 0);
    ASSERT_GT(stats.rewindTime, 0);
    if (failurePoint == cta::tape::drive::FakeDrive::FailurePoint::Rewind) {
      ASSERT_EQ(0, stats.labelReadTime);
    } else {
      ASSERT_GT(stats.labelReadTime, 0);
    }
    ASSERT_GE(stats.cleanupTime,
              stats.encryptionControlTime + stats.lbpResetTime + stats.readinessWaitTime + stats.rewindTime
                + stats.labelReadTime + stats.unloadTime + stats.unmountTime);
  }

  cta::tape::daemon::TapeSessionTracker m_tracker;
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

TEST_P(DriveCleanerTest, EjectsBlankTape) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive();
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
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

TEST_P(DriveCleanerTest, EjectsLabeledTape) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  auto* drive = installDrive();
  cta::tape::tapeFile::LabelSession::label(drive, m_vid, false);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner read the VSN from the volume label"));
  ASSERT_EQ(std::string::npos,
            m_sessionLog.getLog().find("Cleaner failed to prepare the drive or read the volume label"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, EjectsTapeAfterEncryptionClearFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::ClearEncryptionKey);
}

TEST_P(DriveCleanerTest, EjectsTapeAfterRewindFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::Rewind);
}

TEST_P(DriveCleanerTest, EjectsTapeAfterLbpDisableFailure) {
  assertEjectsAfterDriveFailure(cta::tape::drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
}

TEST_P(DriveCleanerTest, BorrowedDriveResetsLbpAfterEncryptionClearFailureWithoutPublishingDown) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);
  cta::tape::drive::FakeDrive drive(5000, cta::tape::drive::FakeDrive::OnFlush);
  // Keep the existing accumulation assertions meaningful even for sub-microsecond fake operations.
  drive.setOperationDelay(cta::tape::drive::FakeDrive::FailurePoint::UnloadTape, std::chrono::milliseconds(1));
  drive.setOperationDelay(cta::tape::drive::FakeDrive::FailurePoint::Rewind, std::chrono::milliseconds(1));
  drive.setTapeInPlace(true);
  drive.enableCRC32CLogicalBlockProtectionReadWrite();
  drive.setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::ClearEncryptionKey);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  m_tracker.beginTapeSession();
  m_tracker.setType(cta::tape::session::SessionType::Retrieve);
  m_tracker.reportState(cta::tape::session::TapeSessionState::Finished);
  m_tracker.updateTapeSetupStats({.encryptionControlTime = 5});
  m_tracker.updateTapeTransferStats({.dataVolume = 1234, .filesCount = 2});
  m_tracker.updateTapeCleanupStats({.unloadTime = 10,
                                    .unmountTime = 20,
                                    .cleanupTime = 40,
                                    .lbpResetTime = 50,
                                    .readinessWaitTime = 60,
                                    .rewindTime = 70,
                                    .labelReadTime = 80,
                                    .encryptionControlTime = 30});
  const auto result = cleaner.cleanDrive(drive, [](auto) {});
  ASSERT_EQ(cta::tape::session::SessionType::Retrieve, m_tracker.type());
  ASSERT_EQ(5, m_tracker.stats().setup.encryptionControlTime);
  ASSERT_EQ(1234, m_tracker.stats().tape.dataVolume);
  ASSERT_EQ(2, m_tracker.stats().tape.filesCount);
  ASSERT_EQ(1, m_tracker.errorStats().at(cta::tape::daemon::TapeSessionError::TapeEncryptionDisable));
  ASSERT_TRUE(result.configurationResetFailed);
  ASSERT_FALSE(result.ejectFailed);
  ASSERT_FALSE(result.driveReusable());
  ASSERT_EQ(cta::tape::session::TapeSessionState::Finalizing, m_tracker.state());
  ASSERT_EQ(0, m_tracker.errorStats().count(cta::tape::daemon::TapeSessionError::TapeLbpDisable));
  ASSERT_GT(m_tracker.stats().cleanup.encryptionControlTime, 30);
  ASSERT_GT(m_tracker.stats().cleanup.unloadTime, 10);
  ASSERT_GT(m_tracker.stats().cleanup.unmountTime, 20);
  ASSERT_GT(m_tracker.stats().cleanup.cleanupTime, 40);
  ASSERT_GE(m_tracker.stats().cleanup.lbpResetTime, 50);
  ASSERT_EQ(60, m_tracker.stats().cleanup.readinessWaitTime);
  ASSERT_GT(m_tracker.stats().cleanup.rewindTime, 70);
  ASSERT_GT(m_tracker.stats().cleanup.labelReadTime, 80);
  ASSERT_EQ(cta::tape::drive::lbpToUse::disabled, drive.getLbpToUse());
  ASSERT_FALSE(drive.hasTapeInPlace());
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  cta::log::LogContext logContext(m_sessionLog);
  ASSERT_TRUE(m_scheduler->getDesiredDriveState(m_driveInfo.driveName, logContext).up);
}

TEST_P(DriveCleanerTest, BorrowedEmptyDriveAttemptsBothConfigurationResets) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);
  cta::tape::drive::FakeDrive drive(5000, cta::tape::drive::FakeDrive::OnFlush);
  drive.setTapeInPlace(false);
  drive.setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::ClearEncryptionKey);
  drive.setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, true, 0, *m_catalogue, m_tracker);

  const auto result = cleaner.cleanDrive(drive, [](auto) {});
  ASSERT_TRUE(result.configurationResetFailed);
  ASSERT_FALSE(result.ejectFailed);
  ASSERT_FALSE(result.driveReusable());
  ASSERT_EQ(cta::tape::session::TapeSessionState::Finalizing, m_tracker.state());
  ASSERT_EQ(cta::tape::session::SessionType::Undetermined, m_tracker.type());
  ASSERT_EQ(1, m_tracker.errorStats().at(cta::tape::daemon::TapeSessionError::TapeEncryptionDisable));
  ASSERT_EQ(1, m_tracker.errorStats().at(cta::tape::daemon::TapeSessionError::TapeLbpDisable));
  ASSERT_NE(std::string::npos, result.errorMessage.find("Failed to clear encryption key"));
  ASSERT_NE(std::string::npos, result.errorMessage.find("Failed to disable logical block protection"));
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  const auto stats = m_tracker.stats().cleanup;
  ASSERT_GT(stats.cleanupTime, 0);
  ASSERT_GT(stats.lbpResetTime, 0);
  ASSERT_EQ(0, stats.readinessWaitTime);
  ASSERT_EQ(0, stats.rewindTime);
  ASSERT_EQ(0, stats.labelReadTime);
  ASSERT_EQ(0, stats.unloadTime);
  ASSERT_EQ(0, stats.unmountTime);
}

TEST_P(DriveCleanerTest, DismountsTapeAfterUnloadFailure) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  auto* drive = installDrive();
  drive->setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::UnloadTape);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
  ASSERT_EQ(1, m_tracker.errorStats().at(cta::tape::daemon::TapeSessionError::TapeUnload));
  ASSERT_EQ(cta::tape::session::TapeSessionState::Finalizing, m_tracker.state());
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unload command failed"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, DisablesTapeAndPutsDriveDownWhenBothDismountAttemptsFail) {
  constexpr uint16_t unavailableRmcPort = 0;
  constexpr uint32_t networkTimeout = 1;
  constexpr uint32_t maxRequestAttempts = 1;
  cta::mediachanger::RmcProxy rmcProxy("localhost", unavailableRmcPort, networkTimeout, maxRequestAttempts);
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  auto* drive = installDrive();
  cta::tape::tapeFile::LabelSession::label(drive, m_vid, false);
  auto driveInfo = m_driveInfo;
  driveInfo.rawLibrarySlot = "smc0";
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::MustRemainDown, cleaner.execute(m_systemWrapper));
  ASSERT_EQ(2, m_tracker.errorStats().at(cta::tape::daemon::TapeSessionError::TapeDismount));
  ASSERT_EQ(cta::tape::session::SessionType::Undetermined, m_tracker.type());
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner failed to dismount tape with VID"));
  ASSERT_NE(std::string::npos,
            m_sessionLog.getLog().find("Cleaner requesting robotic tape dismount with an empty VID"));

  using Tape = cta::common::dataStructures::Tape;
  const auto tape = m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid);
  ASSERT_EQ(Tape::DISABLED, tape.state);

  cta::log::LogContext logContext(m_sessionLog);
  const auto desiredDriveState = m_scheduler->getDesiredDriveState(driveInfo.driveName, logContext);
  ASSERT_FALSE(desiredDriveState.up);

  const auto tapeDrive = m_catalogue->DriveState()->getTapeDrive(driveInfo.driveName);
  ASSERT_TRUE(tapeDrive.has_value());
  ASSERT_EQ(cta::common::dataStructures::DriveStatus::Down, tapeDrive->driveStatus);
}

TEST_P(DriveCleanerTest, AcceptsEmptyDriveWithoutDismounting) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive(false);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, EmptyDriveSkipsReadinessWaitAndResetsConfiguration) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);
  // A readiness call would throw; an empty drive must bypass it and still reset LBP.
  cta::tape::drive::FakeDrive drive(5000, cta::tape::drive::FakeDrive::OnFlush, true);
  drive.setTapeInPlace(false);
  drive.enableCRC32CLogicalBlockProtectionReadWrite();
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, true, 300, *m_catalogue, m_tracker);

  ASSERT_TRUE(cleaner.cleanDrive(drive, [](auto) {}).driveReusable());
  ASSERT_EQ(cta::tape::drive::lbpToUse::disabled, drive.getLbpToUse());
  ASSERT_EQ(0, m_tracker.stats().cleanup.readinessWaitTime);
  ASSERT_EQ(std::string::npos, m_sessionLog.getLog().find("Cleaner waiting for drive to become ready"));
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, OccupiedDriveWaitsForReadinessBeforeEjecting) {
  installDrive();
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid, false, true));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner detected that the drive is ready"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, MediaDetectionFailureRetainsReadinessWaitAndEjects) {
  installDrive()->setFailurePoint(FailurePoint::HasTapeInPlace);
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid, false, true));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("could not detect media before readiness wait"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner detected that the drive is ready"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, PutsEmptyDriveDownAfterConfigurationResetFailure) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);
  auto* drive = installDrive(false);
  drive->setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
  cta::tape::daemon::DriveCleaner
    cleaner(mediaChanger, m_sessionLog, m_driveInfo, m_vid, true, 300, *m_catalogue, m_tracker);

  ASSERT_EQ(cta::tape::daemon::DriveUsability::MustRemainDown, cleaner.execute(m_systemWrapper));
  ASSERT_EQ(0, m_tracker.stats().cleanup.readinessWaitTime);
  ASSERT_EQ(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  cta::log::LogContext logContext(m_sessionLog);
  ASSERT_FALSE(m_scheduler->getDesiredDriveState(m_driveInfo.driveName, logContext).up);
  ASSERT_EQ(cta::common::dataStructures::Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
}

TEST_P(DriveCleanerTest, EjectsTapeWithUnknownVid) {
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mediaChanger(rmcProxy, m_changerLog);

  installDrive();
  cta::tape::daemon::DriveCleaner
    unknownVidCleaner(mediaChanger, m_sessionLog, m_driveInfo, "", false, 0, *m_catalogue, m_tracker);
  ASSERT_EQ(cta::tape::daemon::DriveUsability::Reusable, unknownVidCleaner.execute(m_systemWrapper));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, DriveOpenFailureStillDismountsAndKeepsDriveDown) {
  // Device discovery succeeds; drive construction fails before ownership is established.
  EXPECT_CALL(m_systemWrapper, getDriveByPath("/dev/nst0"))
    .WillOnce(testing::Throw(std::runtime_error("drive open failed")));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid));
  assertDriveDown();
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("drive could not be opened"));
  cta::log::LogContext lc(m_sessionLog);
  const auto state = m_scheduler->getDesiredDriveState(m_driveInfo.driveName, lc);
  ASSERT_TRUE(state.reason);
  EXPECT_THAT(*state.reason, testing::HasSubstr("drive open failed"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find(R"(dismountVid="")"));
  ASSERT_EQ(0, m_tracker.errorStats().count(TapeSessionError::TapeDismount));
}

TEST_P(DriveCleanerTest, DriveDiscoveryFailureStillAttemptsDismount) {
  m_systemWrapper.fake.m_stats.erase(m_driveInfo.devFilename);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid));
  assertDriveDown();
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
}

TEST_P(DriveCleanerTest, DriveOpenAndDismountFailureDisableTapeAndKeepDriveDown) {
  EXPECT_CALL(m_systemWrapper, getDriveByPath("/dev/nst0"))
    .WillOnce(testing::Throw(cta::exception::Exception("drive open failed")));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true));
  assertDriveDown();
  ASSERT_EQ(Tape::DISABLED, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeDismount));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("failed to dismount the tape"));
}

TEST_P(DriveCleanerTest, ReadinessFailureDoesNotPreventResetsAndEject) {
  auto* drive = new cta::tape::drive::FakeDrive(5000, cta::tape::drive::FakeDrive::OnFlush, true);
  drive->enableCRC32CLogicalBlockProtectionReadWrite();
  m_systemWrapper.fake.m_pathToDrive["/dev/nst0"] = drive;
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid, false, true));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("non-fatal exception while waiting"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  ASSERT_EQ(0, m_tracker.errorStats().count(TapeSessionError::TapeLbpDisable));
}

TEST_P(DriveCleanerTest, MediaPresenceFailureStillAttemptsEject) {
  installDrive()->setFailurePoint(FailurePoint::HasTapeInPlace);
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("could not determine whether the drive contains a tape"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
}

TEST_P(DriveCleanerTest, CombinedResetFailuresStillEjectAndRecordBothErrors) {
  auto* drive = installDrive();
  drive->setFailurePoint(FailurePoint::ClearEncryptionKey);
  drive->setFailurePoint(FailurePoint::DisableLogicalBlockProtection);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid));
  assertDriveDown();
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeEncryptionDisable));
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeLbpDisable));
  ASSERT_NE(std::string::npos, m_changerLog.getLog().find("Dummy dismount"));
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
}

TEST_P(DriveCleanerTest, CombinedResetUnloadAndDismountFailuresRecordAllErrors) {
  auto* drive = installDrive();
  drive->setFailurePoint(FailurePoint::ClearEncryptionKey);
  drive->setFailurePoint(FailurePoint::DisableLogicalBlockProtection);
  drive->setFailurePoint(FailurePoint::UnloadTape);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true));
  assertDriveDown();
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeEncryptionDisable));
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeLbpDisable));
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeUnload));
  ASSERT_EQ(2, m_tracker.errorStats().at(TapeSessionError::TapeDismount));
  ASSERT_EQ(Tape::DISABLED, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
}

TEST_P(DriveCleanerTest, FailedEjectDisablesRepackingTape) {
  installDrive();
  setTapeState(Tape::REPACKING);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true));
  ASSERT_EQ(Tape::REPACKING_DISABLED, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  assertDriveDown();
}

TEST_P(DriveCleanerTest, FailedEjectPreservesStatesThatCannotBeDisabledAutomatically) {
  for (const auto state : {Tape::DISABLED,
                           Tape::REPACKING_DISABLED,
                           Tape::BROKEN,
                           Tape::EXPORTED,
                           Tape::BROKEN_PENDING,
                           Tape::REPACKING_PENDING,
                           Tape::EXPORTED_PENDING}) {
    SCOPED_TRACE(Tape::stateToString(state));
    installDrive();
    setTapeState(state);
    ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true));
    const auto tape = m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid);
    ASSERT_EQ(state, tape.state);
    ASSERT_EQ("Test setup", tape.stateReason);
    assertDriveDown();
  }
}

TEST_P(DriveCleanerTest, FailedEjectWithoutVidDoesNotDisableAnyTape) {
  installDrive();
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner("", true));
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeDismount));
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  ASSERT_NE(std::string::npos,
            m_sessionLog.getLog().find("cannot disable tape after failed eject because its VID is unknown"));
  assertDriveDown();
}

TEST_P(DriveCleanerTest, TapeLookupFailureDoesNotPreventDriveDownPublication) {
  installDrive();
  CleanerCatalogue catalogue(*m_catalogue);
  catalogue.failures().lookupFailure = std::make_exception_ptr(std::runtime_error("tape lookup failed"));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true, false, &catalogue));
  ASSERT_EQ(0, catalogue.failures().modifications);
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("tape lookup failed"));
  assertDriveDown();
}

TEST_P(DriveCleanerTest, TapeStateModificationFailureDoesNotPreventDriveDownPublication) {
  installDrive();
  CleanerCatalogue catalogue(*m_catalogue);
  catalogue.failures().modificationFailure = std::make_exception_ptr(cta::exception::Exception("state update failed"));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true, false, &catalogue));
  ASSERT_EQ(1, catalogue.failures().modifications);
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("state update failed"));
  assertDriveDown();
}

TEST_P(DriveCleanerTest, LabelFormatLookupFailureStillEjectsWithProvidedVid) {
  auto* drive = installDrive();
  cta::tape::tapeFile::LabelSession::label(drive, m_vid, false);
  CleanerCatalogue catalogue(*m_catalogue);
  catalogue.failures().labelFailure = std::make_exception_ptr(std::runtime_error("label format unavailable"));
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid, false, false, &catalogue));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("label format unavailable"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find(R"(dismountVid="TSTVID")"));
}

TEST_P(DriveCleanerTest, DesiredStatePublicationFailureIsContainedAfterReportedDown) {
  installDrive(false)->setFailurePoint(FailurePoint::ClearEncryptionKey);
  CleanerCatalogue catalogue(*m_catalogue);
  auto& failures = catalogue.driveFailures();
  failures.desiredFailure = std::make_exception_ptr(std::runtime_error("desired publication failed"));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, false, false, &catalogue));
  ASSERT_EQ(1, failures.reportedCalls);
  ASSERT_EQ(1, failures.desiredCalls);
  ASSERT_EQ(cta::common::dataStructures::DriveStatus::Down,
            m_catalogue->DriveState()->getTapeDrive(m_driveInfo.driveName)->driveStatus);
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("desired publication failed"));
}

TEST_P(DriveCleanerTest, ReportedStatePublicationFailureIsContained) {
  installDrive(false)->setFailurePoint(FailurePoint::ClearEncryptionKey);
  CleanerCatalogue catalogue(*m_catalogue);
  auto& failures = catalogue.driveFailures();
  failures.reportedFailure = std::make_exception_ptr(cta::exception::Exception("reported publication failed"));
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, false, false, &catalogue));
  ASSERT_EQ(1, failures.reportedCalls);
  ASSERT_EQ(1, failures.desiredCalls);
  cta::log::LogContext lc(m_sessionLog);
  ASSERT_FALSE(m_scheduler->getDesiredDriveState(m_driveInfo.driveName, lc).up);
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("reported publication failed"));
}

TEST_P(DriveCleanerTest, TapeAlertsAreCountedAndLoggedAfterSuccessfulCleanup) {
  installDrive()->setTapeAlertCodes({1, 2, 1});
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid));
  ASSERT_EQ(2, m_tracker.tapeAlertStats().at(1));
  ASSERT_EQ(1, m_tracker.tapeAlertStats().at(2));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Fake tape alert 1"));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Fake tape alert 2"));
}

TEST_P(DriveCleanerTest, TapeAlertsAreReportedAfterFailedCleanup) {
  auto* drive = installDrive();
  drive->setTapeAlertCodes({1});
  drive->setFailurePoint(FailurePoint::ClearEncryptionKey);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid));
  ASSERT_EQ(1, m_tracker.tapeAlertStats().at(1));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Fake tape alert 1"));
}

TEST_P(DriveCleanerTest, TapeAlertQueryFailureDoesNotChangeReusableResult) {
  installDrive()->setFailurePoint(FailurePoint::TapeAlertCodes);
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner failed to get tape alerts"));
}

TEST_P(DriveCleanerTest, TapeAlertDescriptionFailurePreservesCountedAlerts) {
  auto* drive = installDrive();
  drive->setTapeAlertCodes({1, 2});
  drive->setFailurePoint(FailurePoint::TapeAlerts);
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid));
  ASSERT_EQ(1, m_tracker.tapeAlertStats().at(1));
  ASSERT_EQ(1, m_tracker.tapeAlertStats().at(2));
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("Cleaner failed to get tape alerts"));
}

TEST_P(DriveCleanerTest, TracksSuccessAndFailure) {
  installDrive()->setTapeAlertCodes({1});
  ASSERT_EQ(DriveUsability::Reusable, runCleaner(m_vid));
  installDrive()->setFailurePoint(FailurePoint::ClearEncryptionKey);
  ASSERT_EQ(DriveUsability::MustRemainDown, runCleaner(m_vid, true));
  ASSERT_EQ(Tape::DISABLED, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  assertDriveDown();
}

TEST_P(DriveCleanerTest, BorrowedCleanupReportsProgressBeforeHardwareOperations) {
  using cta::common::dataStructures::DriveStatus;
  using cta::tape::session::TapeSessionState;
  cta::mediachanger::RmcProxy proxy;
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  cta::tape::drive::FakeDrive drive;
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);
  std::vector<DriveStatus> statuses;
  const auto result = cleaner.cleanDrive(drive, [&](DriveStatus status) {
    statuses.push_back(status);
    EXPECT_EQ(status == DriveStatus::Unloading ? TapeSessionState::Unloading : TapeSessionState::Unmounting,
              m_tracker.state());
    EXPECT_EQ(status == DriveStatus::Unloading, drive.hasTapeInPlace());
  });
  EXPECT_TRUE(result.driveReusable());
  EXPECT_EQ((std::vector {DriveStatus::Unloading, DriveStatus::Unmounting}), statuses);
  EXPECT_EQ(TapeSessionState::Finalizing, m_tracker.state());
}

TEST_P(DriveCleanerTest, ProgressPublicationFailuresDoNotPreventEject) {
  using cta::common::dataStructures::DriveStatus;
  cta::mediachanger::RmcProxy proxy;
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  cta::tape::drive::FakeDrive drive;
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);
  unsigned int calls = 0;
  const auto result = cleaner.cleanDrive(drive, [&](DriveStatus status) {
    ++calls;
    if (status == DriveStatus::Unloading) {
      throw std::runtime_error("publication failed");
    }
    throw 42;
  });
  EXPECT_TRUE(result.driveReusable());
  EXPECT_FALSE(drive.hasTapeInPlace());
  EXPECT_EQ(2, calls);
  EXPECT_EQ(2, m_tracker.errorStats().at(TapeSessionError::Reporting));
  EXPECT_EQ(cta::tape::daemon::TapeSessionOutcome::Failure, m_tracker.outcome());
}

TEST_P(DriveCleanerTest, BorrowedDriveRemainsOwnedByCallerAndResetsPersistentConfiguration) {
  cta::mediachanger::RmcProxy proxy;
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  cta::tape::drive::FakeDrive drive;
  drive.enableCRC32CLogicalBlockProtectionReadWrite();
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, m_driveInfo, m_vid, false, 0, *m_catalogue, m_tracker);
  const auto result = cleaner.cleanDrive(drive, [](auto) {});
  ASSERT_TRUE(result.driveReusable());
  ASSERT_FALSE(result.configurationResetFailed);
  ASSERT_FALSE(result.ejectFailed);
  ASSERT_TRUE(result.errorMessage.empty());
  ASSERT_FALSE(drive.hasTapeInPlace());
  ASSERT_EQ(cta::tape::drive::lbpToUse::disabled, drive.getLbpToUse());
}

TEST_P(DriveCleanerTest, BorrowedFailedEjectDisablesTapeWithoutPublishingDown) {
  cta::mediachanger::RmcProxy proxy("localhost", 0, 1, 1);
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  cta::tape::drive::FakeDrive drive;
  auto info = m_driveInfo;
  info.rawLibrarySlot = "smc0";
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, info, m_vid, false, 0, *m_catalogue, m_tracker);
  const auto result = cleaner.cleanDrive(drive, [](auto) {});
  ASSERT_FALSE(result.driveReusable());
  ASSERT_TRUE(result.ejectFailed);
  ASSERT_FALSE(result.configurationResetFailed);
  ASSERT_NE(std::string::npos, result.errorMessage.find("Failed to dismount tape"));
  ASSERT_EQ(Tape::DISABLED, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
  cta::log::LogContext lc(m_sessionLog);
  ASSERT_TRUE(m_scheduler->getDesiredDriveState(m_driveInfo.driveName, lc).up);
  ASSERT_EQ(cta::common::dataStructures::DriveStatus::Up,
            m_catalogue->DriveState()->getTapeDrive(m_driveInfo.driveName)->driveStatus);
}

TEST_P(DriveCleanerTest, DismountRetriesWithEmptyVidAfterCartridgeNameFailure) {
  ScriptedRobot robot({1, 0});
  cta::mediachanger::RmcProxy proxy("localhost", robot.port(), 2, 1);
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  installDrive();
  auto info = m_driveInfo;
  info.rawLibrarySlot = "smc0";
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, info, m_vid, false, 0, *m_catalogue, m_tracker);
  ASSERT_EQ(DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
  ASSERT_EQ((std::vector<std::string> {m_vid, ""}), robot.requests());
  ASSERT_EQ(1, m_tracker.errorStats().at(TapeSessionError::TapeDismount));
  ASSERT_EQ(Tape::ACTIVE, m_catalogue->Tape()->getTapesByVid(m_vid).at(m_vid).state);
}

TEST_P(DriveCleanerTest, MismatchedLabelUsesActualLabelForRoboticDismount) {
  const std::string actualVid = "ACTUAL";
  ScriptedRobot robot({0});
  cta::mediachanger::RmcProxy proxy("localhost", robot.port(), 2, 1);
  cta::mediachanger::MediaChangerFacade changer(proxy, m_changerLog);
  auto* drive = installDrive();
  cta::tape::tapeFile::LabelSession::label(drive, actualVid, false);
  auto info = m_driveInfo;
  info.rawLibrarySlot = "smc0";
  cta::tape::daemon::DriveCleaner cleaner(changer, m_sessionLog, info, m_vid, false, 0, *m_catalogue, m_tracker);
  ASSERT_EQ(DriveUsability::Reusable, cleaner.execute(m_systemWrapper));
  ASSERT_EQ((std::vector<std::string> {actualVid}), robot.requests());
  ASSERT_NE(std::string::npos, m_sessionLog.getLog().find("volume label does not match provided VID"));
}

#ifdef CTA_PGSCHED
cta::RelationalDBTestFactory relationalDbTestFactory;
INSTANTIATE_TEST_SUITE_P(RelationalDB,
                         DriveCleanerTest,
                         ::testing::Values(DriveCleanerTestParam {relationalDbTestFactory}));
#else
cta::OStoreDBFactory<cta::objectstore::BackendVFS> objectStoreDbFactory;
INSTANTIATE_TEST_SUITE_P(ObjectStore,
                         DriveCleanerTest,
                         ::testing::Values(DriveCleanerTestParam {objectStoreDbFactory}));
#endif

}  // namespace

}  // namespace unitTests
