/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "TapedMetricsTestUtils.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyDriveStateCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"

#include <array>
#include <future>
#include <gtest/gtest.h>
#include <thread>

namespace cta::telemetry::testing {
using namespace common::dataStructures;

namespace {
class RecordingDriveCatalogue : public catalogue::DummyDriveStateCatalogue {
public:
  bool fail = false;

  bool updateTapeDriveStatus(const TapeDrive& drive) override {
    // The local snapshot must already be visible, even if the backend rejects this update.
    EXPECT_EQ(drive.driveStatus, metrics::getDriveStatus());
    if (fail) {
      throw std::runtime_error("catalogue unavailable");
    }
    return true;
  }

  void createTapeDrive(const TapeDrive& drive) override { EXPECT_EQ(drive.driveStatus, metrics::getDriveStatus()); }

  std::vector<std::string> getTapeDriveNames() const override { return {}; }
};

class RecordingCatalogue : public catalogue::DummyCatalogue {
public:
  const std::unique_ptr<catalogue::DriveStateCatalogue>& DriveState() const override { return drive; }

  std::unique_ptr<catalogue::DriveStateCatalogue> drive = std::make_unique<RecordingDriveCatalogue>();
};
}  // namespace

TEST(TapedMetricsTest, CatalogueReportsAndRegistrationUpdateSnapshotBeforeBackendAccess) {
  ScopedTapedMetrics observed;
  RecordingCatalogue catalogue;
  TapeDrivesCatalogueState state(catalogue);
  log::DummyLogger logger("host", "test");
  log::LogContext lc(logger);
  DriveInfo info("drive", "host", "library", "device", "slot");
  state
    .createTapeDriveStatus(info, DesiredDriveState {}, MountType::NoMount, DriveStatus::Down, SecurityIdentity {}, lc);
  EXPECT_EQ(1, observed.driveStatus(DriveStatus::Down));
  state.reportDriveStatus(info, MountType::NoMount, DriveStatus::CleaningUp, 0, lc);
  EXPECT_EQ(1, observed.driveStatus(DriveStatus::CleaningUp));
  static_cast<RecordingDriveCatalogue&>(*catalogue.drive).fail = true;
  ReportDriveStatusInputs inputs {};
  inputs.status = DriveStatus::Down;
  inputs.mountType = MountType::NoMount;
  EXPECT_THROW(state.updateDriveStatus(info, inputs, lc), std::runtime_error);
  EXPECT_EQ(1, observed.driveStatus(DriveStatus::Down));
}

TEST(TapedMetricsTest, StalledMetricCollectionDoesNotBlockCatalogueReporting) {
  ScopedTapedMetrics observed;
  RecordingCatalogue catalogue;
  TapeDrivesCatalogueState state(catalogue);
  log::DummyLogger logger("host", "test");
  log::LogContext lc(logger);
  DriveInfo info("drive", "host", "library", "device", "slot");

  struct BlockedObserver {
    std::promise<void> entered;
    std::promise<void> release;
  } blocked;

  const auto callback = +[](opentelemetry::metrics::ObserverResult, void* context) noexcept {
    auto& observer = *static_cast<BlockedObserver*>(context);
    observer.entered.set_value();
    observer.release.get_future().wait();
  };
  metrics::CtaTapedDriveStatus->AddCallback(callback, &blocked);
  auto collecting = std::async(std::launch::async, [&] { return observed.driveStatus(DriveStatus::Unknown); });
  EXPECT_EQ(std::future_status::ready, blocked.entered.get_future().wait_for(std::chrono::seconds(2)));
  auto reporting = std::async(std::launch::async,
                              [&] { state.reportDriveStatus(info, MountType::NoMount, DriveStatus::Down, 0, lc); });
  // Always release collection before joining, even when the independence assertion fails.
  EXPECT_EQ(std::future_status::ready, reporting.wait_for(std::chrono::seconds(2)));
  blocked.release.set_value();
  collecting.get();
  reporting.get();
  metrics::CtaTapedDriveStatus->RemoveCallback(callback, &blocked);
  EXPECT_EQ(1, observed.driveStatus(DriveStatus::Down));
}

TEST(TapedMetricsTest, EmitsCurrentStateAndZeroesForInactiveCategories) {
  ScopedTapedMetrics observed;
  EXPECT_EQ(1, observed.driveStatus(DriveStatus::Unknown));
  EXPECT_EQ(1, observed.mountType(MountType::NoMount));
  for (const auto active : AllDriveStatuses) {
    metrics::setDriveStatus(active);
    const auto values =
      observed.collect(semconv::metrics::kMetricCtaTapedDriveStatus, semconv::attr::kCtaTapedDriveState);
    ASSERT_EQ(AllDriveStatuses.size(), values.size());
    for (const auto status : AllDriveStatuses) {
      EXPECT_EQ(status == active ? 1 : 0, values.at(toString(status)));
    }
  }
  const std::array types {MountType::NoMount,
                          MountType::ArchiveForUser,
                          MountType::ArchiveForRepack,
                          MountType::Retrieve};
  for (const auto active : types) {
    metrics::setMountType(active);
    const auto values = observed.collect(semconv::metrics::kMetricCtaTapedMountType, semconv::attr::kCtaTapedMountType);
    ASSERT_EQ(types.size(), values.size());
    for (const auto type : types) {
      EXPECT_EQ(type == active ? 1 : 0, values.at(toCamelCaseString(type)));
    }
  }
}

TEST(TapedMetricsTest, ReinitializationDoesNotDuplicateCallbacks) {
  ScopedTapedMetrics observed;
  metrics::setMountType(MountType::Retrieve);
  metrics::initAllInstruments();
  EXPECT_EQ(1, observed.mountType(MountType::Retrieve));
}

TEST(TapedMetricsTest, ConcurrentUpdatesProduceOneActiveCategory) {
  ScopedTapedMetrics observed;
  std::jthread updates([](std::stop_token stop) {
    while (!stop.stop_requested()) {
      metrics::setDriveStatus(DriveStatus::Up);
      metrics::setDriveStatus(DriveStatus::Down);
    }
  });
  for (int i = 0; i < 20; ++i) {
    const auto values =
      observed.collect(semconv::metrics::kMetricCtaTapedDriveStatus, semconv::attr::kCtaTapedDriveState);
    int64_t active = 0;
    for (const auto& [name, value] : values) {
      active += value;
    }
    EXPECT_EQ(1, active);
  }
}
}  // namespace cta::telemetry::testing
