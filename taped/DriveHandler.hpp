/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapedConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/Scheduler.hpp"
#include "session/TapeSessionTracker.hpp"
#include "session/TransferSessionResult.hpp"
#include "system/Wrapper.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

namespace cta::tape::daemon {

class DriveHandler final {
public:
  DriveHandler(const TapedConfig& tapedConfig, cta::log::Logger& lc);

  // Inject external operations while retaining the handler's lifecycle and recovery decisions.
  // TODO: ideally remove this and update the unit tests to work without it
  struct Operations {
    std::function<bool()> logicalLibraryExists;
    std::function<std::pair<bool, std::optional<std::string>>()> probeDrive;
    std::function<std::unique_ptr<TapeMount>()> getNextMount;
    std::function<TransferSessionResult(TapeMount&)> transfer;
    std::function<bool()> clean;
    std::function<void(unsigned int)> sleep;
  };

  // The config and scheduler must outlive the handler. All injected operations must be supplied.
  DriveHandler(const TapedConfig& config, log::Logger& log, IScheduler& scheduler, Operations operations);

  ~DriveHandler() = default;

  void stop();

  int run();

  bool isLive() const;

  bool isReady() const;

private:
  friend class DriveHandlerTest;

  struct InitializeMembers {};

  DriveHandler(const TapedConfig& config, log::Logger& log, InitializeMembers);
  void runIteration();
  void sleep(unsigned int seconds);

  IScheduler* m_driveScheduler = nullptr;
  Operations m_operations;

  // Helpers recover only from expected local conditions. Operational failures propagate to run().
  // Registration returns false for an ownership conflict and creates an entry if one is absent.
  bool registerDrive(bool putUpIfPossible);
  // Wait for the configured logical library to exist before scheduling at startup.
  void waitForLogicalLibrary();
  // Read desired state, registering a missing drive as down. Operational failures propagate.
  common::dataStructures::DesiredDriveState getDesiredDriveState();
  // Re-register a missing drive as down while polling for the operator's desired state.
  void waitForDriveToBeUp();
  // Attempt both publications and propagate the first failure after logging each failed operation.
  void putDriveDown(common::dataStructures::DriveDownReason reason,
                    std::string_view detail = {},
                    bool preserveExistingReason = false);
  // Return false when probing requests down and scheduling must be skipped.
  bool prepareDriveForScheduling();
  // Clean and publish down, returning a nonzero exit code if either operation fails.
  int shutdownDrive();
  TransferSessionResult executeDataTransferSession(TapeMount& tapeMount);
  bool executeCleanerSession(const std::optional<std::string>& vid = std::nullopt, bool waitMediaInDrive = true);
  // A null mount means no work is available. Scheduling failures propagate.
  std::unique_ptr<TapeMount> getNextMount();

  std::stop_source m_stopSource;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;

  mediachanger::MediaChangerFacade m_mediaChanger;
  // TODO: maybe we can get away with not having this here
  System::realWrapper m_sysWrapper;
  std::unique_ptr<catalogue::Catalogue> m_catalogue;
  // TODO: can we settle with just catalogue and scheduler?
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  std::unique_ptr<Scheduler> m_scheduler;
  TapeSessionTracker m_tapeSessionTracker;
};

}  // namespace cta::tape::daemon
