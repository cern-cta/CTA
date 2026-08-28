/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeDaemon.hpp"

#include "DriveStartupPolicy.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/ExceptionLogging.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/ScopeExit.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "taped/session/DriveSession.hpp"

#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <unistd.h>

namespace cta::tape::daemon {

TapeDaemon::TapeDaemon(const TapedConfig& config,
                       log::Logger& log,
                       catalogue::Catalogue& catalogue,
                       SchedulerContext& schedulerContext)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log),
      m_catalogue(catalogue),
      m_schedulerContext(schedulerContext) {}

void TapeDaemon::stop() {
  if (!m_stopSource.request_stop() || !m_registered.load()) {
    return;
  }

  // stop() can run on another thread; use a separate context because LogContext is not thread-safe.
  log::LogContext lc(m_lc.logger());
  try {
    TapeDrivesCatalogueState(m_catalogue)
      .requestDriveDown(
        m_driveInfo.driveName,
        common::dataStructures::DriveDownRequest::forDaemon(common::dataStructures::DriveDownReason::Shutdown),
        lc);
  } catch (...) {
    log::logCurrentExceptionNoThrow(lc, "Failed to request drive down while stopping.");
  }
}

bool TapeDaemon::isLive() const {
  // Lock to ensure we are not accessing the session while it's being destructed.
  std::lock_guard lock(m_sessionMutex);
  return !m_activeDriveSession || m_activeDriveSession->isLive();
}

bool TapeDaemon::isReady() const {
  // Readiness cannot require a logical library: system tests create it after services become ready.
  return m_registered.load();
}

int TapeDaemon::run() {
  const utils::ScopeExit clearReadiness([this] { m_registered.store(false); });
  try {
    if (!registerDrive()) {
      return shutdown(ExitCause::RegistrationFailure,
                      false,
                      "Drive name belongs to a different host or logical library");
    }
  } catch (...) {
    return shutdown(ExitCause::RegistrationFailure, false, exception::currentExceptionMessage());
  }

  try {
    // An absent logical library can appear later; wait to avoid transient scheduling errors.
    waitForLogicalLibrary();
    while (!m_stopSource.stop_requested()) {
      // Only spawn drive sessions when we're Up
      waitUntilDriveIsRequestedUp();
      if (m_stopSource.stop_requested()) {
        break;
      }
      auto session = std::make_unique<DriveSession>(m_config, m_lc.logger(), m_schedulerContext);
      // Unpublish before destroying the session, including before an outer catch handles failure.
      const utils::ScopeExit clearActiveSession([this] {
        std::lock_guard lock(m_sessionMutex);
        m_activeDriveSession = nullptr;
      });
      {
        std::lock_guard lock(m_sessionMutex);
        m_activeDriveSession = session.get();
      }
      // No need to lock around run.
      session->run(m_stopSource.get_token());
    }
  } catch (const TapeDrivesCatalogueState::NoSuchDrive& ex) {
    return shutdown(ExitCause::MissingDrive, false, ex.getMessageValue());
  } catch (...) {
    return shutdown(ExitCause::UnexpectedFailure, true, exception::currentExceptionMessage());
  }

  return shutdown(ExitCause::Normal, true);
}

int TapeDaemon::shutdown(ExitCause cause, bool putDriveDown, std::string_view exceptionMsg) {
  using common::dataStructures::DriveDownReason;
  auto reason = DriveDownReason::Shutdown;
  int exitCode = cause == ExitCause::Normal ? 0 : 1;
  const char* message = "Tape daemon shutting down.";
  switch (cause) {
    case ExitCause::Normal:
      message = "Tape daemon shutting down.";
      break;
    case ExitCause::RegistrationFailure:
      message = "Drive registration failed. Exiting.";
      break;
    case ExitCause::MissingDrive:
      // A later daemon run will register the drive from scratch.
      message = "Drive is missing from the catalogue. Exiting.";
      break;
    case ExitCause::UnexpectedFailure:
      reason = DriveDownReason::UnexpectedFailure;
      message = "Tape daemon failed. Requesting Down before exit.";
      break;
  }
  {
    log::ScopedParamContainer params(m_lc);
    if (!exceptionMsg.empty()) {
      params.add(semconv::log::exceptionMessage, std::string(exceptionMsg));
    }
    m_lc.log(cause == ExitCause::Normal ? log::INFO : log::ERR, message);
  }

  if (putDriveDown == false) {
    return exitCode;
  }

  // Preserve specific reasons. Physical cleanup and session release belong to DriveSession.
  try {
    TapeDrivesCatalogueState(m_catalogue)
      .requestDriveDown(m_driveInfo.driveName, common::dataStructures::DriveDownRequest::forDaemon(reason), m_lc);
  } catch (...) {
    log::logCurrentExceptionNoThrow(m_lc, "Failed to request desired Down before daemon exit.");
    exitCode = 1;
  }
  try {
    TapeDrivesCatalogueState(m_catalogue)
      .reportDriveState(m_driveInfo, {.state = common::dataStructures::ReportedDriveState::Down}, m_lc);
  } catch (...) {
    log::logCurrentExceptionNoThrow(m_lc, "Failed to report Down before daemon exit.");
    exitCode = 1;
  }
  return exitCode;
}

void TapeDaemon::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (!m_stopSource.stop_requested()) {
    const auto libraries = m_catalogue.LogicalLibrary()->getLogicalLibraries();
    const bool exists = std::any_of(libraries.begin(), libraries.end(), [this](const auto& library) {
      return library.name == m_driveInfo.logicalLibrary;
    });

    if (exists) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Logical library " + m_driveInfo.logicalLibrary + " is now available. Continuing startup.");
      }
      return;
    }

    if (!waitingLogged) {
      m_lc.log(log::WARNING,
               "Logical library " + m_driveInfo.logicalLibrary + " does not exist. Waiting for creation.");
      waitingLogged = true;
    }

    // Database failures propagate; only an absent library is retried here.
    ::sleep(m_config.mounts.logical_library_poll_interval_secs);
  }
}

void TapeDaemon::waitUntilDriveIsRequestedUp() {
  TapeDrivesCatalogueState driveStateCatalogue(m_catalogue);
  bool waitingLogged = false;

  while (!m_stopSource.stop_requested()) {
    const auto desiredState = driveStateCatalogue.getDesiredDriveState(m_driveInfo.driveName);

    if (desiredState.up) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive preparation.");
      }
      return;
    }

    // Prevent too many repeating log messages
    if (!waitingLogged) {
      m_lc.log(log::INFO, "Waiting for the desired drive state to become up.");
      waitingLogged = true;
    }

    // Keep the catalogue timestamp fresh so a waiting drive is not shown as stale.
    driveStateCatalogue.reportDriveState(m_driveInfo,
                                         {.state = common::dataStructures::ReportedDriveState::Down},
                                         m_lc);
    ::sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

bool TapeDaemon::registerDrive() {
  m_registered.store(false);
  TapeDrivesCatalogueState driveStateCatalogue(m_catalogue);
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  try {
    driveStateCatalogue.checkDriveCanBeCreated(m_driveInfo);
  } catch (const TapeDrivesCatalogueState::DriveAlreadyExistsException& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::CRIT, "Drive name belongs to a different host or logical library.");
    return false;
  }

  const auto previous = m_catalogue.DriveState()->getTapeDrive(m_driveInfo.driveName);
  // Record the previous state before registration can overwrite evidence of an interrupted daemon.
  if (previous && previous->desiredUp) {
    log::ScopedParamContainer params(m_lc);
    params.add("driveStatus", common::dataStructures::TapeDrive::stateToString(previous->driveStatus))
      .add("startupRecoveryEnabled", m_config.drive.startup.recover_existing_up);
    m_lc.logEvent(log::WARNING,
                  "Drive found with desired Up at startup.",
                  semconv::log::EventNameValues::kDriveDesiredUpAtStartup);
  }
  const auto decision = decideDriveStartup(m_config.drive.startup, m_config.drive.registration, previous);
  using namespace common::dataStructures;
  using Action = DriveStartupDecision::Action;
  std::string message;

  switch (decision.action) {
    case Action::RecoverExistingUp:
      // Preserve operator intent while drive preparation establishes readiness for tape work.
      driveStateCatalogue.reportDriveState(m_driveInfo, {.state = ReportedDriveState::Down}, m_lc);
      message = "Reset reported drive status to Down. Preparing drive for recovery.";
      break;
    case Action::RequireOperatorUp:
      driveStateCatalogue.requestDriveDown(m_driveInfo.driveName,
                                           DriveDownRequest::forDaemon(DriveDownReason::StartupRecoveryDisabled),
                                           m_lc);
      driveStateCatalogue.reportDriveState(m_driveInfo, {.state = ReportedDriveState::Down}, m_lc);
      message = "Startup recovery disabled. Set drive Down; operator Up required.";
      break;
    case Action::Register: {
      const SecurityIdentity securityIdentity;
      driveStateCatalogue.createTapeDriveStatus(m_driveInfo,
                                                decision.desiredState,
                                                MountType::NoMount,
                                                DriveStatus::Down,
                                                securityIdentity,
                                                m_lc);
      message = "Drive registered with reported status down and desired state "
                + std::string(decision.desiredState.up ? "up." : "down.");
      break;
    }
  }

  // Readiness is published only after registration and backend publication both succeed.
  {
    utils::Timer timer;
    // Store backend identity as configuration until the drive-state schema has a dedicated column.
    constexpr const char* backendNameKey = "SchedulerBackendName";
    auto& driveConfig = *m_catalogue.DriveConfig();
    if (driveConfig.getTapeDriveConfig(m_driveInfo.driveName, backendNameKey)) {
      driveConfig.modifyTapeDriveConfig(m_driveInfo.driveName,
                                        "general",
                                        backendNameKey,
                                        m_config.scheduler.backend_name,
                                        "TOML config file");
    } else {
      driveConfig.createTapeDriveConfig(m_driveInfo.driveName,
                                        "general",
                                        backendNameKey,
                                        m_config.scheduler.backend_name,
                                        "TOML config file");
    }
    log::ScopedParamContainer params(m_lc);
    params.add("drive", m_driveInfo.driveName).add("schedulerDbTime", timer.secs());
    m_lc.log(log::DEBUG, "Successfully reported the scheduler backend name of drive in the catalogue.");
  }
  m_lc.log(log::INFO, message);
  m_registered.store(true);
  return true;
}

}  // namespace cta::tape::daemon
