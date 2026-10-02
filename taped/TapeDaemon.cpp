/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeDaemon.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/ScopeExit.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/Scheduler.hpp"
#include "session/TapeSessionWorkerTeardownIncomplete.hpp"
#include "taped/session/DriveSession.hpp"
#include "taped/session/DriveStatePublication.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <unistd.h>

namespace cta::tape::daemon {

TapeDaemon::TapeDaemon(const TapedConfig& config, log::Logger& log)
    : m_config(config),
      m_driveInfo(config.drive.name,
                  utils::getShortHostname(),
                  config.drive.logical_library_name,
                  config.drive.device,
                  config.drive.control_path),
      m_lc(log) {
  m_lc.log(log::INFO, "Initialising Catalogue");
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(m_config.catalogue.config_file);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);
  m_ownedCatalogue = catalogueFactory->create();
  m_catalogue = m_ownedCatalogue.get();

  m_lc.log(log::INFO, "Catalogue initialised successfully");
  m_ownedSchedulerContext = std::make_unique<SchedulerContext>(m_config, *m_catalogue, log);
  m_schedulerContext = m_ownedSchedulerContext.get();
}

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
      m_catalogue(&catalogue),
      m_schedulerContext(&schedulerContext) {}

void TapeDaemon::stop() {
  // Always retain the exit request, even if catalogue publication fails.
  // Before registration completes, run() is responsible for publishing down on exit.
  if (!m_stopSource.request_stop() || !m_registered.load()) {
    return;
  }

  // The signal-reactor thread must not share the daemon's scoped log parameters.
  log::LogContext lc(m_lc.logger());
  try {
    // Use the stable catalogue directly; the run thread may be replacing the scheduler.
    requestDriveDown(*m_catalogue, m_driveInfo.driveName, common::dataStructures::DriveDownReason::Shutdown, lc);
  } catch (const std::exception& ex) {
    log::ScopedParamContainer params(lc);
    const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex);
    params.add(semconv::log::exceptionMessage, ctaException ? ctaException->getMessageValue() : ex.what());
    lc.log(log::ERR, "Failed to request drive down while stopping. Daemon exit is still requested.");
  } catch (...) {
    lc.log(log::ERR, "Unknown failure requesting drive down while stopping. Daemon exit is still requested.");
  }
}

bool TapeDaemon::isLive() const {
  // Keep the borrowed session alive throughout delegation, without delaying its destruction onto a health thread.
  std::lock_guard lock(m_sessionMutex);
  return !m_activeDriveSession || m_activeDriveSession->isLive();
}

bool TapeDaemon::isReady() const {
  // Taped is considered ready when the drive has been registered in the catalogue
  // One could argue that the logical library should also exist for it to be ready
  // But that currently presents problems with our system tests, where the
  // logical libraries are only created after the deployment is complete
  // (which requires all services to be ready)
  // Potentially something to improve in the future
  return m_registered.load();
}

int TapeDaemon::run() {
  const utils::ScopeExit clearReadiness([this] { m_registered.store(false); });
  try {
    if (!registerDrive(false)) {
      return shutdown(ExitCause::RegistrationFailure,
                      DownPublication::None,
                      "Drive name belongs to a different host or logical library");
    }
  } catch (const exception::Exception& ex) {
    return shutdown(ExitCause::RegistrationFailure, DownPublication::None, ex.getMessageValue());
  } catch (const std::exception& ex) {
    return shutdown(ExitCause::RegistrationFailure, DownPublication::None, ex.what());
  } catch (...) {
    return shutdown(ExitCause::RegistrationFailure, DownPublication::None, "Unknown exception");
  }

  try {
    // An absent logical library can appear later; wait to avoid transient scheduling errors.
    waitForLogicalLibrary();
    while (!m_stopSource.stop_requested()) {
      waitUntilDriveIsRequestedUp();
      if (m_stopSource.stop_requested()) {
        break;
      }
      auto session = DriveSession::create(m_config, m_lc.logger(), *m_schedulerContext);
      // Unpublish before destroying the session, including before an outer catch handles failure.
      const utils::ScopeExit clearActiveSession([this] {
        std::lock_guard lock(m_sessionMutex);
        m_activeDriveSession = nullptr;
      });
      {
        std::lock_guard lock(m_sessionMutex);
        m_activeDriveSession = session.get();
      }
      session->run(m_stopSource.get_token());
    }
  } catch (const Scheduler::NoSuchDrive& ex) {
    return shutdown(ExitCause::MissingDrive, DownPublication::None, ex.getMessageValue());
  } catch (const TapeSessionWorkerTeardownIncomplete& ex) {
    return shutdown(ExitCause::UnsafeWorkerTeardown, DownPublication::DesiredOnly, ex.what());
  } catch (const exception::Exception& ex) {
    return shutdown(ExitCause::UnexpectedFailure, DownPublication::DesiredAndReported, ex.getMessageValue());
  } catch (const std::exception& ex) {
    return shutdown(ExitCause::UnexpectedFailure, DownPublication::DesiredAndReported, ex.what());
  } catch (...) {
    return shutdown(ExitCause::UnexpectedFailure, DownPublication::DesiredAndReported, "Unknown exception");
  }

  return shutdown(ExitCause::Normal, DownPublication::DesiredAndReported);
}

int TapeDaemon::shutdown(ExitCause cause, DownPublication publication, std::string_view diagnostic) {
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
    case ExitCause::UnsafeWorkerTeardown:
      reason = DriveDownReason::SessionDidNotStopSafely;
      message = "Tape session did not stop safely. Requesting Down without reporting hardware release.";
      break;
  }
  {
    // Keep the original diagnostic out of subsequent publication-failure logs.
    log::ScopedParamContainer params(m_lc);
    if (!diagnostic.empty()) {
      params.add(semconv::log::exceptionMessage, std::string(diagnostic));
    }
    m_lc.log(cause == ExitCause::Normal ? log::INFO : log::ERR, message);
  }

  if (publication == DownPublication::None) {
    return exitCode;
  }

  // Preserve specific reasons. Physical cleanup and session release belong to DriveSession.
  try {
    requestDriveDown(*m_catalogue, m_driveInfo.driveName, reason, m_lc);
  } catch (...) {
    m_lc.log(log::ERR, "Failed to request desired Down before daemon exit.");
    exitCode = 1;
  }
  if (publication == DownPublication::DesiredAndReported) {
    try {
      m_schedulerContext->scheduler().reportDriveStatus(m_driveInfo,
                                                        common::dataStructures::MountType::NoMount,
                                                        common::dataStructures::DriveStatus::Down,
                                                        m_lc);
    } catch (...) {
      m_lc.log(log::ERR, "Failed to report Down before daemon exit.");
      exitCode = 1;
    }
  }
  return exitCode;
}

void TapeDaemon::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (!m_stopSource.stop_requested()) {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();
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
  auto& scheduler = m_schedulerContext->scheduler();
  bool waitingLogged = false;

  while (!m_stopSource.stop_requested()) {
    const auto desiredState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);

    if (desiredState.up) {
      if (waitingLogged) {
        m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive preparation.");
      }
      return;
    }

    if (!waitingLogged) {
      m_lc.log(log::INFO, "Waiting for the desired drive state to become up.");
      waitingLogged = true;
    }

    // Keep the catalogue timestamp fresh so a waiting drive is not shown as stale.
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::Down,
                                m_lc);
    ::sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

bool TapeDaemon::registerDrive(bool putUpIfPossible) {
  m_registered.store(false);
  auto& scheduler = m_schedulerContext->scheduler();
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!scheduler.checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    return false;
  }

  // Preserve an interrupted drive entry and its operator intent.
  const auto previous = m_catalogue->DriveState()->getTapeDrive(m_driveInfo.driveName);
  // Desired Up survives crashes and also represents an operator's pending up request.
  if (previous && previous->desiredUp) {
    // Keep the existing entry and operator intent. CleaningUp does not change desired-up.
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::CleaningUp,
                                m_lc);
    scheduler.reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
    m_lc.log(log::INFO, "Registered interrupted drive for recovery before scheduling.");
    m_registered.store(true);
    return true;
  }

  common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = scheduler.getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (const Scheduler::NoSuchDrive&) {
    m_lc.log(log::INFO, "Drive has no existing catalogue entry. Creating one.");
  }

  common::dataStructures::DesiredDriveState driveState;
  driveState.comment = currentDesiredDriveState.comment;
  // An up request may have arrived since the initial snapshot; preserve that intent too.
  if (currentDesiredDriveState.up) {
    driveState = currentDesiredDriveState;
  } else if (!currentDesiredDriveState.reason || currentDesiredDriveState.reason->empty()
             || common::dataStructures::isCleanDriveShutdownReason(*currentDesiredDriveState.reason)) {
    driveState.reason = common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
    driveState.up = putUpIfPossible;
  } else {
    driveState.reason = currentDesiredDriveState.reason;
  }

  common::dataStructures::SecurityIdentity securityIdentity;
  scheduler.createTapeDriveStatus(m_driveInfo,
                                  driveState,
                                  common::dataStructures::MountType::NoMount,
                                  common::dataStructures::DriveStatus::Down,
                                  securityIdentity,
                                  m_lc);
  scheduler.reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
  m_lc.log(log::INFO,
           "Drive registered with reported status down and desired state "
             + std::string(driveState.up ? "up." : "down."));
  m_registered.store(true);
  return true;
}

}  // namespace cta::tape::daemon
