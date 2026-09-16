/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveHandler.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TimeoutException.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "session/CleanerSession.hpp"
#include "session/DataTransferSession.hpp"
#include "session/EmptyDriveProbe.hpp"

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace cta::tape::daemon {

namespace {
void logDriveFailure(log::LogContext& lc, const char* message, const std::exception& ex) {
  log::ScopedParamContainer params(lc);
  if (const auto* ctaException = dynamic_cast<const exception::Exception*>(&ex)) {
    params.add(semconv::log::exceptionMessage, ctaException->getMessageValue());
  } else {
    params.add(semconv::log::exceptionMessage, ex.what());
  }
  lc.log(log::ERR, message);
}
}  // namespace

DriveHandler::DriveHandler(const TapedConfig& config, log::Logger& log, InitializeMembers)
    : m_config(config),
      m_driveInfo(m_config.drive.name,
                  utils::getShortHostname(),
                  m_config.drive.logical_library_name,
                  m_config.drive.device,
                  m_config.drive.control_path),
      m_lc(log),
      m_mediaChanger(mediachanger::RmcProxy(m_config.rmcd.host,
                                            m_config.rmcd.port,
                                            m_config.rmcd.request_timeout_secs,
                                            m_config.rmcd.request_attempts),
                     log) {}

DriveHandler::DriveHandler(const TapedConfig& config, log::Logger& log, IScheduler& scheduler, Operations operations)
    : DriveHandler(config, log, InitializeMembers {}) {
  if (!operations.logicalLibraryExists || !operations.probeDrive || !operations.getNextMount || !operations.transfer
      || !operations.clean || !operations.sleep) {
    throw std::invalid_argument("DriveHandler requires all injected operations");
  }
  m_driveScheduler = &scheduler;
  m_operations = std::move(operations);
}

DriveHandler::DriveHandler(const TapedConfig& config, log::Logger& log)
    : DriveHandler(config, log, InitializeMembers {}) {
  m_lc.log(log::INFO, "Initialising Catalogue");
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(m_config.catalogue.config_file);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);
  m_catalogue = catalogueFactory->create();

  m_lc.log(log::INFO, "Catalogue initialised successfully");
  m_lc.log(log::INFO, "Initialising Scheduler");
#ifndef CTA_PGSCHED
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                      utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                      m_lc.logger());
#else
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                      utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                      m_config.scheduler.number_of_connections,
                                                      m_lc.logger());
#endif
  m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = m_config.scheduler.tape_cache_max_age_secs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_config.scheduler.retrieve_queue_cache_max_age_secs;
  m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);
  m_scheduler = std::make_unique<Scheduler>(*m_catalogue,
                                            *m_schedDb,
                                            m_config.scheduler.backend_name,
                                            m_config.mounts.minimum_queued_files,
                                            m_config.mounts.minimum_queued_bytes);

  m_driveScheduler = m_scheduler.get();
  m_lc.log(log::INFO, "Scheduler initialised successfully");
}

void DriveHandler::stop() {
  m_stopSource.request_stop();
}

void DriveHandler::waitForLogicalLibrary() {
  bool waitingLogged = false;
  while (true) {
    const bool exists = m_operations.logicalLibraryExists ? m_operations.logicalLibraryExists() : [this] {
      const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();
      return std::any_of(libraries.begin(), libraries.end(), [this](const auto& library) {
        return library.name == m_driveInfo.logicalLibrary;
      });
    }();

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
    // TODO (separate MR): make this startup wait interruptible by graceful shutdown.
    sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

common::dataStructures::DesiredDriveState DriveHandler::getDesiredDriveState() {
  try {
    return m_driveScheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (const Scheduler::NoSuchDrive&) {
    m_lc.log(log::WARNING, "Drive is missing from the catalogue. Attempting to register it as down.");
    if (!registerDrive(false)) {
      throw exception::Exception("In DriveHandler::getDesiredDriveState(): failed to register the missing drive");
    }
    m_lc.log(log::INFO, "Missing drive registered as down. Waiting for an operator up request.");
    // Registration requested down; read the operator's state again on the next polling iteration.
    return {};
  }
}

void DriveHandler::waitForDriveToBeUp() {
  m_lc.log(log::INFO, "Waiting for the desired drive state to become up.");
  // TODO: graceful shutdown (separate MR)
  while (true) {
    const auto desiredState = getDesiredDriveState();

    if (desiredState.up) {
      m_lc.log(log::INFO, "Desired drive state is up. Proceeding with drive probing.");
      return;
    }

    m_lc.log(log::DEBUG, "Desired drive state is down. Refreshing the reported down status.");
    // Keep the catalogue timestamp fresh so a waiting drive is not shown as stale.
    // Reporting failures propagate to the caller.
    m_driveScheduler->reportDriveStatus(m_driveInfo,
                                        common::dataStructures::MountType::NoMount,
                                        common::dataStructures::DriveStatus::Down,
                                        m_lc);

    // TODO: Ensure graceful shutdown can interrupt this sleep
    sleep(m_config.mounts.drive_state_poll_interval_secs);
  }
}

void DriveHandler::putDriveDown(common::dataStructures::DriveDownReason reason,
                                std::string_view detail,
                                bool preserveExistingReason) {
  common::dataStructures::DesiredDriveState driveState;
  driveState.reason = common::dataStructures::formatDriveDownReason(reason, detail);
  std::exception_ptr firstFailure;
  const auto recordFailure = [&](const char* message, const std::exception& ex) {
    if (!firstFailure) {
      firstFailure = std::current_exception();
    }
    logDriveFailure(m_lc, message, ex);
  };

  if (preserveExistingReason) {
    try {
      const auto currentState = m_driveScheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
      const auto startupReason =
        common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
      if (currentState.reason && !currentState.reason->empty() && *currentState.reason != startupReason
          && !common::dataStructures::isCleanDriveShutdownReason(*currentState.reason)) {
        // Leave the catalogue reason untouched, including operator and session failure reasons.
        driveState.reason.reset();
      }
    } catch (const std::exception& ex) {
      driveState.reason.reset();
      recordFailure("Failed to read the existing drive-down reason.", ex);
    }
  }

  if (driveState.reason) {
    m_lc.logEvent(common::dataStructures::driveDownReasonSeverity(reason),
                  *driveState.reason,
                  semconv::log::EventNameValues::kPuttingTapeDriveDown);
  }

  // Failure of one publication must not prevent attempting the other.
  try {
    m_driveScheduler->reportDriveStatus(m_driveInfo,
                                        common::dataStructures::MountType::NoMount,
                                        common::dataStructures::DriveStatus::Down,
                                        m_lc);
  } catch (const std::exception& ex) {
    recordFailure("Failed to publish the reported down status.", ex);
  }

  try {
    m_driveScheduler->setDesiredDriveState(m_driveInfo.driveName, driveState, m_lc);
  } catch (const std::exception& ex) {
    recordFailure("Failed to publish the desired down state.", ex);
  }

  if (firstFailure) {
    std::rethrow_exception(firstFailure);
  }
}

std::unique_ptr<TapeMount> DriveHandler::getNextMount() {
  if (m_operations.getNextMount) {
    return m_operations.getNextMount();
  }
  // TODO: add timeout?
  if (m_scheduler->getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
    return m_scheduler->getNextMount(m_driveInfo.logicalLibrary,
                                     m_driveInfo.driveName,
                                     m_lc,
                                     static_cast<uint64_t>(m_config.mounts.get_next_mount_timeout_secs) * 1000000);
  }
  return nullptr;
}

bool DriveHandler::registerDrive(bool putUpIfPossible) {
  m_lc.log(log::INFO, "Registering the drive in the catalogue.");
  if (!m_driveScheduler->checkDriveCanBeCreated(m_driveInfo, m_lc)) {
    m_lc.log(log::CRIT, "Cannot register the drive: its name belongs to a different host or logical library.");
    return false;
  }

  common::dataStructures::DesiredDriveState currentDesiredDriveState;
  try {
    currentDesiredDriveState = m_driveScheduler->getDesiredDriveState(m_driveInfo.driveName, m_lc);
  } catch (const Scheduler::NoSuchDrive&) {
    m_lc.log(log::INFO, "Drive has no existing catalogue entry. Creating one.");
  }

  common::dataStructures::DesiredDriveState driveState;
  driveState.comment = currentDesiredDriveState.comment;
  // Replace absent or clean-exit reasons with the startup reason. Preserve other reasons for being down.
  if (!currentDesiredDriveState.reason
      || common::dataStructures::isCleanDriveShutdownReason(*currentDesiredDriveState.reason)) {
    driveState.reason = common::dataStructures::formatDriveDownReason(common::dataStructures::DriveDownReason::Startup);
    driveState.up = putUpIfPossible;
  } else {
    driveState.reason = currentDesiredDriveState.reason;
  }

  common::dataStructures::SecurityIdentity securityIdentity;
  m_driveScheduler->createTapeDriveStatus(m_driveInfo,
                                          driveState,
                                          common::dataStructures::MountType::NoMount,
                                          common::dataStructures::DriveStatus::Down,
                                          securityIdentity,
                                          m_lc);
  m_driveScheduler->reportSchedulerBackendName(m_driveInfo.driveName, m_lc);
  m_lc.log(log::INFO,
           "Drive registered with reported status down and desired state "
             + std::string(driveState.up ? "up." : "down."));
  return true;
}

TransferSessionResult DriveHandler::executeDataTransferSession(TapeMount& tapeMount) {
  if (m_operations.transfer) {
    return m_operations.transfer(tapeMount);
  }
  DataTransferSession dataTransferSession(
    m_lc.logger(),
    m_sysWrapper,
    m_driveInfo,
    m_mediaChanger,
    tapeMount,
    m_tapeSessionTracker,
    m_config.transfers,
    m_config.mounts.tape_load_timeout_secs,  // TODO: DataTransferSession should not be responsible for tape loading
    *m_scheduler);
  return dataTransferSession.execute();
}

bool DriveHandler::executeCleanerSession(const std::optional<std::string>& vid, bool waitMediaInDrive) {
  if (m_operations.clean) {
    return m_operations.clean();
  }
  CleanerSession cleanerSession(m_mediaChanger,
                                m_lc.logger(),
                                m_driveInfo,
                                m_sysWrapper,
                                vid.value_or(""),
                                waitMediaInDrive,
                                m_config.mounts.tape_load_timeout_secs,
                                *m_catalogue,
                                *m_scheduler,
                                &m_tapeSessionTracker);

  return cleanerSession.execute() == DriveUsability::Reusable;
}

bool DriveHandler::isLive() const {
  // TODO: look into the timeouts and see if we have spent too much time in any given state
  // We don't ping the catalogue/scheduler here as that would just result in cascading failures
  // A restart won't fix things
  return true;
}

bool DriveHandler::isReady() const {
  // TODO ping catalogue and scheduler
  return true;
}

bool DriveHandler::prepareDriveForScheduling() {
  // Honour the operator's desired state before scheduling another mount.
  // TODO: handle desired-state lookup and status-publication failures at this phase boundary.
  if (!getDesiredDriveState().up) {
    // Wait for an up request; the helper re-registers a missing drive as down.
    waitForDriveToBeUp();
  }

  // Verify the drive is empty before every scheduling attempt, including after transfer exceptions.
  m_driveScheduler->reportDriveStatus(m_driveInfo,
                                      common::dataStructures::MountType::NoMount,
                                      common::dataStructures::DriveStatus::Probing,
                                      m_lc);
  EmptyDriveProbe emptyDriveProbe(m_lc.logger(), m_driveInfo, m_sysWrapper);
  m_lc.log(log::DEBUG, "Checking whether the drive is empty before scheduling.");

  // Automatic recovery cleaning is currently disabled.
  // TODO (separate MR): configure automatic cleaning before probing.
  if (false) {
    // TODO: check the cleaner result and handle escaping exceptions before proceeding to the probe.
    executeCleanerSession();
  }

  // A non-empty or failed probe prevents scheduling and requires another operator up request.
  const auto [empty, probeError] = m_operations.probeDrive ? m_operations.probeDrive() : [&] {
    const bool empty = emptyDriveProbe.driveIsEmpty();
    return std::make_pair(empty, emptyDriveProbe.getProbeErrorMsg());
  }();
  if (!empty) {
    m_lc.log(log::WARNING, "Drive probe did not confirm an empty drive. Requesting the drive down.");
    putDriveDown(probeError ? common::dataStructures::DriveDownReason::DriveProbeFailed :
                              common::dataStructures::DriveDownReason::TapeDetected,
                 probeError.value_or(""));
    return false;
  }
  m_lc.log(log::DEBUG, "No tape detected in the drive. Proceeding with scheduling.");

  // Advertise an idle drive with no active mount before asking the scheduler for work.
  m_driveScheduler->reportDriveStatus(m_driveInfo,
                                      common::dataStructures::MountType::NoMount,
                                      common::dataStructures::DriveStatus::Up,
                                      m_lc);
  // TODO: rip out session reporting
  // tapeSessionReporter.reportState(::session::SessionState::Scheduling,
  //                                 ::session::SessionType::Undetermined);

  return true;
}

int DriveHandler::shutdownDrive() {
  // Use an unknown VID; final cleanup does not depend on a surviving transfer mount.
  int exitCode = 0;
  bool cleaningSucceeded = false;

  // Cleanup failure must not prevent attempting to publish the down state.
  try {
    cleaningSucceeded = executeCleanerSession();
    if (!cleaningSucceeded) {
      m_lc.log(log::ERR, "Final drive cleaning failed.");
      exitCode = 1;
    }
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, "Final drive cleaning threw an exception.", ex);
    exitCode = 1;
  }

  // Preserve existing reasons; publish a clean shutdown only when cleaning succeeded.
  try {
    putDriveDown(cleaningSucceeded ? common::dataStructures::DriveDownReason::Shutdown :
                                     common::dataStructures::DriveDownReason::CleanerFailed,
                 {},
                 true);
  } catch (const std::exception&) {
    // The helper has logged each failure and attempted both down-state publications.
    exitCode = 1;
  }

  return exitCode;
}

int DriveHandler::run() {
  // TODO: handle startup exceptions separately from failures during an active session.
  // TODO: define bounded database recovery for each phase; helpers propagate operational failures.
  // TODO: wait for catalogue and scheduler to be reachable within a reasonable timeout?

  // Register the drive. It will put the drive down with a startup reason.
  // If the drive was already down with an existing reason, this reason will be carried over.
  // The only exception is if the down reason is a previous clean exit.
  // Registration conflicts prevent taped from using the drive.
  // TODO (separate MR): configure whether startup may request the drive to be up.
  if (!registerDrive(false)) {
    return 1;
  }

  // A drive may be defined with a logical library that does not exist (yet)
  // In which case scheduling will not work
  // The scheduler itself already handles this okay, but this is just to avoid spitting out a bunch
  // of repeated warnings.
  waitForLogicalLibrary();

  // TODO (separate MR): stop scheduling on shutdown and reach the final cleanup below.
  while (true) {
    runIteration();
  }

  // Loop-local mount ownership and the borrowed tracker reference are released before final cleaning.
  return shutdownDrive();
}

void DriveHandler::sleep(unsigned int seconds) {
  if (m_operations.sleep) {
    m_operations.sleep(seconds);
  } else {
    ::sleep(seconds);
  }
}

void DriveHandler::runIteration() {
  if (!prepareDriveForScheduling()) {
    return;
  }

  // Retain mount ownership until the transfer and its reporting threads have finished.
  std::unique_ptr<TapeMount> tapeMount;

  // Clear the borrowed tracker reference before destroying the mount, including during unwinding.
  struct MountReferenceReset {
    TapeSessionTracker& tracker;

    ~MountReferenceReset() { tracker.setMount(nullptr); }
  } mountReferenceReset {m_tapeSessionTracker};

  // Acquire work; a scheduling timeout is recoverable by waiting and trying again.
  // TODO: handle other scheduling failures separately from transfer failures.
  utils::Timer t;
  bool schedulingTimedOut = false;
  try {
    tapeMount = getNextMount();
  } catch (const exception::TimeoutException& ex) {
    schedulingTimedOut = true;
    log::ScopedParamContainer params(m_lc);
    params.add("totalScheduleMountTime", t.secs())
      .add("scheduleMountTimeoutSecs", m_config.mounts.get_next_mount_timeout_secs)
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::WARNING, "Scheduling timed out; waiting before retrying.");
  }

  if (tapeMount == nullptr) {
    if (!schedulingTimedOut) {
      m_lc.log(log::DEBUG, "No mount available; waiting before retrying.");
    }
    // TODO: make idle waiting responsive to desired-state changes and graceful shutdown.
    sleep(m_config.mounts.idle_scheduling_interval_secs);
    // Recheck desired state before attempting scheduling again.
    return;
  }

  m_tapeSessionTracker.setMount(tapeMount.get());

  if (m_tapeSessionTracker.mount() != tapeMount.get()) {
    throw exception::Exception("In DriveHandler::run(): tracker does not reference the supplied tape mount");
  }

  // The session result describes hardware usability, not whether every file transferred successfully.
  // TODO: ensure DataTransferSession stops and joins workers/reporters before exceptions escape.
  // TODO: expose the hardware cleanup outcome when a transfer exits exceptionally.
  TransferSessionResult transferResult;
  try {
    // The transfer session handles mounting and cleaning as this is intertwined with its internal logic.
    // For example, an empty mount is discovered inside the session and does not load the tape.
    transferResult = executeDataTransferSession(*tapeMount);
  } catch (const exception::LostDatabaseConnection&) {
    // Database recovery remains separate from software/job failure handling.
    throw;
  } catch (const std::exception& ex) {
    logDriveFailure(m_lc, "Data transfer session threw an exception. Waiting before retrying scheduling.", ex);
    // TODO: make retry waiting interruptible by graceful shutdown.
    sleep(m_config.mounts.idle_scheduling_interval_secs);
    // The next preparation probes for retained media before scheduling another mount.
    return;
  }

  if (transferResult.driveUsability != DriveUsability::Reusable) {
    // Preserve specific session or operator reasons. Publication failures propagate.
    putDriveDown(common::dataStructures::DriveDownReason::TransferSessionFailed, {}, true);
    // Require another operator up request before attempting recovery and scheduling.
  }
  // TODO (separate MR): move transfer cleaning to CleanerSession, called after every transfer here.
}

}  // namespace cta::tape::daemon
