/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "HardwareOwnership.hpp"

#include "SchedulerContext.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/log/LogContext.hpp"

#include <stdexcept>

namespace cta::tape::daemon {

HardwareOwnership::HardwareOwnership(const common::dataStructures::DriveInfo& driveInfo,
                                     log::LogContext& lc,
                                     SchedulerContext& schedulerContext) noexcept
    : m_driveInfo(driveInfo),
      m_lc(lc),
      m_schedulerContext(schedulerContext) {}

HardwareOwnership::~HardwareOwnership() noexcept {
  try {
    release();
  } catch (...) {
    // Fallback teardown must not replace the original exception, even if logging fails.
    try {
      m_lc.log(log::ERR, "Failed to release drive ownership or publish Down.");
    } catch (...) {}
  }
}

void HardwareOwnership::acquire() {
  if (m_state != State::Unacquired && m_state != State::Owned) {
    throw std::logic_error("Cannot reacquire ended or unsafe drive-session ownership");
  }

  // Report cleanup before hardware access; reported Down permits operator access.
  m_schedulerContext.scheduler().reportDriveStatus(m_driveInfo,
                                                   common::dataStructures::MountType::NoMount,
                                                   common::dataStructures::DriveStatus::CleaningUp,
                                                   m_lc);
  if (m_state == State::Unacquired) {
    // Future SCSI reservation acquisition belongs here, before any drive cleanup.
    // Its acquisition must roll back partial failure; recovery must retain the existing reservation.
    m_state = State::Owned;
  }
}

bool HardwareOwnership::ownsHardware() const noexcept {
  return m_state == State::Owned;
}

void HardwareOwnership::release() {
  if (m_state == State::Released || m_state == State::Unsafe) {
    return;
  }
  if (m_state == State::Owned) {
    // Release a future SCSI reservation here, before reporting Down.
    // Keep Owned if physical release fails so destructor cleanup can retry.
    m_state = State::ReleasePending;
  } else if (m_state == State::Unacquired) {
    // Preparation may decline before acquisition; still finish its Down publication.
    m_state = State::ReleasePending;
  }

  // Retry publication without repeating physical release; retirement may have replaced the scheduler.
  auto& scheduler = m_schedulerContext.scheduler();
  if (scheduler.getCatalogue().DriveState()->getTapeDrive(m_driveInfo.driveName)) {
    scheduler.reportDriveStatus(m_driveInfo,
                                common::dataStructures::MountType::NoMount,
                                common::dataStructures::DriveStatus::Down,
                                m_lc);
  }
  m_state = State::Released;
}

void HardwareOwnership::markUnsafe() noexcept {
  // Worker RAII and stop semantics must establish termination before a future reservation can be released.
  m_state = State::Unsafe;
}

}  // namespace cta::tape::daemon
