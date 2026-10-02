/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveReservation.hpp"

#include "common/log/ExceptionLogging.hpp"
#include "common/log/LogContext.hpp"

#include <stdexcept>

namespace cta::tape::daemon {

DriveReservation::DriveReservation(log::LogContext& lc) noexcept : m_lc(lc) {}

DriveReservation::~DriveReservation() noexcept {
  try {
    release();
  } catch (...) {
    log::logCurrentExceptionNoThrow(m_lc, "Failed to release drive hardware ownership.");
  }
}

void DriveReservation::acquire() {
  if (m_state != State::Unacquired && m_state != State::Owned) {
    throw std::logic_error("Cannot reacquire ended or unsafe drive-session ownership");
  }

  if (m_state == State::Unacquired) {
    // Future SCSI reservation acquisition belongs here, before any drive cleanup.
    // Its acquisition must roll back partial failure; recovery must retain the existing reservation.
    m_state = State::Owned;
  }
}

bool DriveReservation::canUseHardware() const noexcept {
  return m_state == State::Owned;
}

bool DriveReservation::release() {
  if (m_state == State::Unsafe) {
    // Both enclosing-session teardown and this destructor can attempt release.
    if (!m_unsafeReleaseLogged) {
      m_unsafeReleaseLogged = true;
      try {
        m_lc.log(log::WARNING,
                 "Refusing to release unsafe drive hardware ownership; workers may still access the drive.");
      } catch (...) {}
    }
    return false;
  }
  if (m_state == State::Owned) {
    // Release a future SCSI reservation here.
    // Keep Owned if physical release fails so destructor cleanup can retry.
  }
  m_state = State::Released;
  return true;
}

void DriveReservation::markUnsafe() noexcept {
  // Worker RAII and stop semantics must establish termination before a future reservation can be released.
  m_state = State::Unsafe;
}

}  // namespace cta::tape::daemon
