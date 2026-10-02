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
    throw std::logic_error("Cannot reacquire ended drive-session ownership");
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

void DriveReservation::release() {
  if (m_state == State::Owned) {
    // Release a future SCSI reservation here.
    // Keep Owned if physical release fails so destructor cleanup can retry.
  }
  m_state = State::Released;
}

}  // namespace cta::tape::daemon
