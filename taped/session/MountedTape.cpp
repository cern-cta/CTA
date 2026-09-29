/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include <stdexcept>
#include <utility>

namespace cta::tape::daemon {

MountedTape::MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
                         const std::string& vid,
                         const mediachanger::LibrarySlot& slot,
                         AccessMode accessMode,
                         DriveCleaner& cleaner,
                         drive::DriveInterface& drive,
                         DriveCleaner::DriveStatusReporter reportStatus,
                         Outcome& outcome,
                         log::LogContext& lc)
    : MountedTape(
        [&] {
          if (accessMode == AccessMode::ReadOnly) {
            mediaChanger.mountTapeReadOnly(vid, slot);
          } else {
            mediaChanger.mountTapeReadWrite(vid, slot);
          }
        },
        [&cleaner, &drive, reportStatus = std::move(reportStatus)] { return cleaner.cleanDrive(drive, reportStatus); },
        outcome,
        lc) {}

MountedTape::MountedTape(Mount mount, Cleanup cleanup, Outcome& outcome, log::LogContext& lc)
    : m_cleanup(std::move(cleanup)),
      m_outcome(outcome),
      m_lc(lc) {
  if (!mount || !m_cleanup) {
    throw std::invalid_argument("MountedTape requires mount and cleanup operations");
  }
  m_outcome = Outcome {};
  try {
    mount();
  } catch (...) {
    // Construction failed, so the destructor cannot clean up a partially mounted cartridge.
    this->cleanup();
    throw;
  }
}

MountedTape::~MountedTape() noexcept {
  cleanup();
}

const MountedTape::Outcome& MountedTape::cleanup() noexcept {
  if (m_finished) {
    return m_outcome;
  }
  m_finished = true;

  try {
    m_outcome.result = m_cleanup();
  } catch (...) {
    m_outcome.exception = std::current_exception();
  }
  if (m_outcome.driveReusable()) {
    return m_outcome;
  }

  // Logging must not interrupt stack unwinding.
  try {
    log::ScopedParamContainer params(m_lc);
    if (m_outcome.exception) {
      try {
        std::rethrow_exception(m_outcome.exception);
      } catch (const std::exception& ex) {
        params.add("exceptionMessage", ex.what());
      } catch (...) {
        params.add("exceptionMessage", "Non-standard exception");
      }
    } else {
      params.add("errorMessage", m_outcome.result->errorMessage);
    }
    m_lc.log(log::ERR, "Mounted tape cleanup failed");
  } catch (...) {}
  return m_outcome;
}

}  // namespace cta::tape::daemon
