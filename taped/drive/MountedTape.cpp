/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include "common/semconv/Attributes.hpp"
#include "common/utils/Timer.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "taped/session/TapeSessionTracker.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <cmath>
#include <opentelemetry/context/runtime_context.h>
#include <utility>

namespace cta::tape::daemon {

MountedTape::MountedTape(mediachanger::MediaChangerFacade& mediaChanger,
                         const VolumeInfo& volume,
                         drive::DriveInterface& drive,
                         catalogue::Catalogue& catalogue,
                         uint32_t tapeLoadTimeout,
                         Outcome& outcome,
                         log::LogContext& lc,
                         TapeSessionTracker& tracker)
    : m_cleaner(mediaChanger, lc.logger(), drive.info, volume.vid, true, tapeLoadTimeout, catalogue, tracker),
      m_drive(drive),
      m_outcome(outcome),
      m_lc(lc) {
  m_outcome = Outcome {};
  const bool readOnly = volume.mountType == common::dataStructures::MountType::Retrieve;
  const auto slot = mediachanger::LibrarySlotParser::parse(drive.info.rawLibrarySlot);
  try {
    log::ScopedParamContainer params(m_lc);
    params.add("drive_Slot", slot.str());
    utils::Timer timer;
    try {
      if (readOnly) {
        mediaChanger.mountTapeReadOnly(volume.vid, slot);
      } else {
        mediaChanger.mountTapeReadWrite(volume.vid, slot);
      }
    } catch (...) {
      // Record mount time before recovery, without replacing the original mount exception.
      const double mountTime = timer.secs();
      try {
        tracker.addTapeSetupStats({.initialMountTime = mountTime});
      } catch (...) {}
      try {
        try {
          throw;
        } catch (const cta::exception::Exception& ex) {
          params.add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
        } catch (const std::exception& ex) {
          params.add(cta::semconv::log::exceptionMessage, ex.what());
        } catch (...) {
          params.add(cta::semconv::log::exceptionMessage, "Non-standard exception");
        }
        m_lc.log(log::ERR,
                 readOnly ? "Failed to mount the tape for read-only access" :
                            "Failed to mount the tape for read/write access");
      } catch (...) {}
      throw;
    }
    const double mountTime = timer.secs();
    tracker.addTapeSetupStats({.initialMountTime = mountTime});
    params.add("MCMountTime", mountTime).add("mode", readOnly ? "R" : "RW");
    telemetry::metrics::ctaTapedMountDuration->Record(
      std::lround(mountTime),
      {
        {semconv::attr::kCtaIoDirection,
         readOnly ? semconv::attr::CtaIoDirectionValues::kRead : semconv::attr::CtaIoDirectionValues::kWrite}
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
    m_lc.log(log::INFO, readOnly ? "Tape mounted for read-only access" : "Tape mounted for read/write access");
  } catch (...) {
    // Construction failed, so the destructor cannot clean up a partially mounted cartridge.
    cleanup();
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
    m_outcome.result = m_cleaner.cleanDrive(m_drive);
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
