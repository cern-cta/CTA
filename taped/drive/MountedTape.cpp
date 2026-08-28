/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/ExceptionLogging.hpp"
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
                         std::optional<DriveCleaner::CleanupResult>& cleanupResult,
                         log::LogContext& lc,
                         TapeSessionTracker& tracker)
    : m_cleaner(mediaChanger, lc.logger(), drive.info, volume.vid, true, tapeLoadTimeout, catalogue, tracker),
      m_drive(drive),
      m_cleanupResult(cleanupResult),
      m_lc(lc) {
  m_cleanupResult.reset();
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
      log::logCurrentExceptionNoThrow(m_lc,
                                      readOnly ? "Failed to mount the tape for read-only access" :
                                                 "Failed to mount the tape for read/write access");
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

const DriveCleaner::CleanupResult& MountedTape::cleanup() noexcept {
  if (m_finished) {
    return *m_cleanupResult;
  }
  m_finished = true;
  m_cleanupResult.emplace();

  try {
    *m_cleanupResult = m_cleaner.cleanDrive(m_drive);
  } catch (...) {
    m_cleanupResult->unexpectedFailure = true;
    try {
      m_cleanupResult->errorMessage = cta::exception::currentExceptionMessage();
    } catch (...) {}
  }
  if (m_cleanupResult->driveReusable()) {
    return *m_cleanupResult;
  }

  // Logging must not interrupt stack unwinding.
  try {
    log::ScopedParamContainer params(m_lc);
    params.add("errorMessage", m_cleanupResult->errorMessage);
    m_lc.log(log::ERR, "Mounted tape cleanup failed");
  } catch (...) {}
  return *m_cleanupResult;
}

}  // namespace cta::tape::daemon
