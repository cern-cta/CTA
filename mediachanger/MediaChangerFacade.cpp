/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/MediaChangerFacade.hpp"

#include "common/exception/Exception.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <toml++/toml.hpp>
#include <unistd.h>

namespace cta::mediachanger {

namespace {

// RAII guard that acquires the single-changer robot lock on construction and
// releases it on destruction.  The lock is a file created with O_CREAT|O_EXCL;
// only one caller holds it at a time.  Callers spin-wait with 50 ms backoff.
// timeoutMs is read from library.conf (library changerTimeoutMs) and defaults
// to 30 s if not present.
struct ChangerGuard {
  const std::filesystem::path lockPath;

  ChangerGuard(const std::filesystem::path& changerDir, int timeoutMs) : lockPath(changerDir / ".lock") {
    constexpr int kRetryMs = 50;

    for (int waited = 0;; waited += kRetryMs) {
      // O_CREAT|O_EXCL is atomic on POSIX: exactly one caller wins.
      int fd = ::open(lockPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
      if (fd >= 0) {
        ::close(fd);
        return;
      }
      if (errno != EEXIST) {
        throw cta::exception::Exception("StressMediaChanger: cannot acquire changer lock at " + lockPath.string() + ": "
                                        + std::strerror(errno));
      }
      if (waited >= timeoutMs) {
        throw cta::exception::Exception("StressMediaChanger: timeout (" + std::to_string(timeoutMs / 1000)
                                        + " s) waiting for changer lock at " + lockPath.string()
                                        + " — another mount/dismount is stuck or a previous run left a stale lock");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(kRetryMs));
    }
  }

  ~ChangerGuard() noexcept {
    try {
      std::filesystem::remove(lockPath);
    } catch (...) {}
  }

  ChangerGuard(const ChangerGuard&) = delete;
  ChangerGuard& operator=(const ChangerGuard&) = delete;
};

}  // namespace

//------------------------------------------------------------------------------
// constructor (normal mode)
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger& log)
    : m_stressMode(false),
      m_rmcProxy(rmcProxy),
      m_dmcProxy(log) {}

//------------------------------------------------------------------------------
// constructor (stress mode)
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(std::string driveName,
                                       std::string logicalLibraryName,
                                       std::filesystem::path stressBaseDir,
                                       log::Logger& log)
    : m_stressMode(true),
      m_stressDriveName(std::move(driveName)),
      m_stressLogicalLibrary(std::move(logicalLibraryName)),
      m_stressBaseDir(std::move(stressBaseDir)),
      m_dmcProxy(log) {
  // Ensure per-drive and changer directories exist.
  const auto contentsBase = m_stressBaseDir / "contents" / m_stressLogicalLibrary;
  std::filesystem::create_directories(contentsBase / "drives" / m_stressDriveName);
  std::filesystem::create_directories(contentsBase / "changer");

  // Read changerTimeoutMs from library.toml; fall back to 30 s if absent.
  const auto confPath = m_stressBaseDir / "config" / m_stressLogicalLibrary / "library.toml";
  if (std::filesystem::exists(confPath)) {
    const auto tbl = toml::parse_file(confPath.string());
    if (auto v = tbl["library"]["changerTimeoutMs"].value<int>()) {
      m_changerTimeoutMs = *v;
    }
  }
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot) {
  if (m_stressMode) {
    stressMountTape(vid, false);
    return;
  }
  try {
    return getProxy(slot).mountTapeReadOnly(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read-only access: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot) {
  if (m_stressMode) {
    stressMountTape(vid, true);
    return;
  }
  try {
    return getProxy(slot).mountTapeReadWrite(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read/write access: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::dismountTape(const std::string& vid, const LibrarySlot& slot) {
  if (m_stressMode) {
    stressDismountTape(vid);
    return;
  }
  try {
    return getProxy(slot).dismountTape(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProxy
//------------------------------------------------------------------------------
MediaChangerProxy& MediaChangerFacade::getProxy(const LibrarySlot& slot) {
  if (slot.isDummy()) {
    return m_dmcProxy;
  }
  return m_rmcProxy;
}

//------------------------------------------------------------------------------
// stressMountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::stressMountTape(const std::string& vid, bool readWrite) {
  const auto contentsBase = m_stressBaseDir / "contents" / m_stressLogicalLibrary;
  const auto slotPath = contentsBase / "slots" / (vid + ".tape");
  const auto changerDir = contentsBase / "changer";
  const auto changerTape = changerDir / (vid + ".tape");
  const auto drivePath = contentsBase / "drives" / m_stressDriveName / (vid + ".tape");

  if (!std::filesystem::exists(slotPath)) {
    if (readWrite) {
      // First archive on this VID: create an empty tape file in the slot.
      std::ofstream(slotPath, std::ios::binary).close();
    } else {
      throw cta::exception::Exception("StressMediaChanger: tape not found for retrieve: VID=" + vid + " expected at "
                                      + slotPath.string() + " — has this tape been archived yet?");
    }
  }

  // Acquire the single changer robot.  Only one tape may be in transit at a
  // time (slots/ → changer/ → drives/<drv>/), matching production behaviour
  // where a single robot arm serialises all mount requests.
  {
    const ChangerGuard guard(changerDir, m_changerTimeoutMs);
    std::filesystem::rename(slotPath, changerTape);   // slots/  → changer/
    std::filesystem::rename(changerTape, drivePath);  // changer/ → drives/<drv>/
  }
}

//------------------------------------------------------------------------------
// stressDismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::stressDismountTape(const std::string& vid) {
  const auto contentsBase = m_stressBaseDir / "contents" / m_stressLogicalLibrary;
  const auto drivePath = contentsBase / "drives" / m_stressDriveName / (vid + ".tape");
  const auto changerDir = contentsBase / "changer";
  const auto changerTape = changerDir / (vid + ".tape");
  const auto slotPath = contentsBase / "slots" / (vid + ".tape");

  // Best-effort: if the tape has already disappeared from the drive directory
  // (e.g. process crashed after StressDrive::unloadTape was called), skip.
  if (!std::filesystem::exists(drivePath)) {
    return;
  }

  // Route the tape back through the changer robot.
  {
    const ChangerGuard guard(changerDir, m_changerTimeoutMs);
    std::filesystem::rename(drivePath, changerTape);  // drives/<drv>/ → changer/
    std::filesystem::rename(changerTape, slotPath);   // changer/ → slots/
  }
}

}  // namespace cta::mediachanger
