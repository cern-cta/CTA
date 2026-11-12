/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/MediaChangerFacade.hpp"

#include "common/exception/Exception.hpp"
#include "mediachanger/DmcProxy.hpp"
#include "mediachanger/MediaChangerProxy.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <syncstream>
#include <system_error>
#include <thread>
#include <toml++/toml.hpp>
#include <unistd.h>

namespace cta::mediachanger {

class MediaChangerFacadeImpl {
public:
  virtual void mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot) = 0;
  virtual void mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot) = 0;
  virtual void dismountTape(const std::string& vid, const LibrarySlot& slot) = 0;
  virtual ~MediaChangerFacadeImpl() = default;
};

}  // namespace cta::mediachanger

namespace {

// RAII guard for the single-changer robot lock (changer/.lock).
// Lock file is created with O_CREAT|O_EXCL (atomic on POSIX).
// Callers spin-wait with 50 ms backoff. A lock file untouched for >= timeoutMs (the library's
// configured changerTimeoutMs, default 30 s — sized for a near-instant rename, so anything still
// holding it that long is not a legitimate mount/dismount in progress) is treated as abandoned by
// a crashed/OOM-killed holder and stolen rather than failed on forever: this RAII guard only ever
// renames the tape file it was given, never writes shared content, so stealing a lock a holder
// merely slow (not actually dead) is still safe — it just reopens one window of contention on the
// rename, not a data-integrity problem. A separate, larger absolute ceiling still gives up for
// real if repeated steals do not clear it (e.g. a genuine bug re-creating the lock faster than
// this can recover).
struct ChangerGuard {
  const std::filesystem::path lockPath;

  ChangerGuard(const std::filesystem::path& changerDir, int timeoutMs) : lockPath(changerDir / ".lock") {
    constexpr auto kRetry = std::chrono::milliseconds(50);
    const auto staleAfter = std::chrono::milliseconds(timeoutMs);
    const auto giveUpAfter = staleAfter * 10;

    const auto waitStart = std::chrono::steady_clock::now();
    for (;;) {
      int fd = ::open(lockPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
      if (fd >= 0) {
        ::close(fd);
        return;
      }
      if (errno != EEXIST) {
        throw cta::exception::Exception("StressMediaChanger: cannot acquire changer lock at " + lockPath.string() + ": "
                                        + std::strerror(errno));
      }
      if (std::chrono::steady_clock::now() - waitStart >= giveUpAfter) {
        throw cta::exception::Exception(
          "StressMediaChanger: giving up after "
          + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(giveUpAfter).count())
          + " s waiting for changer lock at " + lockPath.string()
          + " — repeated steal attempts did not clear it, something beyond a crashed holder is wrong");
      }
      std::error_code ec;
      const auto mtime = std::filesystem::last_write_time(lockPath, ec);
      if (!ec && std::filesystem::file_time_type::clock::now() - mtime >= staleAfter) {
        std::osyncstream(std::cerr) << "WARNING cta-taped StressMediaChanger stealing stale changer lock at "
                                    << lockPath.string() << " (untouched for >= " << timeoutMs
                                    << " ms — holder likely crashed or was OOM-killed)\n";
        std::filesystem::remove(lockPath, ec);  // ignore failure: another waiter may already have removed it
      }
      std::this_thread::sleep_for(kRetry);
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

// Production implementation: routes between RmcProxy (SCSI changer daemon) and
// DmcProxy (operator console prompt) based on the library slot type.
struct HardwareChangerImpl final : cta::mediachanger::MediaChangerFacadeImpl {
  HardwareChangerImpl(const cta::mediachanger::RmcProxy& rmcProxy, cta::log::Logger& log)
      : m_rmcProxy(rmcProxy),
        m_dmcProxy(log) {}

  void mountTapeReadOnly(const std::string& vid, const cta::mediachanger::LibrarySlot& slot) override {
    try {
      getProxy(slot).mountTapeReadOnly(vid, slot);
    } catch (cta::exception::Exception& ne) {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to mount tape for read-only access: vid=" << vid << " slot=" << slot.str() << ": "
                      << ne.getMessage().str();
      throw ex;
    }
  }

  void mountTapeReadWrite(const std::string& vid, const cta::mediachanger::LibrarySlot& slot) override {
    try {
      getProxy(slot).mountTapeReadWrite(vid, slot);
    } catch (cta::exception::Exception& ne) {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to mount tape for read/write access: vid=" << vid << " slot=" << slot.str() << ": "
                      << ne.getMessage().str();
      throw ex;
    }
  }

  void dismountTape(const std::string& vid, const cta::mediachanger::LibrarySlot& slot) override {
    try {
      getProxy(slot).dismountTape(vid, slot);
    } catch (cta::exception::Exception& ne) {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": "
                      << ne.getMessage().str();
      throw ex;
    }
  }

private:
  cta::mediachanger::MediaChangerProxy& getProxy(const cta::mediachanger::LibrarySlot& slot) {
    if (slot.isDummy()) {
      return m_dmcProxy;
    }
    return m_rmcProxy;
  }

  cta::mediachanger::RmcProxy m_rmcProxy;
  cta::mediachanger::DmcProxy m_dmcProxy;
};

// Virtual-drive implementation: simulates a single-robot-arm tape changer via
// filesystem renames on a shared volume. Used for stress/scale testing without
// real tape hardware.
//
// Directory layout (all under baseDir):
//   config/<lib>/library.toml            — library/mediatype configuration
//   contents/<lib>/slots/VID.tape        — tape at rest in a slot
//   contents/<lib>/changer/VID.tape      — tape in transit (robot holds it)
//   contents/<lib>/changer/.lock         — robot-busy flag (O_CREAT|O_EXCL)
//   contents/<lib>/drives/<drv>/         — empty directory = no tape loaded
//   contents/<lib>/drives/<drv>/VID.tape — tape mounted in this drive
//
// Mount path:    slots/ → changer/ → drives/<drv>/
// Dismount path: drives/<drv>/ → changer/ → slots/
//
// Only one tape is ever in changer/ at a time; concurrent calls spin-wait on
// .lock with a configurable timeout before throwing.
struct VirtualChangerImpl final : cta::mediachanger::MediaChangerFacadeImpl {
  VirtualChangerImpl(std::string driveName,
                     std::string logicalLibrary,
                     std::filesystem::path baseDir,
                     cta::log::Logger& /*log*/)
      : m_driveName(std::move(driveName)),
        m_logicalLibrary(std::move(logicalLibrary)),
        m_baseDir(std::move(baseDir)) {
    const auto contentsBase = m_baseDir / "contents" / m_logicalLibrary;
    std::filesystem::create_directories(contentsBase / "drives" / m_driveName);
    std::filesystem::create_directories(contentsBase / "changer");

    const auto confPath = m_baseDir / "config" / m_logicalLibrary / "library.toml";
    if (std::filesystem::exists(confPath)) {
      const auto tbl = toml::parse_file(confPath.string());
      if (auto v = tbl["library"]["changerTimeoutMs"].value<int>()) {
        m_changerTimeoutMs = *v;
      }
    }
  }

  void mountTapeReadOnly(const std::string& vid, const cta::mediachanger::LibrarySlot& /*slot*/) override {
    mountTape(vid, false);
  }

  void mountTapeReadWrite(const std::string& vid, const cta::mediachanger::LibrarySlot& /*slot*/) override {
    mountTape(vid, true);
  }

  void dismountTape(const std::string& vid, const cta::mediachanger::LibrarySlot& /*slot*/) override {
    const auto contentsBase = m_baseDir / "contents" / m_logicalLibrary;
    const auto driveDir = contentsBase / "drives" / m_driveName;
    const auto changerDir = contentsBase / "changer";

    std::filesystem::path drivePath;
    if (vid.empty()) {
      // DriveCleaner uses an empty vid on purpose to mean "eject whatever cartridge is in the
      // drive" (startup cleanDrive(""), the drive-open-failure fallback, and the retry after a
      // VID-specific dismount fails). There is no VID to build a filename from, so find the
      // single *.tape file actually in the drive directory instead.
      for (const auto& entry : std::filesystem::directory_iterator(driveDir)) {
        if (entry.path().extension() != ".tape") {
          continue;
        }
        if (!drivePath.empty()) {
          // Stale state from an earlier crash, not a case this call can resolve on its own:
          // throw so the caller's failure handling (DriveCleaner sets ejectFailed and disables
          // the tape) is engaged instead of silently abandoning whichever file is left behind.
          throw cta::exception::Exception("VirtualChangerImpl::dismountTape: multiple .tape files in "
                                          + driveDir.string() + " with an empty vid — cannot tell which to eject");
        }
        drivePath = entry.path();
      }
      if (drivePath.empty()) {
        return;  // nothing in the drive to eject
      }
    } else {
      drivePath = driveDir / (vid + ".tape");
      if (!std::filesystem::exists(drivePath)) {
        return;
      }
    }

    const auto actualVid = drivePath.stem().string();
    const auto changerTape = changerDir / (actualVid + ".tape");
    const auto slotPath = contentsBase / "slots" / (actualVid + ".tape");

    const ChangerGuard guard(changerDir, m_changerTimeoutMs);
    std::filesystem::rename(drivePath, changerTape);  // drives/<drv>/ → changer/
    std::filesystem::rename(changerTape, slotPath);   // changer/ → slots/
  }

private:
  void mountTape(const std::string& vid, bool readWrite) {
    const auto contentsBase = m_baseDir / "contents" / m_logicalLibrary;
    const auto slotPath = contentsBase / "slots" / (vid + ".tape");
    const auto changerDir = contentsBase / "changer";
    const auto changerTape = changerDir / (vid + ".tape");
    const auto drivePath = contentsBase / "drives" / m_driveName / (vid + ".tape");

    if (!std::filesystem::exists(slotPath)) {
      if (readWrite) {
        // First archive on this VID: create an empty tape file in the slot.
        std::ofstream(slotPath, std::ios::binary).close();
      } else {
        throw cta::exception::Exception("StressMediaChanger: tape not found for retrieve: VID=" + vid + " expected at "
                                        + slotPath.string() + " — has this tape been archived yet?");
      }
    }

    const ChangerGuard guard(changerDir, m_changerTimeoutMs);
    std::filesystem::rename(slotPath, changerTape);   // slots/  → changer/
    std::filesystem::rename(changerTape, drivePath);  // changer/ → drives/<drv>/
  }

  std::string m_driveName;
  std::string m_logicalLibrary;
  std::filesystem::path m_baseDir;
  int m_changerTimeoutMs = 30'000;
};

}  // anonymous namespace

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor (normal mode)
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger& log)
    : m_impl(std::make_unique<HardwareChangerImpl>(rmcProxy, log)) {}

//------------------------------------------------------------------------------
// constructor (virtual-drive mode)
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(std::string driveName,
                                       std::string logicalLibraryName,
                                       std::filesystem::path stressBaseDir,
                                       log::Logger& log)
    : m_impl(std::make_unique<VirtualChangerImpl>(std::move(driveName),
                                                  std::move(logicalLibraryName),
                                                  std::move(stressBaseDir),
                                                  log)) {}

//------------------------------------------------------------------------------
// destructor and move operations — defined here so unique_ptr can see the
// complete MediaChangerFacadeImpl type
//------------------------------------------------------------------------------
MediaChangerFacade::~MediaChangerFacade() = default;
MediaChangerFacade::MediaChangerFacade(MediaChangerFacade&&) noexcept = default;
MediaChangerFacade& MediaChangerFacade::operator=(MediaChangerFacade&&) noexcept = default;

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot) {
  m_impl->mountTapeReadOnly(vid, slot);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot) {
  m_impl->mountTapeReadWrite(vid, slot);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::dismountTape(const std::string& vid, const LibrarySlot& slot) {
  m_impl->dismountTape(vid, slot);
}

}  // namespace cta::mediachanger
