/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/MediaChangerFacade.hpp"

#include "common/exception/Exception.hpp"

#include <filesystem>

namespace cta::mediachanger {

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
                                       std::filesystem::path stressBaseDir,
                                       log::Logger& log)
    : m_stressMode(true),
      m_stressDriveName(std::move(driveName)),
      m_stressBaseDir(std::move(stressBaseDir)),
      m_dmcProxy(log) {
  // Ensure the drives directory exists so that symlinks can be created inside it.
  std::filesystem::create_directories(m_stressBaseDir / "drives");
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
  const auto tapeDir = m_stressBaseDir / "tapes" / vid;

  if (readWrite) {
    // First archive on this VID: create the tape directory.
    std::filesystem::create_directories(tapeDir);
  } else {
    // Retrieve: the tape directory must already exist from a prior archive.
    if (!std::filesystem::is_directory(tapeDir)) {
      throw cta::exception::Exception(
        "StressMediaChanger: tape not found for retrieve: VID=" + vid +
        " expected at " + tapeDir.string() +
        " — has this tape been archived yet?");
    }
  }

  const auto symlinkPath = m_stressBaseDir / "drives" / m_stressDriveName;

  // Remove any stale symlink left by a previous session that crashed between
  // mount and dismount without cleaning up.
  if (std::filesystem::is_symlink(symlinkPath)) {
    std::filesystem::remove(symlinkPath);
  }

  // Use a relative target so the tree remains valid if the base directory is
  // moved or bind-mounted at a different path.
  const auto relTarget = std::filesystem::relative(tapeDir, symlinkPath.parent_path());
  std::filesystem::create_symlink(relTarget, symlinkPath);
}

//------------------------------------------------------------------------------
// stressDismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::stressDismountTape(const std::string& /*vid*/) {
  const auto symlinkPath = m_stressBaseDir / "drives" / m_stressDriveName;
  // Best-effort: if the symlink is already gone (e.g. the drive process
  // crashed) that is not treated as an error here.
  std::filesystem::remove(symlinkPath);
}

}  // namespace cta::mediachanger
