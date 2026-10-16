/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"
#include "mediachanger/DmcProxy.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/MediaChangerProxy.hpp"
#include "mediachanger/RmcProxy.hpp"

#include <filesystem>
#include <memory>
#include <string>

namespace cta::mediachanger {

/**
 * A facade to multiple types of tape media changer.
 *
 * Two construction modes:
 *
 * Normal mode (production):
 *   MediaChangerFacade(rmcProxy, log)
 *   mount/dismount calls are forwarded to the rmcd daemon via RmcProxy or to
 *   the operator console via DmcProxy, depending on the library slot type.
 *
 * Stress mode (scale testing without real hardware):
 *   MediaChangerFacade(driveName, logicalLibraryName, stressBaseDir, log)
 *   Tape movements are serialised through a single virtual changer robot,
 *   mirroring the single-robot-arm behaviour seen in production libraries.
 *   StressDrive::waitUntilReady() scans drives/<drv>/ for the mounted tape.
 *
 * Directory layout (all under stressBaseDir):
 *   config/<lib>/library.toml          — library/mediatype configuration
 *   contents/<lib>/slots/VID.tape      — tape at rest
 *   contents/<lib>/changer/VID.tape    — tape in transit (robot holds it)
 *   contents/<lib>/changer/.lock       — robot-busy flag (O_CREAT|O_EXCL)
 *   contents/<lib>/drives/<drv>/       — empty = no tape loaded
 *   contents/<lib>/drives/<drv>/VID.tape — tape mounted in this drive
 *
 * Mount path:    slots/ → changer/ → drives/<drv>/
 * Dismount path: drives/<drv>/ → changer/ → slots/
 *
 * Only one tape is ever in changer/ at a time; concurrent calls spin-wait on
 * .lock with a 30 s timeout before throwing.
 *
 * Extendability: this implementation supports a single logical library per
 * MediaChangerFacade instance (one stressBaseDir / logicalLibraryName pair).
 * To support multiple libraries with independent drive sets, instantiate one
 * MediaChangerFacade per library — each points to the same stressBaseDir but
 * a different logicalLibraryName, so their changer/ directories are independent
 * (no cross-library lock contention).  The fake-tape-library Helm chart would
 * need a libraries: [...] array instead of the current single library: block.
 */
class MediaChangerFacade {
public:
  /**
   * Normal constructor.
   *
   * @param rmcProxy  Proxy to the rmcd media-changer daemon.
   * @param log       CTA logging API; forwarded to DmcProxy for operator prompts.
   */
  MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger& log);

  /**
   * Stress-mode constructor.  No rmcd connection is made or needed.
   *
   * @param driveName          Drive name as registered in the CTA catalogue
   *                           (e.g. "STRESS0003").
   * @param logicalLibraryName Logical library this drive belongs to; used to
   *                           construct the contents/<lib>/… paths.
   * @param stressBaseDir      Root of the stress-test shared volume
   *                           (e.g. /dev/shm/cta-stress).
   * @param log                CTA logging API.
   */
  MediaChangerFacade(std::string driveName,
                     std::string logicalLibraryName,
                     std::filesystem::path stressBaseDir,
                     log::Logger& log);

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * In stress mode: atomically renames contents/<lib>/slots/VID.tape into
   * contents/<lib>/drives/<driveName>/VID.tape.  Throws if the slot file is
   * absent (tape has never been archived).
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support read-only mounts.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot);

  /**
   * Requests the media changer to mount the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * In stress mode: creates an empty contents/<lib>/slots/VID.tape if it does
   * not yet exist (first archive on this VID), then atomically renames it into
   * contents/<lib>/drives/<driveName>/VID.tape.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot);

  /**
   * Requests the media changer to dismount the specified tape from the
   * drive in the specified library slot.
   *
   * In stress mode: atomically renames contents/<lib>/drives/<driveName>/VID.tape
   * back to contents/<lib>/slots/VID.tape.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void dismountTape(const std::string& vid, const LibrarySlot& slot);

private:
  // Whether this instance operates in stress (no-rmcd) mode.
  const bool m_stressMode = false;

  // Stress-mode drive name, logical library, and base directory (empty in normal mode).
  const std::string m_stressDriveName;
  const std::string m_stressLogicalLibrary;
  const std::filesystem::path m_stressBaseDir;

  // Changer lock timeout in ms, read from library.toml (library changerTimeoutMs).
  int m_changerTimeoutMs = 30'000;

  // SCSI media changer proxy (normal mode only; default-constructed in stress mode).
  RmcProxy m_rmcProxy;

  // Manual media changer proxy.
  DmcProxy m_dmcProxy;

  // Returns the appropriate proxy for a given library slot (normal mode only).
  MediaChangerProxy& getProxy(const LibrarySlot& slot);

  // Stress-mode helpers: move VID.tape via the single changer robot.
  void stressMountTape(const std::string& vid, bool readWrite);
  void stressDismountTape(const std::string& vid);

};  // class MediaChangerFacade

}  // namespace cta::mediachanger
