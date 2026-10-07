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
 *   MediaChangerFacade(driveName, stressBaseDir, log)
 *   mount/dismount are no-ops on the rmcd side; instead they manage a tmpfs
 *   symlink at stressBaseDir/drives/{driveName} → stressBaseDir/tapes/{VID}/
 *   so that StressDrive::waitUntilReady() can resolve which VID is loaded
 *   without any daemon involvement.  See StressDrive.hpp for the full layout.
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
   * @param driveName     Drive name as registered in the CTA catalogue
   *                      (e.g. "STRESS0003").  Written into the symlink path
   *                      stressBaseDir/drives/{driveName} on each mount.
   * @param stressBaseDir Root of the stress-test tmpfs tree
   *                      (e.g. /dev/shm/cta-stress).
   * @param log           CTA logging API.
   */
  MediaChangerFacade(std::string driveName, std::filesystem::path stressBaseDir, log::Logger& log);

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * In stress mode: validates that stressBaseDir/tapes/{vid} exists (i.e. the
   * tape was previously archived), then creates the drives/{driveName} symlink.
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
   * In stress mode: creates stressBaseDir/tapes/{vid}/ if it does not yet
   * exist (first archive on this VID), then creates the drives/{driveName}
   * symlink.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot);

  /**
   * Requests the media changer to dismount the specified tape from the
   * drive in the specified library slot.
   *
   * In stress mode: removes the drives/{driveName} symlink.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void dismountTape(const std::string& vid, const LibrarySlot& slot);

private:
  // Whether this instance operates in stress (no-rmcd) mode.
  const bool m_stressMode = false;

  // Stress-mode drive name and base directory (empty in normal mode).
  const std::string m_stressDriveName;
  const std::filesystem::path m_stressBaseDir;

  // SCSI media changer proxy (normal mode only; default-constructed in stress mode).
  RmcProxy m_rmcProxy;

  // Manual media changer proxy.
  DmcProxy m_dmcProxy;

  // Returns the appropriate proxy for a given library slot (normal mode only).
  MediaChangerProxy& getProxy(const LibrarySlot& slot);

  // Stress-mode helpers: create/remove the drives/{driveName} symlink.
  void stressMountTape(const std::string& vid, bool readWrite);
  void stressDismountTape(const std::string& vid);

};  // class MediaChangerFacade

}  // namespace cta::mediachanger
