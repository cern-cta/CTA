/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/RmcProxy.hpp"

#include <filesystem>
#include <memory>
#include <string>

namespace cta::mediachanger {

class MediaChangerFacadeImpl;

/**
 * A facade to multiple types of tape media changer.
 *
 * The constructor selects the implementation:
 *   - Normal (production): routes mount/dismount calls to rmcd or the operator
 *     console, depending on the library slot type.
 *   - Virtual (stress testing): simulates a single-robot-arm changer via
 *     filesystem operations on a shared volume without real hardware.
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
   * Virtual-drive constructor (stress/scale testing without real hardware).
   *
   * @param driveName          Drive name as registered in the CTA catalogue
   *                           (e.g. "STRESS0003").
   * @param logicalLibraryName Logical library this drive belongs to.
   * @param stressBaseDir      Root of the shared virtual-tape-library volume.
   * @param log                CTA logging API.
   */
  MediaChangerFacade(std::string driveName,
                     std::string logicalLibraryName,
                     std::filesystem::path stressBaseDir,
                     log::Logger& log);

  ~MediaChangerFacade();

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
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
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot);

  /**
   * Requests the media changer to dismount the specified tape from the
   * drive in the specified library slot.
   *
   * @param vid  The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void dismountTape(const std::string& vid, const LibrarySlot& slot);

private:
  std::unique_ptr<MediaChangerFacadeImpl> m_impl;
};

}  // namespace cta::mediachanger
