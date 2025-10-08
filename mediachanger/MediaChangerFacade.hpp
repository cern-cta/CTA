/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/MediaChangerProxy.hpp"
#include "mediachanger/DmcProxy.hpp"
#include "mediachanger/RmcProxy.hpp"

#include <memory>
#include <string>

namespace cta::mediachanger {

/**
 * A facade to multiple types of tape media changer.
 */
class MediaChangerFacade {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.  This log
   * object will be used by the dummy media changer to communicate with the
   * tape operator.
   */
  MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger &log);

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support read-only mounts.
   *
   * @param vid The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid, const LibrarySlot &slot);

  /**
   * Requests the media changer to mount the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid, const LibrarySlot &slot);

  /**
   * Requests the media changer to dismount the specified tape from the
   * drive in the specifed library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void dismountTape(const std::string &vid, const LibrarySlot &slot);

private:

  /**
   * SCSI media changer proxy.
   */
  RmcProxy m_rmcProxy;

  /**
   * Manual media changer proxy.
   */
  DmcProxy m_dmcProxy;

  /**
   * Returns the media changer proxy for the specified library type.
   */
  MediaChangerProxy &getProxy(const TapeLibraryType libraryType);

}; // class MediaChangerFacade

} // namespace cta::mediachanger
