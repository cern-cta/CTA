/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerProxy.hpp"

namespace cta::mediachanger {

/**
 * Manual media changer proxy that simply logs mount and dismount requests.
 */
class DmcProxy: public MediaChangerProxy {
public:

  /**
   * Constructor
   *
   * @param log Object representing the API to the CTA logging system
   */
  explicit DmcProxy(log::Logger& log);

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support read-only mounts.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) override;

  /**
   * Requests the media changer to mount the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) override;

  /**
   * Requests the media changer to dismount the specified tape from the
   * drive in the specifed library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void dismountTape(const std::string &vid, const LibrarySlot &librarySlot) override;

private:

  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

}; // class DmcProxy

} // namespace cta::mediachanger
