/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"

#include <stdint.h>
#include <string>

namespace cta::mediachanger {

/**
 * Abstract class defining the interface to a proxy object representing a
 * media changer daemon.
 */
class MediaChangerProxy {
public:

  /**
   * Destructor.
   */
  virtual ~MediaChangerProxy()  = 0;

  /**
   * Request the media changer daemon to mount the specified tape for read-only
   * access into the tape drive in the specified library slot.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support read-only mounts.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) = 0;

  /**
   * Request the media changer daemon to mount the specifed tape for read/write
   * access into the tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) = 0;

  /**
   * Request the media changer daemon to dismount the specifed tape from the
   * tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  virtual void dismountTape(const std::string &vid, const LibrarySlot &librarySlot) = 0;

}; // class MediaChangerProxy

} // namespace cta::mediachanger

