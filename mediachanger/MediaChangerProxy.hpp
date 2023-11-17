/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

