/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"

#include <stdint.h>
#include <string>

namespace cta {
namespace mediachanger {

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

} // namespace mediachanger
} // namespace cta

