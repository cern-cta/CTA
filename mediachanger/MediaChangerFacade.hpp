/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/log/Logger.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/MediaChangerProxy.hpp"
#include "mediachanger/MmcProxy.hpp"
#include "mediachanger/RmcProxy.hpp"

#include <memory>
#include <string>

namespace cta {
namespace mediachanger {

/**
 * A facade to multiple types of tape media changer.
 */
class MediaChangerFacade {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.  This log
   * object will be used by the manual media changer to communicate with the
   * tape operator.
   */
  MediaChangerFacade(log::Logger &log);

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
   * Manual media changer proxy.
   */
  MmcProxy m_mmcProxy;

  /**
   * SCSI media changer proxy.
   */
  RmcProxy m_rmcProxy;

  /**
   * Returns the media changer proxy for the specified library type.
   */
  MediaChangerProxy &getProxy(const TapeLibraryType libraryType);

}; // class MediaChangerFacade

} // namespace mediachanger
} // namespace cta
