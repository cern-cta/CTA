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
#include "mediachanger/MmcProxy.hpp"
#include "mediachanger/MmcProxyNotSupported.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "mediachanger/AcsProxy.hpp"

#include <unistd.h>
#include <sys/types.h>

namespace cta {
namespace mediachanger {

/**
 * Provides a facade to three communications proxies: acs, manual and rmc.
 *
 * This facade will forward requests to mount and dismount tapes to the
 * appropriate communications proxy based on the type of library slot.
 */
class MediaChangerFacade {
public:

  /**
   * Constructor.
   *
   * @param acs Proxy object representing the CASTOR ACS daemon.
   * @param mmc Proxy object representing the manual media-changer.
   * @param rmc Proxy object representing the rmcd daemon.
   */
  MediaChangerFacade(AcsProxy &acs, MmcProxy &mmc, RmcProxy &rmc) throw();

  /**
   * Constructor.
   *
   * Use this constructor when manual media-changers are not to be supported.
   *
   * @param acs Proxy object representing the CASTOR ACS daemon.
   * @param rmc Proxy object representing the rmcd daemon.
   */
  MediaChangerFacade(AcsProxy &acs, RmcProxy &rmc) throw();

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

  /**
   * Requests the media changer to forcefully dismount the specified tape from
   * the drive in the specifed library slot.  Forcefully means rewinding and
   * ejecting the tape where necessary.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support forceful dismounts.
   *
   * @param vid The volume identifier of the tape.
   * @param slot The library slot containing the tape drive.
   */
  void forceDismountTape(const std::string &vid, const LibrarySlot &slot);

private:

  /**
   * Proxy object representing the CASTOR ACS daemon.
   */
  AcsProxy &m_acs;

  /**
   * Proxy object representing the manual media-changer.
   */
  MmcProxy &m_mmc;

  /**
   * Proxy object representing the rmcd daemon.
   */
  RmcProxy &m_rmc;

  /**
   * The manual media-changer proxy object that is used when manual
   * media-changers are not to be supported.
   */
  MmcProxyNotSupported m_mmcNotSupported;

}; // class MediaChangerFacade

} // namespace mediachanger
} // namespace cta
