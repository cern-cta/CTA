/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/mediachanger/MediaChangerProxy.hpp"
#include "castor/messages/AcsProxy.hpp"

#include <unistd.h>
#include <sys/types.h>

namespace castor {
namespace mediachanger {

/**
 * Provides a facade to three communications proxies: acs, manual and rmc.
 *
 * This facade will forward requests to mount and dismount tapes to the
 * appropriate communications proxy based on the type of library slot.
 */
class MediaChangerFacade: public MediaChangerProxy {
public:

  /**
   * Constructor.
   *
   * @param acs Proxy object representing the CASTOR ACS daemon.
   * @param mmc Proxy object representing the manual media-changer.
   * @param rmc Proxy object representing the rmcd daemon.
   */
  MediaChangerFacade(
    messages::AcsProxy &acs,
    MediaChangerProxy &mmc,
    MediaChangerProxy &rmc) throw();

  /**
   * Requests the media changer to mount of the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid,
    const TapeLibrarySlot &librarySlot);

  /**
   * Requests the media changer to mount of the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid,
    const TapeLibrarySlot &librarySlot);

  /**
   * Requests the media changer to dismount of the specified tape from the
   * drive in the specifed library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void dismountTape(const std::string &vid,
    const TapeLibrarySlot &librarySlot);

private:

  /**
   * Proxy object representing the CASTOR ACS daemon.
   */
  messages::AcsProxy &m_acs;

  /**
   * Proxy object representing the manual media-changer.
   */
  MediaChangerProxy &m_mmc;

  /**
   * Proxy object representing the rmcd daemon.
   */
  MediaChangerProxy &m_rmc;

}; // class MediaChangerFacade

} // namespace mediachanger
} // namespace castor
