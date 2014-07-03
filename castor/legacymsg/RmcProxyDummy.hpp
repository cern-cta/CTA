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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/RmcProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A dummy rmc proxy.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class RmcProxyDummy: public RmcProxy {
public:

  /**
   * Destructor.
   */
  ~RmcProxyDummy() throw();

  /**
   * Asks the remote media-changer daemon to mount the specified tape into the
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in one of the following three forms:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   * @param mode The access mode in which the specified tape should be mounted.
   * Please note that the value of this parameter is honored in a best effort
   * fashion.  If the library and drive combination do not support specifying
   * the access mode, then this parameter will be ignored.
   */
  void mountTape(const std::string &vid, const std::string &librarySlot,
    const MountMode mode); 

  /**
   * Asks the remote media-changer daemon to unmount the specified tape from the
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot in one of the following three forms:
   * "acs@rmc_host,ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER",
   * "manual" or "smc@rmc_host,drive_ordinal".
   */
  void unmountTape(const std::string &vid, const std::string &librarySlot) ;

}; // class RmcProxyDummy

} // namespace legacymsg
} // namespace castor

