/******************************************************************************
 *         castor/legacymsg/TapeserverProxy.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

namespace castor {
namespace legacymsg {

/**
 * Abstract class defining the interface to a proxy object representing the
 * internal network interface of the tapeserverd daemon.
 */
class TapeserverProxy {
public:
  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  virtual ~TapeserverProxy() throw() = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void gotReadMountDetailsFromClient(
    const std::string &unitName,
    const std::string &vid)
     = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void gotWriteMountDetailsFromClient(
    const std::string &unitName,
    const std::string &vid)
     = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void gotDumpMountDetailsFromClient(
    const std::string &unitName,
    const std::string &vid)
     = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void tapeMountedForRead(
    const std::string &unitName,
    const std::string &vid)
     = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void tapeMountedForWrite(
    const std::string &unitName, 
    const std::string &vid)
     = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been unmounted.
   *
   * @param unitName The unit name of the tape drive.
   * @param vid The Volume ID of the tape to be mounted.
   */
  virtual void tapeUnmounted(
    const std::string &unitName,
    const std::string &vid)
     = 0;

}; // class TapeserverProxy

} // namespace legacymsg
} // namespace castor

