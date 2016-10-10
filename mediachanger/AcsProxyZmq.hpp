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

#include "mediachanger/AcsProxy.hpp"
#include "mediachanger/Frame.hpp"
#include "mediachanger/ZmqSocketMT.hpp"

#include <mutex>

namespace cta {
namespace mediachanger {

/**
 * Concrete class providing a ZMQ implementation of an AcsProxy.
 */
class AcsProxyZmq: public AcsProxy {
public:

  /**
   * Constructor.
   *
   * @param serverPort The TCP/IP port on which the CASTOR ACS daemon is
   * listening for ZMQ messages.
   * @param zmqContext The ZMQ context.
   */
  AcsProxyZmq(const unsigned short serverPort, void *const zmqContext) throw();

  /**
   * Request the CASTOR ACS daemon to mount the specified tape for read-only
   * access into the tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

  /** 
   * Request the CASTOR ACS daemon to mount the specifed tape for read/write
   * access into the tape drive in the specified library slot.
   *    
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

  /** 
   * Request the CASTOR ACS daemon to dismount the specifed tape from the tape
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void dismountTape(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

  /**
   * Request the CASTOR ACS daemon to forcefully dismount the specifed tape
   * from the tape drive in the specified library slot.  Forcefully means
   * rewinding and ejecting the tape if necessary.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void forceDismountTape(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

private:
  
  /**
   * Mutex used to implement a critical section around the enclosed
   * ZMQ socket.
   */
  std::mutex m_mutex;
   
  /**
   * The TCP/IP port on which the CASTOR ACS daemon is listening for ZMQ
   * messages.
   */
  const unsigned short m_serverPort;

  /**
   * Socket connecting this proxy the daemon it represents.
   */
  ZmqSocketMT m_serverSocket;

  /**
   * Creates a frame containing a AcsMountTapeForRecall message.
   *
   * @param vid The tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   * @return The frame.
   */
  Frame createAcsMountTapeReadOnlyFrame(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);
  
  /**
   * Creates a frame containing a AcsMountTapeForMigration message.
   *
   * @param vid The tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   * @return The frame.
   */
  Frame createAcsMountTapeReadWriteFrame(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);
  
  /**
   * Creates a frame containing a AcsDismountTape message.
   *
   * @param vid The tape to be dismounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   * @return The frame.
   */
  Frame createAcsDismountTapeFrame(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

  /**
   * Creates a frame containing a AcsDismountTape message.
   *
   * @param vid The tape to be dismounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   * @return The frame.
   */
  Frame createAcsForceDismountTapeFrame(const std::string &vid,
    const cta::mediachanger::AcsLibrarySlot &librarySlot);

}; // class AcsProxyZmq

} // namespace mediachanger
} // namespace cta
