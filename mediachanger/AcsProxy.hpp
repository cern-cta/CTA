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

#include "common/threading/Mutex.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/MediaChangerProxy.hpp"
#include "mediachanger/ZmqSocket.hpp"

#include <memory>

namespace cta {
namespace mediachanger {

/**
 * A ZMQ media changer proxy.
 */
class AcsProxy: public MediaChangerProxy {
public:

  /**
   * Constructor.
   *
   * @param serverPort The TCP/IP port on which the CASTOR ACS daemon is
   * listening for ZMQ messages.
   */
  AcsProxy(const unsigned short serverPort = ACS_PORT);

  /**
   * Request the CASTOR ACS daemon to mount the specified tape for read-only
   * access into the tape drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) override;

  /** 
   * Request the CASTOR ACS daemon to mount the specifed tape for read/write
   * access into the tape drive in the specified library slot.
   *    
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) override;

  /** 
   * Request the CASTOR ACS daemon to dismount the specifed tape from the tape
   * drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void dismountTape(const std::string &vid, const LibrarySlot &librarySlot) override;

  /**
   * Request the CASTOR ACS daemon to forcefully dismount the specifed tape
   * from the tape drive in the specified library slot.  Forcefully means
   * rewinding and ejecting the tape if necessary.
   *
   * @param vid The volume identifier of the tape to be mounted.
   * @param librarySlot The slot in the library that contains the tape drive.
   */
  void forceDismountTape(const std::string &vid, const LibrarySlot &librarySlot) override;

private:

  /**
   * Mutex used to implement a critical section around the enclosed
   * ZMQ socket.
   */
  threading::Mutex m_mutex;
   
  /**
   * The TCP/IP port on which the CASTOR ACS daemon is listening for ZMQ
   * messages.
   */
  const unsigned short m_serverPort;

  /**
   * Socket connecting this proxy to the daemon it represents.
   */
  std::unique_ptr<ZmqSocket> m_serverSocket;

  /**
   * Returns the socket instance connecting this proxy to the daemon it
   * represents.  This method instantiates a socket and connects it if
   * a socket does not already exist.
   *
   * Please note that a lock MUST be taken on m_mutex before calling this
   * method.
   *
   * @return The socket connecting this proxy the daemon it represents.
   */
  ZmqSocket &serverSocketInstance();

}; // class AcsProxy

} // namespace mediachanger
} // namespace cta
