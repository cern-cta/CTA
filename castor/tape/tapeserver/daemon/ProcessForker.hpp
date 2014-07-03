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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"

#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for forking processes.
 */
class ProcessForker {
public:

  /**
   * Constructor.
   *
   * This class takes ownership of the socket used to communicate with the
   * client.  The destructor of this class will close the file-descriptor.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param socketFd The file-descriptor of the socket used to communicate with
   * the client.
   */
  ProcessForker(log::Logger &log, const int socketFd) throw();

  /**
   * Destructor.
   *
   * Closes the  file-descriptor of the socket used to communicate with the
   * client.
   */
  ~ProcessForker() throw();

  /**
   * Executes the main event loop of the ProcessForker.
   */
  void execute();

private:

  /**
   * The maximum permitted size in bytes for the payload of a frame sent between
   * the ProcessForker and its proxy.
   */
  static const ssize_t s_maxPayloadLen = 1024;

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The file-descriptor of the socket used to communicate with the client.
   */
  const int m_socketFd;

  /**
   * Handles any pending events.
   *
   * @return true if the main event loop should continue.
   */
  bool handleEvents();

  /**
   * Reads in and handles a single message from the ProcessForker proxy.
   */
  bool handleMsg();

  /**
   * Reads an unsigned 32-bit integer from the socket connected to the
   * ProcessForkerProxySocket.
   *
   * @return The unsigned 32-bit integer.
   */
  uint32_t readUint32FromSocket();

  /**
   * Dispatches the appropriate handle method for the specified message.
   *
   * @param msgType The type of the message.
   * @param payload The frame payload containing the message.
   * @return True if the main event loop should continue.
   */
  bool dispatchMsgHandler(const uint32_t msgType, const std::string &payload);

  /**
   * Handles a StopProcessForker message.
   *
   * @param payload The frame payload containing the message.
   * @return True if the main event loop should continue.
   */
  bool handleStopProcessForkerMsg(const std::string &payload);

  /**
   * Handles a ForkLabel message.
   *
   * @param payload The frame payload containing the message.
   * @return True if the main event loop should continue.
   */
  bool handleForkLabelMsg(const std::string &payload);

  /**
   * Handles a ForkDataTransfer message.
   *
   * @param payload The frame payload containing the message.
   * @return True if the main event loop should continue.
   */
  bool handleForkDataTransferMsg(const std::string &payload);

  /**
   * Handles a ForkCleaner message.
   *
   * @param payload The frame payload containing the message.
   * @return True if the main event loop should continue.
   */
  bool handleForkCleanerMsg(const std::string &payload);

  /**
   * Reads the payload of a frame from the socket connected to the ProcessForker
   * proxy.
   *
   * @param payloadLen The length of the payload in bytes.
   * @return The payload as a string.
   */
  std::string readPayloadFromSocket(const ssize_t payloadLen);

}; // class ProcessForker

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
