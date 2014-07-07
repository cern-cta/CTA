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
#include "castor/tape/tapeserver/daemon/ProcessForkerFrame.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerMsgType.hpp"

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
   * Structure defining the result of a message handler.
   */
  struct MsgHandlerResult {
    /**
     * True if the main event loop should continue.
     */
    bool continueMainEventLoop;

    /**
     * The reply frame.
     */
    ProcessForkerFrame reply;

    /**
     * Constructor.
     */
    MsgHandlerResult() throw():
      continueMainEventLoop(false) {
    }
  }; // struct MsgHandlerResult

  /**
   * Handles any pending events.
   *
   * @return true if the main event loop should continue.
   */
  bool handleEvents();

  /**
   * Return strue if there is a pending message from the ProcessForker proxy.
   */
  bool thereIsAPendingMsg();

  /**
   * Reads in and handles a single message from the ProcessForker proxy.
   */
  bool handleMsg();

  /**
   * Dispatches the appropriate handler method for the message contained within
   * the specified frame;
   *
   * @param frame The frame containing the message.
   * @return The result of dispatching the message handler.
   */
  MsgHandlerResult dispatchMsgHandler(const ProcessForkerFrame &frame);

  /**
   * Handles a StopProcessForker message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleStopProcessForkerMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkLabel message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkLabelMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkDataTransfer message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkDataTransferMsg(const ProcessForkerFrame &frame);

  /**
   * Handles a ForkCleaner message.
   *
   * @param frame The frame containing the message.
   * @return The result of the message handler.
   */
  MsgHandlerResult handleForkCleanerMsg(const ProcessForkerFrame &frame);

}; // class ProcessForker

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
