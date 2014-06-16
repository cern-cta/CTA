/******************************************************************************
 *                TapeMessageHandler.hpp
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

#include "castor/io/ZMQPollEventHandler.hpp"
#include "castor/io/ZMQReactor.hpp"
#include "castor/log/Logger.hpp"
#include "zmq/zmqcastor.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Heartbeat.pb.h"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the admin
 * commands.
 */
class TapeMessageHandler: public io::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the socket listening for
   * connections from the vdqmd daemon.
   * @param reactor The reactor to which new Vdqm connection handlers are to
   * be registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  TapeMessageHandler(
    io::ZMQReactor &reactor,
    log::Logger &log);

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq::pollitem_t &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  bool handleEvent(const zmq::pollitem_t &fd);
  
private:
   /**
   * The reactor to which new Vdqm connection handlers are to be registered.
   */
  io::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  zmq::socket_t m_socket;
  
  void checkSocket(const zmq::pollitem_t &fd);
  
  void dispatchEvent(const castor::messages::Header& header);
  
  void dealWith(const castor::messages::Header&,
                         const castor::messages::Heartbeat& body);
  /**
   * Unserialise the blob and check the header
   * @param headerBlob
   */
  castor::messages::Header buildHeader(const zmq::message_t& headerBlob);
}; // class TapeMessageHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
