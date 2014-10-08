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


#include "castor/log/Logger.hpp"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/ZmqSocketST.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/acs/AcsDaemon.hpp"

namespace castor     {
namespace acs        {

/**
 * Handles the events of the socket listening for connection from the tape 
 * server daemon.
 */
class AcsMessageHandler: public castor::tape::reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param reactor The reactor to which new CASTOR ACS daemon connection 
   * handlers are to be registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param hostName   The name of the host.
   * @param zmqContext The ZMQ context.
   * @param castorConf The configuration for the CASTOR ACS daemon.
   */
  AcsMessageHandler(
    castor::tape::reactor::ZMQReactor &reactor,
    log::Logger &log,
    const std::string &hostName,
    void *const zmqContext,
    const AcsDaemon::CastorConf &castorConf);

  /**
   * Destructor.
   */
  ~AcsMessageHandler() throw();

  /**
   * Returns the human-readable name this event handler.
   */
  std::string getName() const throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq_pollitem_t &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  bool handleEvent(const zmq_pollitem_t &fd) throw();
  
private:

  /**
   * Creates a message frame containing a ReturnValue message.
   *
   * @param value The return value of the ReturnValue message.
   * @return The message frame.
   */
  messages::Frame createReturnValueFrame(const int value);

  /**
   * Creates a message frame containing an Exception message.
   *
   * @param code The error code of the exception.
   * @param msg The message string of the exception.
   */
  messages::Frame createExceptionFrame(const int code,
    const std::string& msg);
     
  /**
   * Make sure the  zmq_pollitem_t's socket is the same as m_socket
   * Throw an exception if it is not the case
   * @param fd the poll item 
   */
  void checkSocket(const zmq_pollitem_t &fd) const;
  
  /**
   * Dispatches the appropriate handler method for the specified request
   * message.
   *
   * @param  rqst The request.
   * @return The reply.
   */
  messages::Frame dispatchMsgHandler(const messages::Frame &rqst) ;

  /**
   * Handles the mount tape for recall.
   *
   * @param  rqst The request.
   * @return The reply.
   */
  messages::Frame handleAcsMountTapeForRecall(const messages::Frame &rqst);
  
  /**
   * Handles the mount tape for migration request.
   *
   * @param  rqst The request.
   * @return The reply.
   */
  messages::Frame handleAcsMountTapeForMigration(const messages::Frame &rqst);

   /**
   * Handles the dismount tape request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleAcsDismountTape(const messages::Frame &rqst);
  
  /**
   * The reactor to which new CASTOR ACS daemon connection handlers are to
   * be registered.
   */
  castor::tape::reactor::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  /**
   * The ZMQ socket listening for messages.
   */
  messages::ZmqSocketST m_socket;
 
  /**
   * The name of the host on which CASTOR ACS daemon is running.
   */
  const std::string m_hostName;
  
  /**
   * The configuration parameters for the CASTOR ACS daemon.
   */
  const AcsDaemon::CastorConf &m_castorConf;
  
}; // class AcsMessageHandler

} // namespace acs
} // namespace castor
