/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

#include "mediachanger/acs/Constants.hpp"
#include "mediachanger/Frame.hpp"
#include "mediachanger/ZmqSocket.hpp"
#include "mediachanger/reactor/ZMQReactor.hpp"
#include "AcsDaemon.hpp"
#include "AcsdConfiguration.hpp"
#include "AcsPendingRequests.hpp"
#include "common/log/SyslogLogger.hpp"
#include "mediachanger/reactor/ZMQPollEventHandler.hpp"

namespace cta     {
namespace mediachanger      {
namespace acs     {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the tape 
 * server daemon.
 */
class AcsMessageHandler: public cta::mediachanger::reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param reactor The reactor to which new CTA ACS daemon connection 
   * handlers are to be registered.
   * @param hostName   The name of the host.
   * @param zmqContext The ZMQ context.
   * @param ctaConf The configuration for the CTA ACS daemon.
   * @param acsPendingRequests The object to handle requests to the CTA ACS
   * daemon.
   */
  AcsMessageHandler(
    log::Logger &log,
    cta::mediachanger::reactor::ZMQReactor &reactor,
    void *const zmqContext,
    const std::string &hostName,
    const AcsdConfiguration &ctaConf,
    AcsPendingRequests &acsPendingRequests);

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
  cta::mediachanger::Frame createReturnValueFrame(const int value);

  /**
   * Creates a message frame containing an Exception message.
   *
   * @param code The error code of the exception.
   * @param msg The message string of the exception.
   */
  cta::mediachanger::Frame createExceptionFrame(const int code,
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
  cta::mediachanger::Frame dispatchMsgHandler(const cta::mediachanger::Frame &rqst) ;

  /**
   * Handles the mount tape for read-only.
   *
   * @param  rqst The request.
   * @return The reply.
   */
  cta::mediachanger::Frame handleAcsMountTapeReadOnly(const cta::mediachanger::Frame &rqst);
  
  /**
   * Handles the mount tape for read/write.
   *
   * @param  rqst The request.
   * @return The reply.
   */
  cta::mediachanger::Frame handleAcsMountTapeReadWrite(const cta::mediachanger::Frame &rqst);

  /**
   * Handles the dismount tape request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  cta::mediachanger::Frame handleAcsDismountTape(const cta::mediachanger::Frame &rqst);

  /**
   * Handles the force dismount tape request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  cta::mediachanger::Frame handleAcsForceDismountTape(const cta::mediachanger::Frame &rqst);
  
  log::Logger &m_log;
  /**
   * The reactor to which new CTA ACS daemon connection handlers are to
   * be registered.
   */
  cta::mediachanger::reactor::ZMQReactor &m_reactor;

  /**
   * The ZMQ socket listening for messages.
   */
  cta::mediachanger::ZmqSocketST m_socket;
 
  /**
   * The name of the host on which CTA ACS daemon is running.
   */
  const std::string m_hostName;
  

  /**
   * The configuration parameters for the CTA ACS daemon.
   */
  const acs::daemon::AcsdConfiguration m_ctaConf;
  
  /**
   * The object to handle requests to the CTA ACS daemon.
   */
  AcsPendingRequests &m_acsPendingRequests;
  
}; // class AcsMessageHandler

} // namespace deamon
} // namespace acs
} // namespace mediachanger
} // namespace cta
