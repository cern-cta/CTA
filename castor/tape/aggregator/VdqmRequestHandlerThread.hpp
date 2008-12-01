/******************************************************************************
 *                castor/tape/aggregator/VdqmRequestHandlerThread.hpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP
#define CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Queue.hpp"

#include <map>


namespace castor {
namespace tape {
namespace aggregator {

  /**
   * Handles the requests from the VDQM server.
   */
  class VdqmRequestHandlerThread : public castor::server::IThread {

  public:

    /**
     * Constructor
     *
     * @param listenPort The TCP/IP port to which RTCPD should connect.
     */
    VdqmRequestHandlerThread(const int listenPort) throw();

    /**
     * Destructor
     */
    ~VdqmRequestHandlerThread() throw();

    /**
     * Initialization of the thread.
     */
    virtual void init() throw();

    /**
     * Main work for this thread.
     */
    virtual void run(void *param) throw();

    /**
     * Convenience method to stop the thread.
     */
    virtual void stop() throw();


  private:

    /**
     * The TCP/IP port to which RTCPD should connect.
     */
    const int m_listenPort;

    /**
     * Pointer to handler function, where handler function is a member of
     * RequestHandler.
     *
     * @param cuuid the cuuid of the request
     * @param magic the already read and unmarshalled magic number of the
     * request
     * @param reqtype the already read and unmarshalled request type
     * @param len the length of the yet to be read message body
     * @param msgBody the body of the message already read out as an array of
     * bytes.
     * @param socket the from which the request body should be read from
     */
    typedef void (VdqmRequestHandlerThread::*Handler) (Cuuid_t &cuuid,
       const uint32_t magic, const uint32_t reqtype, const uint32_t len,
       char *body,  castor::io::ServerSocket &socket);

    /**
     * A map from request type to handler function.
     */
    typedef std::map<uint32_t, Handler> HandlerMap;

    /**
     * A map from magic number to mape of request type to handler function.
     */
    typedef std::map<uint32_t, HandlerMap*> MagicToHandlersMap;

    /**
     * A map of maps for handling incoming requests.
     *
     * The outer map is from magic number to HandlerMap, and the inner
     * HandlerMap is from request type to handler function.
     */
    MagicToHandlersMap m_magicToHandlers;

    /**
     * Map from supported RTCOPY_MAGIC_OLD0 request types to their
     * corresponding handler functions.
     */
    HandlerMap m_rtcopyMagicOld0Handlers;

    /**
     * Queue of remote copy jobs to be worked on.
     *
     * This queue should only ever contain a maximum of 1 job.  The queue is
     * here in order to detect erronous clients.
     */
    castor::server::Queue m_jobQueue;

    /**
     * Dispatches the incoming request on the specified socket to the
     * appropriate request handler.
     *
     * @param cuuid The cuuid of the request
     * @param socket The socket
     */
    void dispatchRequest(Cuuid_t &cuuid, castor::io::ServerSocket &socket)
      throw(castor::exception::Exception);

    /**
     * Handles the submisison of a remote copy job from the VDQM.
     *
     * @param cuuid The cuuid of the request
     * @param magic The already read and unmarshalled magic number of the
     * request
     * @param reqtype The already read and unmarshalled request type
     * @param len The length of the yet to be read message body
     * @param msgBody the body of the message already read out as an array of
     * bytes.
     * @param socket the from which the request body should be read from
     */
    void handleJobSubmission(Cuuid_t &cuuid, const uint32_t magic,
      const uint32_t reqtype, const uint32_t len, char *body,
      castor::io::ServerSocket &socket) throw();

    /**
     * Marshalls the specified status code and possible error message into
     * the specified destination buffer in order to create an RTCP acknowledge
     * message.
     *
     * @param dst The destination message buffer
     * @param dstLen The length of the destination buffer
     * @param status The status code to be marshalled
     * @param errorMsg The error message to be marshalled
     * @return The total length of the message (header + body)
     */
    size_t marshallRTCPAckn(char *dst, const size_t dstLen,
      const uint32_t status, const char *errorMsg)
      throw (castor::exception::Exception);

  }; // class VdqmRequestHandlerThread

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP
