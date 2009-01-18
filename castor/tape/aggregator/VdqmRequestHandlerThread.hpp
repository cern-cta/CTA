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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP
#define CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Queue.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"

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
     * @param rtcpdListenPort The port on which the tape aggregator listens for
     * connections from RTCPD.
     */
    VdqmRequestHandlerThread(const int rtcpdListenPort) throw();

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
     * The port on which the tape aggregator listens for connections from
     * RTCPD.
     */
    const int m_rtcpdListenPort;

    /**
     * Pointer to handler function, where handler function is a member of
     * RequestHandler.
     *
     * @param cuuid The cuuid of the request.
     * @param header The already unmarshalled message header structure.
     * @param bodyBuf The buffer containing the message body which has not yet
     * been unmarshalled.
     * @param socket The from which the request body should be read from.
     */
    typedef void (VdqmRequestHandlerThread::*Handler) (Cuuid_t &cuuid,
       const MessageHeader &header, const char *bodyBuf,
       castor::io::ServerSocket &socket);

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
     * @param header The already unmarshalled message header structure.
     * @param bodyBuf The buffer containing the message body which has not yet
     * been unmarshalled.
     * @param socket the from which the request body should be read from
     */
    void handleJobSubmission(Cuuid_t &cuuid, const MessageHeader &header,
      const char *bodyBuf, castor::io::ServerSocket &socket) throw();

  }; // class VdqmRequestHandlerThread

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP
