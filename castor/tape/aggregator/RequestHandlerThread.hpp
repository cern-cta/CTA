/******************************************************************************
 *                castor/tape/aggregator/RequestHandlerThread.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_REQUESTHANDLERTHREAD_HPP
#define CASTOR_TAPE_AGGREGATOR_REQUESTHANDLERTHREAD_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Queue.hpp"

#include <map>


namespace castor {
namespace tape {
namespace aggregator {

  /**
   * Handles the requests of the VDQM server's client.
   */
  class RequestHandlerThread : public castor::server::IThread {

  public:

    /**
     * Constructor
     */
    RequestHandlerThread() throw();

    /**
     * Destructor
     */
    ~RequestHandlerThread() throw();

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
     * Pointer to handler function, where handler function is a member of
     * RequestHandler.
     */
    typedef void (RequestHandlerThread::*Handler)
      (Cuuid_t &cuuid, castor::io::ServerSocket &socket);

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
     */
    void handleJobSubmission(Cuuid_t &cuuid, castor::io::ServerSocket &socket)
      throw();

  }; // class RequestHandlerThread

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_REQUESTHANDLERTHREAD_HPP
