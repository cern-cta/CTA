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
#include "castor/tape/aggregator/RcpJobRequestMessage.hpp"

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
     * Queue of remote copy jobs to be worked on.
     *
     * This queue should only ever contain a maximum of 1 job.  The queue is
     * here in order to detect erronous clients.
     */
    castor::server::Queue m_jobQueue;

    /**
     * Processes a job submission request message from the specified
     * connection.
     *
     * If the processing of the request is successfull then this function will
     * have passed the request onto RTCPD and will have given as output the
     * contents of the job submission request and the file descriptor of the
     * listener socket which will be used to accept callback connections from
     * RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param vdqmSocketFd The socket file descriptor of the connection from
     * which the request is to be processed.
     * @param jobRequest Out parameter.  The job request message structure
     * which will be filled by this function.
     * @rtcpdCallbackSocketFd The file descriptor of the listener socket which
     * will be used to accept callback connections from RTCPD.
     */
    void processJobSubmissionRequest(const Cuuid_t &cuuid,
      const int vdqmSocketFd, RcpJobRequestMessage &jobRequest,
      int &rtcpdCallbackSocketFd) throw(castor::exception::Exception);

    /**
     * Throws an exception if the peer host associated with the specified
     * socket is not an authorised RCP job submitter.
     *
     * @param socketFd The socket file descriptor.
     */
    void checkRcpJobSubmitterIsAuthorised(const int socketFd)
      throw(castor::exception::Exception);

  }; // class VdqmRequestHandlerThread

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLERTHREAD_HPP
