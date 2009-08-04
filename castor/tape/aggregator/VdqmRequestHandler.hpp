/******************************************************************************
 *                castor/tape/aggregator/VdqmRequestHandler.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLER_HPP
#define CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLER_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/tape/aggregator/BoolFunctor.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "h/Cuuid.h"

#include <list>


namespace castor {
namespace tape {
namespace aggregator {

/**
 * Handles the requests from the VDQM server.
 */
class VdqmRequestHandler : public castor::server::IThread {

public:

  /**
   * Constructor.
   */
  VdqmRequestHandler() throw();

  /**
   * Destructor
   */
  ~VdqmRequestHandler() throw();

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

  /**
   * Returns true if the daemon is stopping gracefully.
   */
  bool stoppingGracefully() throw();


private:

  /**
   * True if the daemon is stopping gracefully.
   */
  bool m_stoppingGracefully;

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  class StoppingGracefullyFunctor : public BoolFunctor {
  public:

    /**
     * Constructor.
     *
     * @param daemon The daemon on which this functor calls
     *               BaseDaemon::stoppingGracefully().
     */
    StoppingGracefullyFunctor(VdqmRequestHandler &vdqmRequestHandler) :
      m_vdqmRequestHandler(vdqmRequestHandler) {
      // Do nothing
    }

    /**
     * Returns true if the daemon is sotpping gracefully.
     */
    bool operator()() {
      return m_vdqmRequestHandler.stoppingGracefully();
    }

  private:

    /**
     * The daemon on which this functor calls BaseDaemon::stoppingGracefully().
     */
    VdqmRequestHandler &m_vdqmRequestHandler;
  };

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  StoppingGracefullyFunctor m_stoppingGracefullyFunctor;

  /**
   * Throws an exception if the peer host associated with the specified
   * socket is not an authorised RCP job submitter.
   *
   * @param socketFd The socket file descriptor.
   */
  void checkRcpJobSubmitterIsAuthorised(const int socketFd)
    throw(castor::exception::Exception);

}; // class VdqmRequestHandler

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_VDQMREQUESTHANDLER_HPP
