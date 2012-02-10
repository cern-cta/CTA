/******************************************************************************
 *                castor/tape/tapebridge/VdqmRequestHandler.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLER_HPP
#define CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLER_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/tape/tapebridge/BulkRequestConfigParams.hpp"
#include "castor/tape/tapebridge/Counter.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/BoolFunctor.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "h/Cuuid.h"

#include <list>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Handles the requests from the VDQM server.
 */
class VdqmRequestHandler : public castor::server::IThread {

public:

  /**
   * Constructor
   *
   * @param bulkRequestConfigParams The values of the bulk-request
   *                                configuration-parameters to be used by the
   *                                tapebridged daemon.
   * @param tapeFlushConfigParams   The values of the tape-flush
   *                                configuration-parameters to be used by the
   *                                tapebridged daemon.
   * @param nbDrives                The number of tape drives attached to the
   *                                tape server on which the tapebridged daemon
   *                                is running.
   */
  VdqmRequestHandler(
    const BulkRequestConfigParams &bulkRequestConfigParams,
    const TapeFlushConfigParams   &tapeFlushConfigParams,
    const uint32_t                nbDrives) throw();

  /**
   * Destructor
   */
  ~VdqmRequestHandler() throw();

  /**
   * Initialization of the thread.
   */
  virtual void init() throw();

  /**
   * The entry point of this thread.
   *
   * @param param Parameter passed in by the CASTOR framework which should be
   *              the VDQM connection socket.
   */
  virtual void run(void *param) throw();

  /**
   * Convenience method to stop the thread.
   */
  virtual void stop() throw();

private:

  /**
   * The values of the bulk-request configuration-parameters to be used by the
   * tapebridged daemon.
   */
  const BulkRequestConfigParams m_bulkRequestConfigParams;

  /**
   * The values of the tape-flush configuration-parameters to be used by the
   * tapebridged daemon.
   */
  const TapeFlushConfigParams m_tapeFlushConfigParams;

  /**
   * The number of tape drives that were attached to the tape server just
   * before the VdqmRequestHandlerPool thread-pool was created.
   *
   * This constant will be set by the main thread and will be read by one or
   * more threads which may or may not include the main thread.
   *
   * This constant is used to log an error message when a tape operator adds
   * a set of new drive entires into TPCONFIG without restarting tapebridged.
   */
  const uint32_t m_nbDrives;

  /**
   * True if the daemon is stopping gracefully.
   */
  static bool s_stoppingGracefully;

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  class StoppingGracefullyFunctor : public utils::BoolFunctor {
  public:

    /**
     * Returns true if the daemon is stopping gracefully.
     */
    bool operator()() const {
      return s_stoppingGracefully;
    }
  };

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  static StoppingGracefullyFunctor s_stoppingGracefullyFunctor;

  /**
   * The entry point of this thread delegates its work to this method with a
   * try and catch around the call so that we can throw exceptions.
   *
   * @param cuuid                The ccuid to be used for logging.
   * @param jobRequest           The RTCOPY job request from the VDQM.
   * @param tapebridgeTransactionCounter The counter used to generate
   *                             tapebridge transaction IDs.  These are the IDs
   *                             used in requests to the clients.
   * @param bridgeCallbackSockFd The file descriptor of the listener socket to
   *                             be used to accept callback connections from
   *                             RTCPD.
   */
  void exceptionThrowingRun(
    const Cuuid_t                       &cuuid,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    Counter<uint64_t>                   &tapebridgeTransactionCounter,
    const int                           bridgeCallbackSockFd)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if the peer host associated with the specified
   * socket is not an authorised RCP job submitter.
   *
   * @param socketFd The socket file descriptor.
   */
  static void checkRtcpJobSubmitterIsAuthorised(const int socketFd)
    throw(castor::exception::Exception);

  /**
   * Enters the thread into either bridge or tapebridge mode.
   *
   * @param cuuid                    The ccuid to be used for logging.
   * @param rtcpdCallbackSockFd      The file descriptor of the listener socket
   *                                 to be used to accept callback connections
   *                                 from RTCPD.
   * @param rtcpdInitialSockFd       The socket file descriptor of initial
   *                                 RTCPD connection.
   * @param jobRequest               The RTCOPY job request from the VDQM.
   * @param volume                   The volume message received from the
   *                                 client.
   * @param nbFilesOnDestinationTape If migrating and the client is the tape
   *                                 gateay, then this must be set to the
   *                                 current number of files on the tape, else
   *                                 this parameter is ignored.
   * @param stoppingGracefully       Functor that returns true if the daemon is
   *                                 stopping gracefully.
   * @param tapebridgeTransactionCounter The counter used to generate
   *                                 tapebridge transaction IDs.  These are the
   *                                 IDs used in requests to the clients.
   */
  void enterBridgeOrTapeBridgeMode(
    const Cuuid_t                       &cuuid,
    const int                           rtcpdCallbackSockFd,
    const int                           rtcpdInitialSockFd,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    tapegateway::Volume                 &volume,
    const uint32_t                      nbFilesOnDestinationTape,
    utils::BoolFunctor                  &stoppingGracefully,
    Counter<uint64_t>                   &tapebridgeTransactionCounter)
  throw(castor::exception::Exception);
}; // class VdqmRequestHandler

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLER_HPP
