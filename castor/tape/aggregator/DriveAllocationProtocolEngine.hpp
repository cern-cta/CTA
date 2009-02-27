/******************************************************************************
 *            castor/tape/aggregator/DriveAllocationProtocolEngine.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_DRIVEALLOCARIONPROTOCOLENGINE
#define CASTOR_TAPE_AGGREGATOR_DRIVEALLOCARIONPROTOCOLENGINE

#include "castor/exception/Exception.hpp"
#include "castor/tape/fsm/StateMachine.hpp"
#include "h/Cuuid.h"


namespace castor     {
namespace tape       {
namespace aggregator {

  /**
   * Class responsible for the vdqm/aggregator/rtpcd/aggregator protocol when
   * a free drive is allocated to tape mount request.
   */
  class DriveAllocationProtocolEngine {
  public:

    /**
     * Constructor.
     * @param cuuid The ccuid to be used for logging.
     * @param rtcpdCallbackSocketFd The file descriptor of the listener socket
     * to be used to accept callback connections from RTCPD.
     * @param rtcpdInitialSocketFd The socket file descriptor of initial RTCPD
     * connection.
     */
    DriveAllocationProtocolEngine(const Cuuid_t &cuuid,
      const int rtcpdCallbackSocketFd, const int rtcpdInitialSocketFd)
      throw(castor::exception::Exception);

    /**
     * Temporary test routine to help determine the FSTN of the state machine.
     */
    void testFsm();


  private:

    /**
     * The ccuid to be used for logging.
     */
    const Cuuid_t &m_cuuid;

    /**
     * The file descriptor of the listener socket to be used to accept callback
     * connections from RTCPD.
     */
    const int m_rtcpdCallbackSocketFd;

    /**
     * The socket file descriptor of initial RTCPD connection.
     */
    const int m_rtcpdInitialSocketFd;

    /**
     * State machine responsible for controlling the dynamic behaviour of this
     * protocol engine.
     */
    fsm::StateMachine m_fsm;

    const char *getVolFromRtcpd();

    const char *getReqFromRtcpd();

    const char *error();

    const char *closeRtcpdConAndError();
  };

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_DRIVEALLOCARIONPROTOCOLENGINE
