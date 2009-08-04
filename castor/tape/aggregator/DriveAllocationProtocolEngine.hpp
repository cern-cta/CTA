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

#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractTCPSocket.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/SmartFd.hpp"
#include "castor/tape/fsm/StateMachine.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Castor_limits.h"
#include "h/common.h"
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
   * Execute the drive allocation protocol which will result in the volume
   * information being received from the tape gateway.
   *
   * This method will send an EndNotificationErrorReport message to the tape
   * gateway if an error is detected.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param vdqmSock            The socket of the VDQM connection.
   * @param rtcpdCallbackSockFd The file descriptor of the listener socket to
   *                            be used to accept callback connections from
   *                            RTCPD.
   * @param rtcpdCallbackHost   The host name of the listener socket to be used
   *                            to accept callback connections from RTCPD.
   * @param rtcpdCallbackPort   The port number of the listener socket to be
   *                            used to accept callback connections from RTCPD.
   * @param rtcpdInitialSockFd  Out parameter: The socket file descriptor of
   *                            the initial RTCPD connection.
   * @param jobRequest          The RTCOPY job requext from the VDQM.
   * @param volume              Out parameter: The volume message received from
   *                            the tape gateway.
   * @return                    True if there is a volume to mount.
   */
  bool run(
    const Cuuid_t                 &cuuid,
    castor::io::AbstractTCPSocket &vdqmSock,
    const int                     rtcpdCallbackSockFd,
    const char                    *rtcpdCallbackHost,
    const unsigned short          rtcpdCallbackPort,
    SmartFd                       &rtcpdInitialSockFd,
    const RcpJobRqstMsgBody       &jobRequest,
    tapegateway::Volume           &volume)
    throw(castor::exception::Exception);

  /**
   * Temporary test routine to help determine the FSTN of the state machine.
   */
  void testFsm();


private:

  /**
   * State machine responsible for controlling the dynamic behaviour of this
   * protocol engine.
   */
  fsm::StateMachine m_fsm;

  const char *getReqFromRtcpd();

  const char *getVolFromTGate();

  const char *error();
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_DRIVEALLOCARIONPROTOCOLENGINE
