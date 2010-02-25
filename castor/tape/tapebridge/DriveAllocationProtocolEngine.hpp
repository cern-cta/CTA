/******************************************************************************
 *            castor/tape/tapebridge/DriveAllocationProtocolEngine.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_DRIVEALLOCARIONPROTOCOLENGINE
#define CASTOR_TAPE_TAPEBRIDGE_DRIVEALLOCARIONPROTOCOLENGINE

#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractTCPSocket.hpp"
#include "castor/tape/tapebridge/Counter.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Castor_limits.h"
#include "h/common.h"
#include "h/Cuuid.h"


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Class responsible for the vdqm/tapebridge/rtpcd/tapebridge protocol when
 * a free drive is allocated to tape mount request.
 */
class DriveAllocationProtocolEngine {
public:

  /**
   * Constructor.
   *
   * @param tapebridgeTransactionCounter The counter used to generate tapebridge
   *                                     transaction IDs.  These are the IDS
   *                                     used in requests to the clients.
   */
  DriveAllocationProtocolEngine(Counter<uint64_t> &tapebridgeTransactionCounter)
    throw();

  /**
   * Execute the drive allocation protocol which will result in the volume
   * information being received from the tape gateway.
   *
   * This method will send an EndNotificationErrorReport message to the tape
   * gateway if an error is detected.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param rtcpdCallbackSockFd The file descriptor of the listener socket to
   *                            be used to accept callback connections from
   *                            RTCPD.
   * @param rtcpdCallbackHost   The host name of the listener socket to be used
   *                            to accept callback connections from RTCPD.
   * @param rtcpdCallbackPort   The port number of the listener socket to be
   *                            used to accept callback connections from RTCPD.
   * @param rtcpdInitialSockFd  Out parameter: The socket file descriptor of
   *                            the initial RTCPD connection.
   * @param jobRequest          The RTCOPY job request from the VDQM.
   * @return                    A pointer to the volume message received from
   *                            the client or NULL if there is no volume to
   *                            mount.  The callee is responsible for
   *                            deallocating the message.
   */
  tapegateway::Volume *run(
    const Cuuid_t                       &cuuid,
    const int                           rtcpdCallbackSockFd,
    const char                          *rtcpdCallbackHost,
    const unsigned short                rtcpdCallbackPort,
    utils::SmartFd                      &rtcpdInitialSockFd,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest)
    throw(castor::exception::Exception);


private:

  /**
   * The counter used to generate the next tapebridge transaction ID to be used
   * in a request to the client.
   */
  Counter<uint64_t> &m_tapebridgeTransactionCounter;

};

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_DRIVEALLOCARIONPROTOCOLENGINE
