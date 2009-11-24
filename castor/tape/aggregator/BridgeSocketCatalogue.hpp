/******************************************************************************
 *                      castor/tape/aggregator/BridgeSocketCatalogue.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_BRIDGESOCKETCATALOGUE
#define CASTOR_TAPE_AGGREGATOR_BRIDGESOCKETCATALOGUE

#include "castor/exception/Exception.hpp"

#include <list>
#include <sys/select.h>

namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * The BridgeProtocolEngine is implemented as a single thread spinning in a
 * select loop which looks the status of the descriptors of the following
 * sockets:
 * <ul>
 * <li>The single listen socket used to accept rtcpd connections.
 * <li>One or more rtcpd connection sockets.
 * <li>One or more tape-gateway connection sockets.
 * </ul>
 *
 * The BridgeSocketCatalogue is responsible for cataloguing the above sockets.
 * The BridgeSocketCatalogue behaves like a smart pointer in that its
 * destructor will close the rtcpd and tape-gateway connections.  The
 * BridgeSocketCatalogue will not close the listen socket used to accept rtcpd
 * connections.
 */
class BridgeSocketCatalogue {
public:

  /**
   * Enumeration of the three different types of socket-descriptor that can
   * stored in the catalogue.
   */
  enum SocketType {
    /**
     * The listen socket used to listen for and accept RTCPD callback
     * connections.
     */
    LISTEN,

    /**
     * The initial rtcpd connection.
     */
    INITIAL_RTCPD,

    /**
     * An rtcpd disk/tape IO control-connection.
     */
    RTCPD_DISK_TAPE_IO_CONTROL,

    /**
     * A tape-gateway connection socket.
     */
    GATEWAY
  };

  /**
   * Constructor.
   */
  BridgeSocketCatalogue();

  /**
   * Destructor.
   *
   * Besides freeing the resources used by this object, this destructor also
   * closes all of the rtcpd and gateway connections in the catalogue.  This
   * destructor does not however close the listen socket used to accept rtcpd
   * connections.
   */
  ~BridgeSocketCatalogue();

  /**
   * Adds to the catalogue the listen socket used to listen for and accept
   * RTCPD callback connections.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the socket-descriptor of the listen
   * socket has already been entered into the catalogue.
   *
   * @param listenSock The socket-descriptor of the listen socket.
   */
  void addListenSocket(const int listenSock)
    throw(castor::exception::Exception);

  /**
   * Adds to the catalogue the initial rtpcd connection.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the socket-descriptor of the initial
   * rtcpd connection has already been entered into the catalogue.
   *
   * @param initialRtcpdSock The socket-descriptor of the initial rtcpd
   *                         connection.
   */
  void addInitialRtcpdSocket(const int initialRtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Adds to the catalogue an rtcpd disk/tape IO control-connection.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the specified socket-descriptor has
   * already been added to the catalogue as that of an rtcpd disk or tape IO
   * control-connection.
   *
   * @param rtcpdSock The socket-descriptor of the rtcpd disk/tape IO
   *                  control-connection.
   */
  void addRtcpdDiskTapeIOControlSocket(const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Associates the specified tape-gateway connection with that of the
   * specified rtcpd disk/tape IO control-connection.  The association means
   * the rtcpd control-connection is awaiting a reply from the tape-gateway
   * connection.
   *
   * This method throws an exception if either of the specified
   * socket-descriptors are negative.
   *
   * This method throws an exception if the specified socket-descriptor of
   * the rtcpd disk/tape IO control-connection is not known to the catalogue.
   *
   * This method throws an exception if the specified association already
   * exists in the catalogue.
   *
   * @param rtpcdSock    The socket-descriptor of the rtcpd disk/tape IO
   *                     control-connection that is awaiting a reply from the
   *                     specified tape-gateway connection.
   * @param gatewaySock  The socket-descriptor of the tape-gateway connection
   *                     from which a reply is expected.
   */
  void addGatewaySocket(const int rtcpdSock, const int gatewaySock)
    throw(castor::exception::Exception);

  /**
   * Releases the specified rtcpd disk/tape IO control-connection from the
   * catalogue.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the specified socket-descriptor does
   * not exist in the catalogue as an rtcpd disk/tape IO control-connection.
   *
   * This method throws an exception if the specified rtcpd disk/tape IO
   * control-connection has an associated tape-gateway connection.
   *
   * @param rtcpdSock The socket-descriptor of the rtcpd disk/tape IO
   *                  control-connection.
   * @return          The socket-descriptor of the released rtcpd disk/tape IO
   *                  control-connection.
   */
  int releaseRtcpdDiskTapeIOControlSocket(const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Unassociates the specified tape-gateway connection from the specified
   * rtcpd disk/tape IO control-connection and releases it from the catalogue.
   * The release means the destructor of the catalogue will no longer close the
   * tape-gateway connection.
   *
   * This method throws an exception if either of the specified
   * socket-descriptors are negative.
   *
   * This method throws an exception if the specified association does not
   * exist in the catalogue.
   *
   * @param rtcpdSock   The socket-descriptor of the rtcpd connection.
   * @param gatewaySock The socket-descriptor of the tape-gateway connection.
   * @return            The socket-descriptor of the released tape-gateway
   *                    connection.
   */
  int releaseGatewaySocket(const int rtcpdSock, const int gatewaySock)
    throw(castor::exception::Exception);

  /**
   * Returns the rtcpd disk/tape IO control-connection associated with the
   * specified tape-gateway connection.
   *
   * This method throws an exception if the specified socket-descriptor is
   * is negative.
   *
   * This method throws an exception if the specified tape-gateway connection
   * socket-descriptor does not exist in the catalogue.
   *
   * @param gatewaySock The socket-descriptor of the tape-gateway connection.
   * @return            The socket-descriptor of the rtcpd disk/tape IO
   *                    control-connection.
   */
  int getRtcpdSocket(const int gatewaySock) throw(castor::exception::Exception);

  /**
   * Builds the set of file descriptors to be passed to select() to see if any
   * of them are ready to read.
   *
   * @param readFdSet Output parameter: The built set of file descriptors.
   * @param maxFd     Output parameter: The maximum value of all of the file
   *                  descriptors in the set.
   */
  void buildReadFdSet(fd_set &readFdSet, int &maxFd) throw();

  /**
   * If the specified read file-descriptor set has one or more pending
   * socket-descriptors, then this method will return one of them, else it will
   * return -1.
   *
   * @param readFdSet The read file-descriptor set from calling select().
   * @param sockType  Output parameter: The type of the pending file-descriptor
   *                  if one was found, else undefined.
   * @return          A pending file-descriptor or -1 if none was found.
   */
  int getAPendingSocket(fd_set &readFdSet, SocketType &sockType) throw();


private:

  /**
   * The socket-descriptor of the listen socket used to listen for and accept
   * RTCPD callback connections, or -1 if it has not been set.
   */
  int m_listenSock;

  /**
   * The socket-descriptor of the initial rtcpd connection, or -1 if it has not
   * been set.
   */
  int m_initialRtcpdSock;

  /**
   * The socket-descriptor of an rtcpd connection together with the socket
   * descriptor of a tape-gateway connection which is expected to send a
   * reply requested by the rtcpd connection.  If no reply is expected then
   * the value of the tape-gateway connection socket-descriptor will be -1.
   */
  struct RtcpdGateway {
    int rtcpdSock;
    int gatewaySock;

    RtcpdGateway(const int r, const int g) : rtcpdSock(r), gatewaySock(g) {
    }
  };

  /**
   * Type used to define a list of rtcpd / tape-gateway socket associations.
   */
  typedef std::list<RtcpdGateway> RtcpdGatewayList;

  /**
   * List of rtcpd disk/tape IO control-connections together with their
   * associated tape-gateway connections.
   *
   * The size of this list is equal to the total number of disk/tape IO
   * control-connections.  This value is used to control the "good day"
   * shutdown sequence of an rtcpd session.
   *
   * The "good day" shutdown sequence of an rtcpd session is as follows:
   * <ol>
   * <li>For each rtcpd disk/tape IO control-connection, rtcpd sends an
   *     RTCP_ENDOF_REQ message which the aggregator acknowledges and closes
   *     its end of the connection.
   * <li>Immediately after the aggregator closes the last rtcpd disk/tape IO
   *     control-onnection (detected by the size of this this list reaching 0),
   *     the aggregator sends an RTCP_ENDOF_REQ message to rtcpd using the
   *     initial rtpcd connection.  The aggregator then receives an acknowledge
   *     from rtcpd and  closes its end of the initial callback connection.
   * <li>The rtcpd session is now successfully shutdown.
   * </ol>
   */
  RtcpdGatewayList m_rtcpdGatewaySockets;

}; // class BridgeSocketCatalogue

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_BRIDGESOCKETCATALOGUE
