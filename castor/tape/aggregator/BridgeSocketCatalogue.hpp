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
 * <li>One or more tape gateway connection sockets.
 * </ul>
 *
 * The BridgeSocketCatalogue is responsible for cataloguing the above sockets.
 * The BridgeSocketCatalogue behaves like a smart pointer in that its
 * destructor will close the rtcpd and tape gateway connections.  The
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
     * The socket-descriptor of the listen socket used to listen for and accept
     * RTCPD callback connections.
     */
    CALLBACK,

    /**
     * The socket-descriptor of an RTCPD connection socket.
     */
    RTCPD,

    /**
     * The socket-descriptor of a tape gateway connection socket.
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
   * does not however close the listen socket used to accept rtcpd connections.
   */
  ~BridgeSocketCatalogue();

  /**
   * Enters the specified socket-descriptor into the catalogue as that of the
   * single listen socket used to listen for an accept RTCPD callback
   * connections.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the descriptor of the listen socket
   * has already been entered into the catalogue.
   *
   * @param l The socket-descriptor of the listen socket.
   */
  void enterCallbackSocket(const int callback)
    throw(castor::exception::Exception);

  /**
   * Adds the specified socket-descriptor to the catalogue as that of an rtcpd
   * connection.
   *
   * This method throws an exception if the specifed socket-descriptor is
   * negative.
   *
   * This method throws an exception if the specified socket-descriptor has
   * already been added to the catalogue as that of an rtcpd connection.
   *
   * @param rtcpd The socket-descriptor of the rtcpd connection.
   */
  void addRtcpdConnectionSocket(const int rtcpd)
    throw(castor::exception::Exception);

  /**
   * Releases the specified rtcpd socket-descriptor from the catalogue together
   * with its associated tape gateway socket-descriptor.  After the release the
   * destructor of the catalogue will not close either of the connections.
   *
   * This method throws an exception if the specifed rtpd socket-descriptor is
   * negative.
   *
   * This method throws an exception if the specified socket-descriptor does
   * not exist in the catalogue as an rtcpd socket-descriptor.
   *
   * @param rtpcd The socket-descriptor of the rtcpd connection.
   * @return      The socket-descriptor of the associated tape gateway
   *              connection or -1 if there isn't one.
   */
  int releaseRtcpdConnectionSocket(const int rtcpd)
    throw(castor::exception::Exception);

  /**
   * Associates the specified tape gateway socket-descriptor with that of the
   * specified rtpcd connection.  The association means the specified rtcpd
   * connection is awaiting a reply from the specified tape gateway
   * connection.
   *
   * This method throws an exception if either the socket-descriptor of the
   * rtcpd connection or that of the tape gateway connection is negative.
   *
   * This method throws an exception if the specified socket-descriptor of
   * the rtcpd connection is not known to the catalogue.  The socket
   * descriptor of an rtcpd socket must be added to the catalogue before an
   * associated tape gateway connection can be added.
   *
   * This method throws an exception if the specified rtcpd / tape gateway
   * association already exists in the catalogue.
   *
   * @param rtcpd   The socket-descriptor of the rtcpd connection which is
   *                awaiting a reply from the tape gateway connection.
   * @param gateway The socket-descriptor of the tape gateway connection from
   *                a reply is expected.
   */
  void associateGatewayConnectionSocket(const int rtcpd, const int gateway)
    throw(castor::exception::Exception);

  /**
   * Unassociates the specified tape gateway socket-descriptor from the
   * specified rtcpd socket-descriptor and releases it from the catalogue.  The
   * removal of the association means the specified rtcpd connection is no 
   * longer awaiting a reply from the specified tape gateway connection.  The
   * fact that the tape gateway socket-descriptor is released means that it
   * will not be closed by the destructor of the catalogue.
   *
   * This method throws an exception if either the specified socket-descriptor
   * of the rtcpd connection or that of the tape gateway connection is negative.
   *
   * This method throws an exception if the specified rtcpd / tape gateway
   * association does not exist in the catalogue.
   *
   * @param rtcpd   The socket-descriptor of the rtcpd connection.
   * @param gateway The socket-descriptor of the tape gateway connection.
   */
  void releaseGatewayConnectionSocket(const int rtcpd, const int gateway)
    throw(castor::exception::Exception);

  /**
   * Returns the tape gateway socket-descriptor associated with specified rtcpd
   * socket-descriptor.
   *
   * This method throws an exception if the specified socket-descriptor of the
   * rtcpd connection is negative.
   *
   * This method throws an exception if the specified rtcpd socket-descriptor
   * does not exist in the catalogue.
   *
   * This method throws an exception if there is no associated tape gateway
   * socket-descriptor in the catalogue.
   *
   * @param rtcpd The socket-descriptor of the rtcpd connection.
   * @return      The associated tape-gateway socket-descriptor.
   */
  int getAssociatedGatewayConnectionSocket(const int rtcpd)
    throw(castor::exception::Exception);

  /**
   * Builds the set of file descriptors to checked for  being  ready to read.
   *
   * @param readFdSet Output parameter: The built set of file descriptors.
   * @param maxFd     Output parameter: The maximum of all of the file
   *                  descriptors in the set of file descriptors.
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
  int m_callback;

  /**
   * The socket-descriptor of an rtcpd connection together with the socket
   * descriptor of a tape gateway connection which is expected to send a
   * reply requested by the rtcpd connection.  If no reply is expected then
   * the value of the tape gateway connection socket-descriptor will be -1.
   */
  struct RtcpdGateway {
    int rtcpd;
    int gateway;

    RtcpdGateway(const int r, const int g) : rtcpd(r), gateway(g) {
    }
  };

  /**
   * List of rtcpd connection socket-descriptors together with their associated
   * tape gateway connection socket-descriptors from which they expect replies.
   */
  std::list<RtcpdGateway> m_rtcpdGateways;

}; // class BridgeSocketCatalogue

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_BRIDGESOCKETCATALOGUE
