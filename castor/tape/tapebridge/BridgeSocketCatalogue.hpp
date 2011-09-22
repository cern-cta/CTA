/******************************************************************************
 *                      castor/tape/tapebridge/BridgeSocketCatalogue.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE
#define CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE

#include "castor/exception/Exception.hpp"
#include "castor/exception/TimeOut.hpp"
#include "h/Castor_limits.h"

#include <list>
#include <stdint.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The BridgeProtocolEngine is implemented as a single thread spinning in a
 * select loop which looks the status of the descriptors of the following
 * sockets:
 * <ul>
 * <li>The single listen socket used to accept rtcpd connections.
 * <li>The initial rtcpd connection.
 * <li>The disk/tape IO control-connections.
 * <li>The client connections.
 * </ul>
 *
 * The BridgeSocketCatalogue is responsible for cataloguing the above sockets.
 *
 * The BridgeSocketCatalogue behaves like a smart pointer for the rtcpd
 * disk/tape IO control-connections and the client connections in that
 * its destructor will close them if they are still open.
 *
 * The BridgeSocketCatalogue will not close the listen socket used to accept
 * rtcpd connections.  This is the responsibility of the VdqmRequestHandler.
 *
 * The BridgeSocketCatalogue will not close the initial rtpcd connection.  This
 * is the responsibility of the VdqmRequestHandler.
 */
class BridgeSocketCatalogue {
public:

  /**
   * Constructor.
   */
  BridgeSocketCatalogue();

  /**
   * Destructor.
   *
   * Besides freeing the resources used by this object, this destructor also
   * closes the initila rtcpd connection, the rtcpd disk/tape IO
   * control-connections and the client connections that are still open.
   *
   * The destructor does not close the listen socket used to accept rtcpd
   * connections.  This is the responsibility of the VdqmRequestHandler.
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
  void addListenSock(const int listenSock)
    throw(castor::exception::Exception);

  /**
   * Adds to the catalogue the initial rtpcd-connection.
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
  void addInitialRtcpdConn(const int initialRtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Returns and releases the socket-descriptor of the iniial rtpcd-connection.
   *
   * This method throws an exception if the socket-descriptor of the initial
   * rtpcd-connection is not set.
   */
  int releaseInitialRtcpdConn() throw(castor::exception::Exception);

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
  void addRtcpdDiskTapeIOControlConn(const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Associates the specified client connection with that of the
   * specified rtcpd disk/tape IO control-connection.  The association means
   * the rtcpd control-connection is awaiting a reply from the client
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
   * @param rtpcdSock        The socket-descriptor of the rtcpd disk/tape IO
   *                         control-connection that is awaiting a reply from
   *                         the specified client connection.
   * @param rtcpdReqMagic    The magic number of the rtcpd request.  This magic
   *                         number will be used in any acknowledge message to
   *                         be sent to rtcpd.
   * @param rtpcdReqTapePath The tape path associated with the rtcpd request.
   *                         If there is no such tape path then this parameter
   *                         should be passed NULL.
   * @param rtcpdReqType     The request type of the rtcpd request. The request
   *                         type will be used in any acknowledge message to be
   *                         sent to rtcpd.
   * @param clientSock       The socket-descriptor of the client connection
   *                         from which a reply is expected.
   * @param aggregatorTransactionId The tapebridge transaction ID associated
   *                         with the client request.
   */
  void addClientConn(
    const int      rtcpdSock,
    const uint32_t rtcpdReqMagic,
    const uint32_t rtcpdReqType,
    const char     (&rtcpdReqTapePath)[CA_MAXPATHLEN+1],
    const int      clientSock,
    const uint64_t aggregatorTransactionId)
    throw(castor::exception::Exception);

  /**
   * Returns the socket-descriptor of the listen socket used to listen for and
   * accept rtpcd connections.
   *
   * This method throws an exception if the socket-descriptor of the listen
   * socket does not exist in the catalogue.
   */
  int getListenSock() const throw(castor::exception::Exception);

  /**
   * Returns the socket-descriptor of the initial rtcpd connection.
   *
   * This method throws an exception if the socket-descriptor of the initial
   * rtcpd connection does not exist in the catalogue.
   */
  int getInitialRtcpdConn() const throw(castor::exception::Exception);

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
   * control-connection has an associated client connection.
   *
   * @param rtcpdSock The socket-descriptor of the rtcpd disk/tape IO
   *                  control-connection.
   * @return          The socket-descriptor of the released rtcpd disk/tape IO
   *                  control-connection.
   */
  int releaseRtcpdDiskTapeIOControlConn(const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Unassociates the specified client connection from the specified
   * rtcpd disk/tape IO control-connection and releases it from the catalogue.
   * The release means the destructor of the catalogue will no longer close the
   * client connection.
   *
   * The status of the rtcpd disk/tape IO control-connection is not changed by
   * this method.
   *
   * This method throws an exception if either of the specified
   * socket-descriptors are negative.
   *
   * This method throws an exception if the specified association does not
   * exist in the catalogue.
   *
   * @param rtcpdSock  The socket-descriptor of the rtcpd connection.
   * @param clientSock The socket-descriptor of the client connection.
   * @return           The socket-descriptor of the released client
   *                   connection.
   */
  int releaseClientConn(const int rtcpdSock, const int clientSock)
    throw(castor::exception::Exception);

  /**
   * Gets the rtcpd disk/tape IO control-connection associated with the
   * specified client connection.
   *
   * This method throws an exception if the specified socket-descriptor is
   * is negative.
   *
   * This method throws an exception if the specified client connection
   * socket-descriptor does not exist in the catalogue.
   *
   * @param clientSock         The socket-descriptor of the client-connection.
   * @param rtcpdSock          Output parameter: The socket-descriptor of the
   *                           rtcpd disk/tape IO control-connection.
   * @param rtcpdReqMagic      Output parameter: The magic number of the
   *                           initiating rtcpd request.  This magic number
   *                           will be used in any acknowledge message to be
   *                           sent to rtcpd.
   * @param rtcpdReqType       Output parameter: The request type of the
   *                           initiating rtcpd request. This request type will
   *                           be used in any acknowledge message to be sent to
   *                           rtcpd.
   * @param rtcpdReqTapePath   Output parameter: The tape path associated with
   *                           the initiating rtcpd request.  If there is no
   *                           such tape path, then this parameter is set to
   *                           NULL.
   * @param aggregatorTransactionId Output parameter: The tapebridge
   *                           transaction ID associated with the request sent
   *                           to the client.
   * @param clientReqTimeStamp Output parameter: The time at which the request
   *                           was sent to the client.
   */
  void getRtcpdConn(
    const int             clientSock,
    int                   &rtcpdSock,
    uint32_t              &rtcpdReqMagic,
    uint32_t              &rtcpdReqType,
    const char            *&rtcpdReqTapePath,
    uint64_t              &aggregatorTransactionId,
    struct timeval        &clientReqTimeStamp) const
    throw(castor::exception::Exception);

  /**
   * Builds the set of file descriptors to be passed to select() to see if any
   * of them are ready to read.
   *
   * @param readFdSet Output parameter: The built set of file descriptors.
   * @param maxFd     Output parameter: The maximum value of all of the file
   *                  descriptors in the set.
   */
  void buildReadFdSet(fd_set &readFdSet, int &maxFd) const throw();

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
     * A client connection other than a migration-report connection.
     */
    CLIENT,

    /**
     * A client migration-report connection.
     */
    CLIENT_MIGRATION_REPORT
  };

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
  int getAPendingSock(fd_set &readFdSet, SocketType &sockType) const;

  /**
   * Returns the total number of disk/tape IO control-conenction.
   */
  int getNbDiskTapeIOControlConns() const;

  /**
   * Checks the socket catalogue to see if an rtcpd disk/tape IO
   * control-connections has timed out waiting for a reply from the client.
   *
   * This method throws a TimeOut exception if a timeout is found.
   */
  void checkForTimeout() const throw(castor::exception::TimeOut);

  /**
   * Sets the socket-descriptor of the pending client migration-report
   * connection.
   *
   * There should only be one client migration-report connection open at any
   * moment in time and therefore this method will throw an exception if the
   * socket-descriptor is already set.
   *
   * @param sock                    The socket-descriptor of the client
   *                                migration-report connection.
   * @param aggregatorTransactionId The tapebridge transaction id of the
   *                                migration-report message written to the
   *                                client migration-report connection.
   */
  void addClientMigrationReportSock(const int sock,
    const uint64_t aggregatorTransactionId)
    throw(castor::exception::Exception);

  /**
   * This method returns true if the socket-descriptor of the client
   * migration-report connection is set, else this method returns false.
   */
  bool clientMigrationReportSockIsSet() const;

  /**
   * Returns and releases the socket-descriptor of the client migration-report
   * connection.
   *
   * This method throws an exception if the socket-descriptor of the client
   * migration-report connection is not set.
   *
   * @param aggregatorTransactionId Output parameter: The tapebridge
   *                                transaction id of the migration-report
   *                                message written to the client
   *                                migration-report connection.
   */
  int releaseClientMigrationReportSock(uint64_t &aggregatorTransactionId)
    throw(castor::exception::Exception);

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
   * The information stored in the socket catalogue about an individual
   * rtcpd disk/tape IO control-connection.
   */
  struct RtcpdConnection {

    /**
     * The socket-descriptor of an rtcpd-connection.
     */
    const int rtcpdSock;

    /**
     * The magic number of the rtcpd request message awaiting a reply from the
     * client.  If there is no such rtcpd request message, then the value of
     * this member will be 0.
     *
     * The primary goal of this member is to enable the tapebridge to send
     * acknowledgement messages to rtcpd with the correct magic number, i.e.
     * the same magic number as the initiating rtcpd request message.
     */
    uint32_t rtcpdReqMagic;

    /**
     * Type of the type of the rtcpd message awaiting a reply from the client.
     * If there is no such rtcpd request message, then the value of this member
     * will be 0.
     *
     * The primary goal of this member is to enable the tapebridge to send
     * acknowledgement messages to rtcpd with the correct request type, i.e.
     * the same request type as the initiating rtcpd request message.
     */
    uint32_t rtcpdReqType;

    /**
     * The value of this memeber is true if the rtcpd request message awaiting
     * a reply has an associated tape path, else the value is false.
     */
    bool rtcpdReqHasTapePath;

    /**
     * The tape path associated with the rtcpd request message awaiting a reply
     * from the client.  If there is no such tape path, then the value of this
     * member is an empty string.
     *
     * Please note the rtcpdReqHasTapePath member should be used to determine
     * whether or not the rtcpd request message has an associated tape path.
     * The fact that this string is empty should not.
     */
    char rtcpdReqTapePath[CA_MAXPATHLEN+1];

    /**
     * The socket-descriptor of an associated client-connection.
     *
     * If the rtcpd-connection is waiting for a reply from the client, then
     * this the value of this member is the client-connection socket-descriptor,
     * else the value of this member is -1.
     */
    int clientSock;

    /**
     * If a request has been resent to the client, then this is the tapebridge
     * transaction ID associated with that request.  If no message has been
     * sent, then the value of this member is 0.
     */
    uint64_t aggregatorTransactionId;

    /**
     * If a request has been resent to the client, then this is the absolute
     * time when the request was sent.  If no message has been sent, then the
     * value of this member is all zeros.
     */
    struct timeval clientReqTimeStamp;

    /**
     * Constructor.
     *
     * @param rSock The socket-descriptor of the rtcpd disk/tape IO
     *              control-connection.
     */
    RtcpdConnection(const int rSock) :
      rtcpdSock(rSock),
      rtcpdReqMagic(0),
      rtcpdReqType(0),
      rtcpdReqHasTapePath(false),
      clientSock(-1),
      aggregatorTransactionId(0) {
      rtcpdReqTapePath[0] = '\0';
      clientReqTimeStamp.tv_sec  = 0;
      clientReqTimeStamp.tv_usec = 0;
    }
  };

  /**
   * Type used to define a list of rtcpd connections.
   */
  typedef std::list<RtcpdConnection> RtcpdConnectionList;

  /**
   * The list used to store information about the rtcpd disk/tape IO
   * contol-connections.
   *
   * The size of this list is equal to the total number of disk/tape IO
   * control-connections.  This value is used to control the "good day"
   * shutdown sequence of an rtcpd session.
   *
   * The "good day" shutdown sequence of an rtcpd session is as follows:
   * <ol>
   * <li>For each rtcpd disk/tape IO control-connection, rtcpd sends an
   *     RTCP_ENDOF_REQ message which the tapebridge acknowledges and closes
   *     its end of the connection.
   * <li>Immediately after the tapebridge closes the last rtcpd disk/tape IO
   *     control-onnection (detected by the size of this this list reaching 0),
   *     the tapebridge sends an RTCP_ENDOF_REQ message to rtcpd using the
   *     initial rtpcd connection.  The tapebridge then receives an acknowledge
   *     from rtcpd and  closes its end of the initial callback connection.
   * <li>The rtcpd session is now successfully shutdown.
   * </ol>
   */
  RtcpdConnectionList m_rtcpdConnections;

  /**
   * Information about a single client request to be used to create a history
   * about sent and pending client requests.
   */
  struct ClientReqHistoryElement {
    const int            clientSock;
    const struct timeval clientReqTimeStamp;

    /**
     * Constructor.
     */
    ClientReqHistoryElement(
      const int            cSock,
      const struct timeval cReqTimeStamp) :
      clientSock(cSock),
      clientReqTimeStamp(cReqTimeStamp) {
    }
  };

  /**
   * Type used to define a list of client request history elements.
   */
  typedef std::list<ClientReqHistoryElement> ClientReqHistoryList;

  /**
   * The history of pending client requests implemented as a time-ordered FIFO,
   * where the oldest request is at the front and the youngest at the back.
   *
   * This history is used to efficiently determine if an rtcpd-connection has
   * timed out waiting for a reply from a client.
   */
  ClientReqHistoryList m_clientReqHistory;

  /**
   * Structure representing a pending client migration-report connection.  This
   * is a connection that has been opened with the client and has had a
   * migration-report message written into it.
   */
  struct ClientMigrationReportConnection {

    /**
     * Constructor.
     */
    ClientMigrationReportConnection():
      clientSock(-1),
      aggregatorTransactionId(0) {
      // Do nothing
    }

    /**
     * The socket-descriptor of the client-connection if there is one, else -1
     * if there is not.
     */
    int clientSock;

    /**
     * The tapebridge transaction ID associated with the migration-report
     * message that was sent to the client.  This member of the structure
     * should be ignored if the clientSock member is set to -1.
     */
    uint64_t aggregatorTransactionId;
  };

  /**
   * The client migration-report connection.
   */
  ClientMigrationReportConnection m_clientMigrationReportConnection;

}; // class BridgeSocketCatalogue

} // namespace tapebridge
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE
