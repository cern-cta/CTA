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

#ifndef CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE_HPP
#define CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoValue.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/tape/tapebridge/GetMoreWorkConnection.hpp"
#include "castor/tape/tapebridge/IFileCloser.hpp"
#include "castor/tape/tapebridge/MigrationReportConnection.hpp"
#include "h/Castor_limits.h"

#include <list>
#include <set>
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
 * The BridgeSocketCatalogue behaves like a smart pointer for the initital
 * rtcpd-connection, the rtcpd disk/tape IO control-connections and the client
 * connections.  This means the destructor of the BridgeSocketCatalogue will
 * close them if they are still open.
 *
 * The BridgeSocketCatalogue will not close the listen socket used to accept
 * rtcpd connections.  This is the responsibility of the VdqmRequestHandler.
 */
class BridgeSocketCatalogue {
public:

  /**
   * Constructor.
   *
   * @param fileCloser The object used to close file-descriptors.  The main
   *                   goal of this object to facilitate in unit-testing the
   *                   BridgeSocketCatalogue.
   */
  BridgeSocketCatalogue(IFileCloser &fileCloser);

  /**
   * Destructor.
   *
   * Besides freeing the resources used by this object, this destructor also
   * closes the initial rtcpd-connection, the rtcpd disk/tape IO
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
   * This method throws a castor::exception::InvalidArgument exception if the
   * specified socket-descriptor is negative or equal to or greater than
   * FD_SETSIZE.
   *
   * This method throws an exception if the socket-descriptor of the listen
   * socket has already been entered into the catalogue.
   *
   * @param listenSock The socket-descriptor of the listen socket.
   */
  void addListenSock(const int listenSock)
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

  /**
   * Adds to the catalogue the initial rtpcd-connection.
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * specified socket-descriptor is negative or equal to or greater than
   * FD_SETSIZE.
   *
   * This method throws an exception if the socket-descriptor of the initial
   * rtcpd connection has already been entered into the catalogue.
   *
   * @param initialRtcpdSock The socket-descriptor of the initial rtcpd
   *                         connection.
   */
  void addInitialRtcpdConn(const int initialRtcpdSock)
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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
   * This method throws a castor::exception::InvalidArgument exception if the
   * specified socket-descriptor is negative or equal to or greater than
   * FD_SETSIZE.
   *
   * This method throws an exception if the specified socket-descriptor has
   * already been added to the catalogue as that of an rtcpd disk or tape IO
   * control-connection.
   *
   * @param rtcpdSock The socket-descriptor of the rtcpd disk/tape IO
   *                  control-connection.
   */
  void addRtcpdDiskTapeIOControlConn(const int rtcpdSock)
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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
   * The specified rtcpd disk/tape IO control-connection is removed from the
   * set of all rtcpd connections.
   *
   * If the specified rtcpd disk/tape IO control-connection is associated with
   * the client "get more work" connection then the rtcpd socket
   * file-descriptor of the association will be set to -1 to indicate that it
   * has been released from the socket-catalogue.
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * specified socket-descriptor is negative.
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
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

  /**
   * Sets the socket-descriptor of the pending client "get more work"
   * connection.
   *
   * A pending client "get more work" connection is a connection that has been
   * made with the client and then had a FilesToMigrateListRequest or a
   * FilesToRecallListRequest sent over it.
   *
   * There can only be one pending client "get more work" connection at any
   * single moment in time.  The rtcpd daemon will only ask for the next file
   * to work on after it has first received the reply to its current request.
   *
   * This method also associates the specified client connection with that of
   * the specified rtcpd disk/tape IO control-connection.  The association
   * means the rtcpd control-connection is awaiting a reply from the client
   * connection.
   *
   * This method throws a castor::exception::InvalidArgument exception if
   * either of the specified socket-descriptors are negative or are equal to
   * or greater than FD_SETSIZE.
   *
   * This method throws a castor::exception::Exception exception if both of the
   * socket-descriptors are positive but the rtcpd disk/tape IO
   * control-connection is not known to the catalogue.
   *
   * This method throws an exception if the "get more work" connection has
   * already been set and not yet released.
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
  void addGetMoreWorkConnection(
    const int         rtcpdSock,
    const uint32_t    rtcpdReqMagic,
    const uint32_t    rtcpdReqType,
    const std::string &rtcpdReqTapePath,
    const int         clientSock,
    const uint64_t    aggregatorTransactionId)
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

  /**
   * This method returns true if the socket-descriptor of the pending client
   * "get more work" connection is set, else this method returns false.
   *
   * A pending client "get more work" connection is a connection that has been
   * made with the client and then had a FilesToMigrateListRequest or a
   * FilesToRecallListRequest sent over it.
   */
  bool getMoreWorkConnectionIsSet() const;

  /**
   * Gets the all the information stored in the catalgue about the "get more
   * work" connection with the client including the rtcpd disk/tape IO control
   * connection that initiated its construction.
   *
   * This method throws a castor::exception::NoValue exception if the client
   * "get more work" connection is not set
   *
   * @return The information about the "get more work" connection.
   */
  const GetMoreWorkConnection &getGetMoreWorkConnection() const
    throw(castor::exception::NoValue, castor::exception::Exception);

  /**
   * Returns and releases the socket-descriptor of the pending client
   * "get more work" connection.
   *
   * A pending client "get more work" connection is a connection that has been
   * made with the client and then had a FilesToMigrateListRequest or a
   * FilesToRecallListRequest sent over it.
   *
   * This method throws an exception if the socket-descriptor of the pending
   * client "get more work" connection is not set.
   */
  int releaseGetMoreWorkClientSock()
    throw(castor::exception::Exception);

  /**
   * Builds the set of file descriptors to be passed to select() to see if any
   * of them are ready to read.
   *
   * This method throws a castor::exception::NoValue exception if the
   * socket catalogue does not contain any file-descriptors to build the set.
   * If this exception is throw then the values of readFdSet and maxFd will be
   * non-deterministic and should therefore be discarded by the caller.
   *
   * @param readFdSet Output parameter: The built set of file descriptors.
   * @param maxFd     Output parameter: The maximum value of all of the file
   *                  descriptors in the set.
   */
  void buildReadFdSet(fd_set &readFdSet, int &maxFd) const
    throw(castor::exception::NoValue, castor::exception::Exception);

  /**
   * Enumeration of the different types of socket-descriptor that can be stored
   * in the catalogue.
   */
  enum SocketType {

    /**
     * Unknown socket type.
     */
    UNKNOWN,

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
     * A pending client "get more work" connection.  This is a connection that
     * has been made with the client and then had a FilesToMigrateListRequest
     * or a FilesToRecallListRequest sent over it.
     */
    CLIENT_GET_MORE_WORK,

    /**
     * A client migration-report connection.
     */
    CLIENT_MIGRATION_REPORT

  }; //enum SocketType

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
   * Returns the total number of disk/tape IO control-connections.
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
   * This method will throw a castor::exception::InvalidArgument exception if
   * the specified socket-descriptor is negative or equal to or greater than
   * FD_SETSIZE.
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
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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

protected:

  /**
   * If the specified file-descriptor is valid (=> 0), then this method inserts
   * file-descriptor into the specified descriptor-set and updates the
   * specified max file-descriptor accordingly.
   *
   * @return True if the file-descriptor was valid and therefore inserted into
   *         the set, else false.
   */
  bool ifValidInsertFdIntoSet(const int fd, fd_set &fdSet, int &maxFd) const
    throw();

private:

  /**
   * The object used to close file-descriptors.
   *
   * The main goal of this object to facilitate in unit-testing the
   * BridgeSocketCatalogue.
   */
  IFileCloser &m_fileCloser;

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
   * The set of the connections the rtcpd daemon has made with the tapebridged
   * daemon.
   *
   * The size of this set is equal to the total number of disk/tape IO
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
  std::set<int> m_rtcpdIOControlConns;

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
   * The pending client migration-report connection.
   */
  MigrationReportConnection m_migrationReportConnection;

  /**
   * The pending client "get more work" connection.
   */
  GetMoreWorkConnection m_getMoreWorkConnection;

}; // class BridgeSocketCatalogue

} // namespace tapebridge
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUE_HPP
