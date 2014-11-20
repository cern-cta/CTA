/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/io/io.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/RmcMountMsgBody.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/legacymsg/RmcUnmountMsgBody.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/rmc_constants.h"

#include <unistd.h>
#include <sys/types.h>

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the rmc daemon.
 */
class RmcProxyTcpIp: public RmcProxy {
public:

  /**
   * Constructor.
   *
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  RmcProxyTcpIp(const unsigned short rmcPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~RmcProxyTcpIp() throw();

  /**
   * Requests the media changer to mount the specified tape for read-only
   * access into the drive in the specified library slot.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support read-only mounts.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadOnly(const std::string &vid,
    const mediachanger::ScsiLibrarySlot &librarySlot);

  /**
   * Requests the media changer to mount of the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid,
    const mediachanger::ScsiLibrarySlot &librarySlot);

  /** 
   * Requests the media changer to dismount of the specified tape from the
   * drive in the specifed library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void dismountTape(const std::string &vid,
    const mediachanger::ScsiLibrarySlot &librarySlot);

  /**
   * Requests the media changer to forcefully dismount the specified tape from
   * the drive in the specifed library slot.  Forcefully means rewinding and
   * ejecting the tape where necessary.
   *
   * Please note that this method provides a best-effort service because not all
   * media changers support forceful dismounts.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void forceDismountTape(const std::string &vid,
    const mediachanger::ScsiLibrarySlot &librarySlot);

protected:

  /**
   * The size of buffer used to marshal or unmarshal RMC messages.
   */
  static const int RMC_MSGBUFSIZ = 256;

  /**
   * The maximum number of attempts a retriable RMC request should be issued.
   */
  static const int RMC_MAXATTEMPTS = 3;

  /**
   * The TCP/IP port on which the rmcd daemon is listening.
   */
  const unsigned short m_rmcPort;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Connects to the rmcd daemon.
   *
   * @param rmcHost The name of the host on which the rmcd daemon is running.
   * @return The socket-descriptor of the connection with the rmcd daemon.
   */
  int connectToRmc(const std::string &rmcHost) const ;

  /**
   * Writes an RMC_SCSI_MOUNT message with the specifed body to the specified
   * connection.
   *
   * @param fd The file descriptor of the connection.
   * @param body The body of the message.
   */
  void writeRmcMountMsg(const int fd, const RmcMountMsgBody &body) ;

  /**
   * Reads the header of an RMC_MAGIC message from the specified connection.
   *
   * @param fd The file descriptor of the connection.
   * @return The message header.
   */
  MessageHeader readRmcMsgHeader(const int fd) ;

  /**
   * Writes an RMC_SCSI_UNMOUNT message with the specifed body to the specified
   * connection.
   *
   * @param fd The file descriptor of the connection.
   * @param body The body of the message.
   */
  void writeRmcUnmountMsg(const int fd, const RmcUnmountMsgBody &body) ;

  /**
   * Sends the specified request to the rmcd daemon and receives the reply
   * until success or the specified number of retriable attempts has been
   * reached.
   *
   * @param maxAttempts The maximum number of retriable attempts.
   * @param rmcHost The name of the host on which the rmcd daemon is running.
   * @param rqstBody The request to be sent.
   */
  template<typename T> void rmcSendRecvNbAttempts(const int maxAttempts,
    const std::string &rmcHost, const T &rqstBody) {
    for(int attemptNb = 1; attemptNb <= maxAttempts; attemptNb++) {
      std::ostringstream rmcErrorStream;
      const int rmcRc = rmcSendRecv(rmcHost, rqstBody, rmcErrorStream);
      switch(rmcRc) {
      case 0: // Success
        return;
      case ERMCFASTR: // Fast retry
        // If this was the last attempt
        if(maxAttempts == attemptNb) {
          castor::exception::Exception ex;
          ex.getMessage() <<
            "Received error from rmcd after several fast retries" <<
            ": nbAttempts=" << attemptNb << " rmcErrorStream=" <<
            rmcErrorStream.str();
          throw ex;
        }

        // Pause a moment between attempts
        sleep(1);

        continue;
      default:
        {
          castor::exception::Exception ex;
          ex.getMessage() << "Received error from rmcd: rmcRc=" << rmcRc <<
            " rmcRcStr=" << sstrerror(rmcRc);
          if(!rmcErrorStream.str().empty()) {
            ex.getMessage() << " rmcErrorStream=" << rmcErrorStream.str();
          }
          throw ex;
        }
      }
    }
  }

  /**
   * Sends the specified request to the rmcd daemon and receives the reply.
   *
   * @param rmcHost The name of the host on which the rmcd daemon is running.
   * @param rqstBody The request to be sent.
   * @param rmcErrorStream The error stream constructed from ERR_MSG received
   * within the reply from the rmcd daemon.
   * @param The RMC return code.
   */
  template<typename T> int rmcSendRecv(const std::string &rmcHost,
    const T &rqstBody, std::ostringstream &rmcErrorStream) {
    // Connect to rmcd and send request
    castor::utils::SmartFd fd(connectToRmc(rmcHost));
    {
      char buf[RMC_MSGBUFSIZ];
      const size_t len = marshal(buf, rqstBody);
      io::writeBytes(fd.get(), m_netTimeout, len, buf);
    }

    // A single RMC reply is composed of 0 to 10 ERR_MSG replies
    // followed by a terminating RMC_RC reply
    const int maxERR_MSG = 10;
    int nbERR_MSG = 0;
    while(true) {
      const MessageHeader header = readRmcMsgHeader(fd.get());
      switch(header.reqType) { 
      case RMC_RC:
        return header.lenOrStatus;
      case MSG_ERR:
        nbERR_MSG++;

        if(maxERR_MSG < nbERR_MSG) {
          castor::exception::Exception ex;
          ex.getMessage() <<
            "Reply from rmcd contains too many ERR_MSG messages"
            ": maxERR_MSG=" << maxERR_MSG << " rmcErrorStream=" <<
            rmcErrorStream;
          throw ex;
        }

        if(nbERR_MSG > 1) {
          rmcErrorStream << " ";
        }

        rmcErrorStream << handleMSG_ERR(header, fd.get());
        break;
      default:
        {
          castor::exception::Exception ex;
          ex.getMessage() <<
            "First part of reply from rmcd has unexpected type"
            ": expected=RMC_RC or MSG_ERR actual=" <<
            rmcReplyTypeToStr(header.reqType);
          throw ex;
        }
      } // switch(header.reqType)
    } // while(true)
  }

  /**
   * Returns a string representation of the specified RMC reply type.
   *
   * @param replyType The reply type.
   * @return The string representation.
   */
  std::string rmcReplyTypeToStr(const int replyType);

  /**
   * Handles a MSG_ERR reply from rmcd.
   *
   * @param header The header of the reply.
   * @param fd The file descriptor of the connection with rmcd daemon.
   * @return The message contained within the MSG_ERR reply.
   */
  std::string handleMSG_ERR(const MessageHeader &header, const int fd);

}; // class RmcProxyTcpIp

} // namespace legacymsg
} // namespace castor
