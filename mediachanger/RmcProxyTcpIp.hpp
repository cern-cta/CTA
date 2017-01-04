/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/SmartFd.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/io.hpp"
#include "mediachanger/MediaChangerProxy.hpp"
#include "mediachanger/MessageHeader.hpp"
#include "mediachanger/RmcMountMsgBody.hpp"
#include "mediachanger/RmcUnmountMsgBody.hpp"

#include <unistd.h>
#include <sys/types.h>

/*
 *------------------------------------------------------------------------
 * RMC (Remote SCSI media changer server) errors
 *------------------------------------------------------------------------
 */
#define ERMBASEOFF      2200            /* RMC error base offset        */
#define        ERMCNACT        ERMBASEOFF+1    /* Remote SCSI media changer server not active or service being drained */
#define        ERMCRBTERR      (ERMBASEOFF+2)  /* Remote SCSI media changer error */
#define        ERMCUNREC       ERMCRBTERR+1    /* Remote SCSI media changer unrec. error */
#define        ERMCSLOWR       ERMCRBTERR+2    /* Remote SCSI media changer error (slow retry) */
#define        ERMCFASTR       ERMCRBTERR+3    /* Remote SCSI media changer error (fast retry) */
#define        ERMCDFORCE      ERMCRBTERR+4    /* Remote SCSI media changer error (demount force) */
#define        ERMCDDOWN       ERMCRBTERR+5    /* Remote SCSI media changer error (drive down) */
#define        ERMCOMSGN       ERMCRBTERR+6    /* Remote SCSI media changer error (ops message) */
#define        ERMCOMSGS       ERMCRBTERR+7    /* Remote SCSI media changer error (ops message + retry) */
#define        ERMCOMSGR       ERMCRBTERR+8    /* Remote SCSI media changer error (ops message + wait) */
#define        ERMCUNLOAD      ERMCRBTERR+9    /* Remote SCSI media changer error (unload + demount) */
#define ERMMAXERR       ERMBASEOFF+11

namespace cta {
namespace mediachanger {

/**
 * A concrete implementation of the interface to the rmc daemon.
 */
class RmcProxyTcpIp: public MediaChangerProxy {
public:

  /**
   * Constructor.
   *
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   * @parm maxRqstAttempts The maximum number of attempts a retriable RMC
   * request should be issued.
   */
  RmcProxyTcpIp(
    const unsigned short rmcPort = RMC_PORT,
    const int netTimeout = RMC_NET_TIMEOUT,
    const unsigned int maxRqstAttempts = RMC_MAX_RQST_ATTEMPTS) throw();

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
  void mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) override;

  /**
   * Requests the media changer to mount of the specified tape for read/write
   * access into the drive in the specified library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) override;

  /** 
   * Requests the media changer to dismount of the specified tape from the
   * drive in the specifed library slot.
   *
   * @param vid The volume identifier of the tape.
   * @param librarySlot The library slot containing the tape drive.
   */
  void dismountTape(const std::string &vid, const LibrarySlot &librarySlot) override;

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
  void forceDismountTape(const std::string &vid, const LibrarySlot &librarySlot) override;

protected:

  /**
   * The size of buffer used to marshal or unmarshal RMC messages.
   */
  static const int RMC_MSGBUFSIZ = 256;

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
   * The maximum number of attempts a retriable RMC request should be issued.
   */
  const unsigned int m_maxRqstAttempts;

  /**
   * Connects to the rmcd daemon.
   *
   * Please note that the rmcd daemon only listens on loopback interface.
   *
   * @return The socket-descriptor of the connection with the rmcd daemon.
   */
  int connectToRmc() const ;

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
   * @param rqstBody The request to be sent.
   */
  template<typename T> void rmcSendRecvNbAttempts(const int maxAttempts,
    const T &rqstBody) {
    for(int attemptNb = 1; attemptNb <= maxAttempts; attemptNb++) {
      std::ostringstream rmcErrorStream;
      const int rmcRc = rmcSendRecv(rqstBody, rmcErrorStream);
      switch(rmcRc) {
      case 0: // Success
        return;
      case ERMCFASTR: // Fast retry
        // If this was the last attempt
        if(maxAttempts == attemptNb) {
          cta::exception::Exception ex;
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
          cta::exception::Exception ex;
          ex.getMessage() << "Received error from rmcd: rmcRc=" << rmcRc;
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
   * @param rqstBody The request to be sent.
   * @param rmcErrorStream The error stream constructed from ERR_MSG received
   * within the reply from the rmcd daemon.
   * @param The RMC return code.
   */
  template<typename T> int rmcSendRecv(const T &rqstBody,
    std::ostringstream &rmcErrorStream) {
    // Connect to rmcd and send request
    cta::SmartFd fd(connectToRmc());
    {
      char buf[RMC_MSGBUFSIZ];
      const size_t len = marshal(buf, rqstBody);
      writeBytes(fd.get(), m_netTimeout, len, buf);
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
          cta::exception::Exception ex;
          ex.getMessage() <<
            "Reply from rmcd contains too many ERR_MSG messages"
            ": maxERR_MSG=" << maxERR_MSG << " rmcErrorStream=" <<
            rmcErrorStream.str();
          throw ex;
        }

        if(nbERR_MSG > 1) {
          rmcErrorStream << " ";
        }

        rmcErrorStream << handleMSG_ERR(header, fd.get());
        break;
      default:
        {
          cta::exception::Exception ex;
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

} // namespace mediachanger
} // namespace cta
