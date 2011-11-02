/******************************************************************************
 *                castor/tape/tapebridge/VmgrTxRx.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/LogHelper.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapebridge/VmgrTxRx.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/io/ClientSocket.hpp"
#include "h/getconfent.h"
#include "h/vmgr.h"

#include <stdlib.h>
#include <sys/time.h>


//-----------------------------------------------------------------------------
// getTapeInfoFromVmgr
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VmgrTxRx::getTapeInfoFromVmgr(
  const Cuuid_t                  &cuuid,
  const uint32_t                 volReqId,
  const int                      netReadWriteTimeout,
  const uint32_t                 uid,
  const uint32_t                 gid,
  const char                     *const vid,
  legacymsg::VmgrTapeInfoMsgBody &reply)
  throw(castor::exception::Exception) {

  // Determine the VMGR host
  char vmgrHost[CA_MAXHOSTNAMELEN+1];
  {
    char *p = NULL;

    if((p = getenv("VMGR_HOST")) || (p = getconfent("VMGR", "HOST", 0))) {
      utils::copyString(vmgrHost, p);
    } else {
      castor::exception::Exception ex(EVMGRNOHOST);

      ex.getMessage() << "VMGR HOST not set";
      throw(ex);
    }
  }

  // Determine the VMGR port
  uint16_t vmgrPort = VMGR_PORT;
  {
    char *p = NULL;

    if((p = getenv("VMGR_PORT")) || (p = getconfent("VMGR", "PORT", 0))) {
      if(!utils::isValidUInt(p)) {
        castor::exception::InvalidArgument ex;

        ex.getMessage() << "VMGR PORT is not a valid unsigned integer"
          ": Value=" << p;
        throw(ex);
      }

      vmgrPort = atoi(p);
    }
  }

  // Connect to the VMGR
  castor::io::ClientSocket sock(vmgrPort, vmgrHost);
  sock.setTimeout(netReadWriteTimeout);
  timeval connectDuration = {0, 0};
  try {
    timeval connectStartTime = {0, 0};
    timeval connectEndTime   = {0, 0};
    utils::getTimeOfDay(&connectStartTime, NULL);
    sock.connect();
    utils::getTimeOfDay(&connectEndTime, NULL);
    connectDuration = utils::timevalAbsDiff(connectStartTime, connectEndTime);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to the VMGR "
      ": vid=" << vid <<
      ": " << ex.getMessage().str());
  }

  // Prepare the logical request
  legacymsg::VmgrTapeInfoRqstMsgBody request;
  utils::setBytes(request, '\0');
  request.uid = uid;
  request.gid = gid;
  utils::copyString(request.vid, vid);
  request.side = 0; // HARDCODED side

  // Marshal the request
  char buf[VMGRMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to marshal request for tape information"
      ": vid=" << vid <<
      ": " << ex.getMessage().str());
  }

  // Send the request
  timeval sendAndReceiveStartTime = {0, 0};
  timeval sendAndReceiveEndTime   = {0, 0};
  timeval sendRecvDuration        = {0, 0};
  utils::getTimeOfDay(&sendAndReceiveStartTime, NULL);
  try {
    net::writeBytes(sock.socket(), netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send request for tape information to the VMGR: "
      << ex.getMessage().str());
  }

  // Receive header from the VMGR
  legacymsg::MessageHeader header;
  utils::setBytes(header, '\0');
  try {
    LegacyTxRx::receiveMsgHeader(cuuid, volReqId, sock.socket(),
      netReadWriteTimeout, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to receive tape request from RTCPD"
      ": " << ex.getMessage().str());
  }

  switch(header.reqType) {

  // The VMGR wishes to keep the connection open - this is unexpected
  case VMGR_IRC:
    {
      sock.close();

      castor::exception::Exception ex(EBADMSG);

      ex.getMessage() <<
        "VMGR unexpectedly wishes to keep the connection open"
        ": reqType=VMGR_IRC"
        ": vid=" << vid;
      throw ex;
    }
    break;

  // The VMGR has returned an error code
  case VMGR_RC:
    {
      sock.close();

      // Convert the error code into an exception
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Received an error code from the VMGR"
        ": reqType=VMGR_RC"
        ": vid=" << vid <<
        ": VMGR error code=" << header.lenOrStatus;
      throw(ex);
    }
    break;

  // The VMGR has returned an error string
  case MSG_ERR:
    {
      // Length of body buffer = Length of message buffer - length of header
      char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

      // If the error string is larger than the receive buffer
      if(header.lenOrStatus > sizeof(bodyBuf)) {
        sock.close();

        castor::exception::Exception ex(ECANCELED);

        ex.getMessage() <<
          "Error string from VMGR is larger than the receive buffer"
          ": reqType=MSG_ERR"
          ": vid=" << vid <<
          ": receive buffer size=" << sizeof(bodyBuf) << " bytes"
          ": error string size including null terminator=" <<
          header.lenOrStatus << " bytes";
        throw(ex);
      }

      // Receive the error string
      try {
        net::readBytes(sock.socket(), netReadWriteTimeout, header.lenOrStatus,
          bodyBuf);
      } catch (castor::exception::Exception &ex) {
        TAPE_THROW_CODE(EIO,
          ": Failed to receive error string from VMGR" <<
          ": reqType=MSG_ERR"
          ": vid=" << vid <<
          ": "<< ex.getMessage().str());
      }

      // Ensure the error string is null terminated
      bodyBuf[sizeof(bodyBuf)-1] = '\0';
      
      // Convert the error string into an exception
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Received an error string from the VMGR"
        ": reqType=MSG_ERR"
        ": vid=" << vid <<
        ": VMGR error string=" << bodyBuf;
      throw(ex);
    }

  // The VMGR returned the tape information
  case MSG_DATA:
    {
      // Length of body buffer = Length of message buffer - length of header
      char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

      // If the message body is larger than the receive buffer
      if(header.lenOrStatus > sizeof(bodyBuf)) {
        sock.close();

        castor::exception::Exception ex(ECANCELED);

        ex.getMessage() <<
          "Message body from the VMGR is larger than the receive buffer"
          ": reqType=MSG_DATA"
          ": vid=" << vid <<
          ": receive buffer size=" << sizeof(bodyBuf) << " bytes"
          ": message body size=" << header.lenOrStatus << " bytes";
        throw(ex);
      }

      // Receive the message body
      try {
        net::readBytes(sock.socket(), netReadWriteTimeout, header.lenOrStatus,
          bodyBuf);
        utils::getTimeOfDay(&sendAndReceiveEndTime, NULL);
        sendRecvDuration = utils::timevalAbsDiff(sendAndReceiveStartTime,
          sendAndReceiveEndTime);
      } catch (castor::exception::Exception &ex) {
        TAPE_THROW_CODE(EIO,
          ": Failed to receive message body from VMGR" <<
          ": reqType=MSG_DATA"
          ": vid=" << vid <<
          ": "<< ex.getMessage().str());
      }

      // Unmarshal the message body
      try {
        const char *p           = bodyBuf;
        size_t     remainingLen = header.lenOrStatus;
        legacymsg::unmarshal(p, remainingLen, reply);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to unmarshal message body from VMGR" <<
          ": reqType=MSG_DATA"
          ": vid=" << vid <<
          ": "<< ex.getMessage().str());
      }

      LogHelper::logMsgBody(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_RECEIVED_TAPE_INFO_FROM_VMGR, volReqId, sock.socket(),
        reply, connectDuration, sendRecvDuration);
    }
    break;

  // The VMGR returned an unknown message type
  default:
    {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "Received an unkown message type from the VMGR"
        ": reqType=" << header.reqType <<
        ": vid=" << vid;

      throw(ex);
    }
  } // switch(header.reqType)
}
