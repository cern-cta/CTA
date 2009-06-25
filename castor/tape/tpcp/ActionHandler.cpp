/******************************************************************************
 *                 castor/tape/tpcp/ActionHandler.cpp
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
 
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tpcp/ActionHandler.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/utils/utils.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ActionHandler::ActionHandler(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
  const int volReqId, castor::io::ServerSocket &callbackSocket) throw() :
  m_debug(debug),
  m_tapeFseqRanges(tapeFseqRanges),
  m_filenames(filenames),
  m_vmgrTapeInfo(vmgrTapeInfo),
  m_dgn(dgn),
  m_volReqId(volReqId),
  m_callbackSocket(callbackSocket) {
}


//------------------------------------------------------------------------------
// tellAggregatorNoMoreFiles
//------------------------------------------------------------------------------
void castor::tape::tpcp::ActionHandler::acknowledgeEndOfSession()
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the aggregator
  int connectionSocketFd = 0;

  // Wait for a callback connection from the aggregator
  {
    bool waitForCallback = true;
    while(waitForCallback) {
      try {
        connectionSocketFd = net::acceptConnection(m_callbackSocket.socket(),
          WAITCALLBACKTIMEOUT);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {
        std::cout << "Waited " << WAITCALLBACKTIMEOUT << "seconds for a "
        "callback connection from the tape server." << std::endl
        << "Continuing to wait." <<  std::endl;
      }
    }
  }

  // If debug, then display a textual description of the aggregator
  // callback connection
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "ActionHandler: Aggregator connection");
    os << std::endl;
    net::writeSocketDescription(os, connectionSocketFd);
    os << std::endl;
    os << std::endl;
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket callbackConnectionSocket(connectionSocketFd);

  // Read in the object sent by the aggregator
  std::auto_ptr<castor::IObject> obj(callbackConnectionSocket.readObject());

  // Pointer to the received object with the object's type
  tapegateway::EndNotification *endNotification = NULL;

  // Cast the object to its type
  endNotification = dynamic_cast<tapegateway::EndNotification*>(obj.get());
  if(endNotification == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
     << "Received the wrong type of object from the aggregator"
     << ": Actual=" << utils::objectTypeToString(obj->type())
     << " Expected=EndNotification";

    throw ex;
  }

  // If debug, then display reception of the EndNotification message
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "ActionHandler: Received EndNotification from "
      "aggregator");
    os << std::endl;
    os << std::endl;
  }

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setTransactionId(m_volReqId);

  // Send the volume message to the aggregator
  callbackConnectionSocket.sendObject(acknowledge);

  // Close the connection to the aggregator
  callbackConnectionSocket.close();

  // If debug, then display sending of the NotificationAcknowledge message
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "Sent NotificationAcknowledge to aggregator");
    os << std::endl;
    os << std::endl;
  }
}
