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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/server/Threading.hpp"
#include "castor/server/AtomicCounter.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  /**
   * A class managing the communications with the tape server's client.
   * The client address will have been received from the VDQM by the main
   * process. 
   */
  class ClientProxy : public ClientInterface {
  public:
    /**
     * Constructor: contact client, gather initial information about the
     * session and decide get that information ready for th user of the class
     * (read/write session, first file information, etc...)
     */
    ClientProxy();
    
    /**
     * Retrieves the volume Id from the client (with transfer direction)
     * Throws an EndOfSession exception
     * @param report report on timing and request Id. It will still be filled
     * up and can be used when a exception is thrown.
     * @return the transaction id
     */
    void fetchVolumeId(VolumeInfo & volInfo, RequestReport &report);
    
    /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     */
    virtual void reportEndOfSession(RequestReport &report);
    
    /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     * @param transactionReport Placeholder to network timing information,
     * populated during the call and used by the caller to log performance 
     * and context information
     * @param errorMsg (sent to the client)
     * @param errorCode (sent to the client)
     */
    virtual void reportEndOfSessionWithError(const std::string & errorMsg, int errorCode, 
    RequestReport &transactionReport);
    /**
     * Exception thrown when the wrong response type was received from
     * the client after a request. Extracts the type and prints it.
     */
    class UnexpectedResponse: public castor::exception::Exception {
    public:
      UnexpectedResponse(const castor::IObject * resp, const std::string & w="");
    };
    
    /**
     * Exception marking end of session
     */
    class EndOfSession: public castor::exception::Exception {
    public:
      EndOfSession(std::string w=""):castor::exception::Exception(w) {}
    };
   
    /**
     * Exception marking end of with error
     */
    class EndOfSessionWithError: public EndOfSession {
    public:
      EndOfSessionWithError(std::string w=""):EndOfSession(w) {}
    };
    
  private:
    /**
     * A helper function managing a single request-response session with the
     * client.
     * @param req the request to send to the client
     * @param report Report of the connection and request-reply time
     * @param timeout (optional) read response timeout (castor wide default if
     * not set, currently 20 seconds)
     * @return the response from the client
     */
    tapegateway::GatewayMessage * requestResponseSession(
            const tapegateway::GatewayMessage &req,
            RequestReport & report,
            int timeout = 0);
    
    /** The file transaction id a.k.a. aggregator transaction id. This is the 
     * serial number of the message in the session */
    castor::server::AtomicCounter<uint32_t> m_transactionId;
  };
  
}
}
}
}
