/******************************************************************************
 *                      clientInterface.hpp
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

#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "../threading/Threading.hpp"

using namespace castor::tape;

namespace castor {
namespace tape {
namespace server {
  /**
   * A class managing the communications with the tape server's client.
   * The client address will have been received from the VDQM by the main
   * process. 
   */
  class ClientInterface {
  public:
    /**
     * Constructor: contact client, gather initial information about the
     * session and decide get that information ready for th user of the class
     * (read/write session, first file information, etc...)
     * @param clientRequest the client information as sent by VDQM.
     */
    ClientInterface(const legacymsg::RtcpJobRqstMsgBody & clientRequest)
            throw (castor::tape::Exception);
    
    /**
     * Class holding the timing information for the request/reply,
     * and the message sequence Id.
     */
    class RequestReport {
    public:
      RequestReport(): transactionId(0),
        connectDuration(0), sendRecvDuration(0) {}
      uint32_t transactionId;
      double connectDuration;
      double sendRecvDuration;
    };
    
    /**
     * Class holding the result of a Volume request
     */
    class VolumeInfo {
    public:
      VolumeInfo() {};
      /** The VID we will work on */
      std::string vid;
      /** The type of the session */
      tapegateway::ClientType clientType;
      /** The density of the volume */
      std::string density;
      /** The label field seems to be in disuse */
      std::string labelObsolete;
      /** The read/write mode */
      tapegateway::VolumeMode volumeMode;
    };
    
    /**
     * Retrieves the volume Id from the client (with transfer direction)
     * Throws an EndOfSession exception
     * @param report report on timing and request Id. It will still be filled
     * up and can be used when a exception is thrown.
     * @return the transaction id
     */
    void fetchVolumeId(VolumeInfo & volInfo, RequestReport &report) throw (castor::tape::Exception);
    
    /**
     * Reports end of session to the client. This should be the last call to
     * the client.
     */
    void reportEndOfSession(RequestReport &report) throw (Exception);
    
    /**
     * Exception thrown when the wrong response type was received from
     * the client after a request. Extracts the type and prints it.
     */
    class UnexpectedResponse: public castor::tape::Exception {
    public:
      UnexpectedResponse(const castor::IObject * resp, const std::string & w="");
    };
    
    /**
     * Exception marking end of session
     */
    class EndOfSession: public castor::tape::Exception {
    public:
      EndOfSession(std::string w=""):castor::tape::Exception(w) {}
    };
   
    
  private:
    /** The VDQM request that kickstarted the session */
    legacymsg::RtcpJobRqstMsgBody m_request;
    /**
     * A helper function managing a single request-response session with the
     * client.
     * @param req the request to send to the client
     * @return the response from the client
     */
    tapegateway::GatewayMessage * requestResponseSession(
            const tapegateway::GatewayMessage &req,
            RequestReport & report) throw (castor::tape::Exception);
    
    /**
     * A helper class managing a thread safe message counter (we need it thread
     * safe as the ClientInterface class will be used by both the getting of
     * the work to be done and the reporting of the completed work, in parallel
     */
    template <typename T>
    class ThreadSafeCounter {
    public:
      ThreadSafeCounter(): m_val(0) {};
      operator T () {
        threading::MutexLocker ml(&m_mutex);
        return ++m_val;
      }
    private:
      T m_val;
      threading::Mutex m_mutex;
    };
    /** The file transaction id a.k.a. aggregator transaction id. This is the 
     * serial number of the message in the session */
    ThreadSafeCounter<uint32_t> m_transactionId;
  };
  
}
}
}
