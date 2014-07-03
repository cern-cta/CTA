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

#include "../../tpcp/TpcpCommand.hpp"
#include <memory>
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  /**
   * A templated class, reusing code of TpcpCommand which simulates the 
   * tapegateway part of the client communication with the server. The 
   * constructor will hence setup a client callback socket and wait. The
   * method processRequest will wait for a message and reply with the type
   * chosen on template implementation.
   */
  template<class ReplyType>
  class ClientSimSingleReply: public tpcp::TpcpCommand {
  public:
    ClientSimSingleReply(uint32_t volReqId, const std::string & vid, 
            const std::string & density, bool breakTransaction = false): 
    TpcpCommand("clientSimulator::clientSimulator"), m_vid(vid), 
    m_density(density), m_breakTransaction(breakTransaction)
    {
      m_volReqId = volReqId;
      setupCallbackSock();
    }
    
    virtual ~ClientSimSingleReply() throw() {}
    
    struct ipPort {
      ipPort(uint32_t i, uint16_t p): ip(i), port(p) {}
      union {
        uint32_t ip;
        struct {
          uint8_t a;
          uint8_t b;
          uint8_t c;
          uint8_t d;
        };
      };
      uint16_t port;
    };
    struct ipPort getCallbackAddress() {
      unsigned short port = 0;
      /* This is a workaround for the usage of unsigned long for ips in castor
       * (it's not fine anymore on 64 bits systems).
       */
      unsigned long ip = 0;
      m_callbackSock.getPortIp(port, ip);
      return ipPort(ip,port);
    }
    void sessionLoop() {
      processFirstRequest();
      m_callbackSock.close();
    }
    
  protected:
    // Place holders for pure virtual members of TpcpCommand we don't
    // use in the simulator
    virtual void usage(std::ostream &) const throw() {}
    virtual void parseCommandLine(const int, char **)
       {}
    virtual void checkAccessToDisk()
      const  {}
    virtual void checkAccessToTape()
      const  {}
    virtual void requestDriveFromVdqm(char *const)
       {}
    virtual void performTransfer()  {}
    
    
    // The functions we actually implement in the simulator
    virtual void sendVolumeToTapeBridge(
      const tapegateway::VolumeRequest &volumeRequest,
      castor::io::AbstractTCPSocket    &connection)
      const  {}
    virtual bool dispatchMsgHandler(castor::IObject *const obj,
      castor::io::AbstractSocket &sock)  {
      return false;
    }
    
  private:
    // Process the first request which should be getVolume
    void processFirstRequest()  {
      // Accept the next connection
      std::auto_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
      // Read in the message sent by the tapebridge
      std::auto_ptr<castor::IObject> obj(clientConnection->readObject());
      // Convert to a gateway message (for transactionId)
      tapegateway::GatewayMessage & gm = 
              dynamic_cast<tapegateway::GatewayMessage &>(*obj);
      // Reply with our own type and transaction id
      ReplyType repl;
      repl.setAggregatorTransactionId(gm.aggregatorTransactionId() ^ 
        (m_breakTransaction?666:0));
      repl.setMountTransactionId(m_volReqId);
      clientConnection->sendObject(repl);
    }
    // Notify the client
    void sendEndNotificationErrorReport(
    const uint64_t             tapebridgeTransactionId,
    const int                  errorCode,
    const std::string          &errorMessage,
    castor::io::AbstractSocket &sock)
    throw();
    std::string m_vid;
    std::string m_volLabel;
    std::string m_density;
    bool m_breakTransaction;
  };
  
  /**
   * Specific version for the FilesToMigrateList: we want
   * to pass a non empty list, so it has to be a little bit less trivial.
   */
  template<>
  void ClientSimSingleReply<castor::tape::tapegateway::FilesToMigrateList>::processFirstRequest() 
           {
    using namespace castor::tape::tapegateway;
      // Accept the next connection
      std::auto_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
      // Read in the message sent by the tapebridge
      std::auto_ptr<castor::IObject> obj(clientConnection->readObject());
      // Convert to a gateway message (for transactionId)
      GatewayMessage & gm = 
              dynamic_cast<tapegateway::GatewayMessage &>(*obj);
      // Reply with our own type and transaction id
      FilesToMigrateList repl;
      repl.setAggregatorTransactionId(gm.aggregatorTransactionId() ^ 
        (m_breakTransaction?666:0));
      repl.setMountTransactionId(m_volReqId);
      repl.filesToMigrate().push_back(new FileToMigrateStruct());
      clientConnection->sendObject(repl);
  }
  
    /**
   * Specific version for the FilesToRecallList: we want
   * to pass a non empty list, so it has to be a little bit less trivial.
   */
  template<>
  void ClientSimSingleReply<castor::tape::tapegateway::FilesToRecallList>::processFirstRequest() 
           {
    using namespace castor::tape::tapegateway;
      // Accept the next connection
      std::auto_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
      // Read in the message sent by the tapebridge
      std::auto_ptr<castor::IObject> obj(clientConnection->readObject());
      // Convert to a gateway message (for transactionId)
      GatewayMessage & gm = 
              dynamic_cast<tapegateway::GatewayMessage &>(*obj);
      // Reply with our own type and transaction id
      FilesToRecallList repl;
      repl.setAggregatorTransactionId(gm.aggregatorTransactionId() ^ 
        (m_breakTransaction?666:0));
      repl.setMountTransactionId(m_volReqId);
      repl.filesToRecall().push_back(new FileToRecallStruct());
      clientConnection->sendObject(repl);
  }
}
}
}
}
