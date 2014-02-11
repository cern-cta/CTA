/******************************************************************************
 *                      tapeSession.hpp
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

#include <sys/socket.h>
#include <stdint.h>
#include <string>

#include "../../tapegateway/GatewayMessage.hpp"

namespace castor {
namespace tape {
namespace server {
  class tapeSession {
  public:
    tapeSession(uint32_t client_ip, uint16_t client_port,
            uint64_t mountTransactionId):
      m_client(client_ip, client_port), 
      m_mountTransactionId(mountTransactionId) {}
    std::string m_vid;
    void getVolume();
  private:
    castor::IObject *
      requestReplySession(castor::tape::tapegateway::GatewayMessage &req);
    // Address of the client (in terms of tape server, as it's a TCP SERVER)
    struct clientAddr {
      clientAddr(uint32_t cip, uint16_t cport):
        ip(cip), port(cport) {}
      uint32_t ip;
      uint16_t port;
    } m_client;
    uint64_t m_mountTransactionId;

  };  
}
}
}
