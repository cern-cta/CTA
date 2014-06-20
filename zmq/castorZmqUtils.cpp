/******************************************************************************
 *                castorZmqUtils.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "zmq/castorZmqUtils.hpp"
#include "zmq/castorZmqWrapper.hpp"
#include "castor/utils/utils.hpp"
namespace castor {
  namespace utils {
void connectToLocalhost(zmq::socket_t& m_socket){
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress+=castor::utils::toString(tape::tapeserver::daemon::TAPE_SERVER_INTERNAL_LISTENING_PORT);
  m_socket.connect(bindingAdress.c_str());
}
castor::messages::Header preFilleHeader(){
    castor::messages::Header header;
    header.set_magic(TPMAGIC);
    header.set_protocoltype(castor::messages::protocolType::Tape);
    header.set_protocolversion(castor::messages::protocolVersion::prototype);
    header.set_bodyhashtype("SHA1");
    header.set_bodysignaturetype("SHA1");
    return header;
  }
  }}