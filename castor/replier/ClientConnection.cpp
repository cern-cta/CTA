/******************************************************************************
 *                      ClientConnection.cpp
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
 * @(#)ClientConnection.cpp,v 1.7 $Release$ 2004/09/17 09:08:27 bcouturi
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/

#include <Cthread_api.h>

#include "ClientConnection.hpp"
#include "castor/rh/Client.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/Constants.hpp"

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <net.h>
#include <netdb.h>
#include <errno.h>

#include <cstring>
#include <string>

//-----------------------------------------------------------------------------
// constructors
//-----------------------------------------------------------------------------

castor::replier::NoMoreMessagesException::NoMoreMessagesException() : castor::exception::Exception(SEINTERNAL) {}

castor::replier::EndConnectionException::EndConnectionException() : castor::exception::Exception(SEINTERNAL) {}

castor::replier::ClientConnection::ClientConnection() throw() :
  BaseObject(), m_client(), m_responses(), m_fd(-1),
  m_lastEventDate(time(0)), m_status(INACTIVE), m_terminate(false) {};

castor::replier::ClientConnection::ClientConnection(ClientResponse cr) throw() :
  BaseObject(), m_client(cr.client), m_responses(), m_fd(-1),
  m_lastEventDate(time(0)), m_status(INACTIVE), m_terminate(false) {
    m_responses.push(cr.response);
};

void castor::replier::ClientConnection::addResponse(ClientResponse cr) throw() {
  m_responses.push(cr.response);
  if (cr.isLast) {
    m_terminate = true;
  }
}

castor::replier::RCStatus  castor::replier::ClientConnection::getStatus() throw() {
  return m_status;
}



std::string castor::replier::ClientConnection::getStatusStr()
  throw() {
  std::string str;
  switch(m_status) {
  case INACTIVE:
    str = "INACTIVE";
    break;
  case CONNECTING:
    str = "CONNECTING";
    break;
  case CONNECTED:
    str = "CONNECTED";
    break;
  case RESEND:
    str = "RESEND";
    break;
  case SENDING:
    str = "SENDING";
    break;
  case DONE_SUCCESS:
    str = "DONE_SUCCESS";
    break;
  case DONE_FAILURE:
    str = "DONE_FAILURE";
    break;
  default:
    str = "UNKNOWN(";
    str += m_status;
    str += ")";
  }

  return str;
}

void castor::replier::ClientConnection::close() throw() {

  ::close(m_fd);
  while (!m_responses.empty()) {
    clog() << DEBUG << "Deleting response" << std::endl;
    castor::io::biniostream *response = m_responses.front();
    delete response;
    m_responses.pop();
  }
}


castor::rh::Client castor::replier::ClientConnection::getClient()  throw() {
  return m_client;
}

void castor::replier::ClientConnection::setStatus(enum RCStatus stat)
  throw() {
  m_status = stat;
  m_lastEventDate = time(0);
  clog() << DEBUG << "<" << m_fd << "> Setting status to "
         << getStatusStr() << std::endl;
}


void castor::replier::ClientConnection::createSocket()
  throw(castor::exception::Exception) {

  // Creating the socket
  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0){
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't create socket:" << strerror(errno) << std::endl;
    throw ex;
  }
  
  // Setting the socket to nodelay mode
  int yes = 1;
  if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char *)&yes, sizeof(yes)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't set socket to TCP_NODELAY mode:"
                    << strerror(errno) << std::endl;
    ::close(s);
    throw ex;

  }

  // Setting the socket to asynchonous mode
  int nonblocking= 1 ;
  if (ioctl(s, FIONBIO,&nonblocking) == -1 ) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't set socket to asynchonous mode:"
                    << strerror(errno) << std::endl;
    ::close(s);
    throw ex;
  }

  // Setting the socket in the ClientConnection object
  m_fd = s;

}

void castor::replier::ClientConnection::connect()
  throw(castor::exception::Exception) {

  // Preparing the client address
  struct sockaddr_in saddr;
  memset(&saddr, 0, sizeof(saddr));
  saddr.sin_addr.s_addr = htonl(m_client.ipAddress());
  saddr.sin_port = htons(m_client.port());
  saddr.sin_family = AF_INET;

  // Connecting to the client
  int rc = ::connect(m_fd, (struct sockaddr *)&saddr, sizeof(saddr));
  if (rc == -1 && errno != EINPROGRESS) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Can't connect to client:"
                    << strerror(errno) << std::endl;
    ::close(m_fd);
    throw ex;
  }
  clog() << INFO << "Connected back to "
         << castor::ip << m_client.ipAddress()
         << " successfully." << std::endl;   
  setStatus(CONNECTING);  
}

void castor::replier::ClientConnection::pop()  throw(castor::exception::Exception) {
  if (!m_responses.empty()) {
    castor::io::biniostream *response = m_responses.front();
    delete response;
    m_responses.pop();
  }
}

void castor::replier::ClientConnection::sendData()  throw(castor::exception::Exception) {
  if (m_responses.empty()) {
    NoMoreMessagesException nme;
    throw nme;
  }
  send();
}

void castor::replier::ClientConnection::send()
  throw(castor::exception::Exception) {

  castor::io::biniostream *response;

  if (!m_responses.empty()) {
    response = m_responses.front();
  } else {
    return;
  }

  unsigned int len =  response->str().length();
  int buflen = 2*sizeof(int) + len;
  char *buf = new char[buflen];
  char *p = buf;

  unsigned int magic = SEND_REQUEST_MAGIC;
  memcpy(p, &magic, sizeof(int));
  p+=  sizeof(int);
  memcpy(p, &len, sizeof(int));
  p+=  sizeof(int);
  memcpy(p, response->str().data(), len);

  char *pc = (char *)buf;

  int rc = write(m_fd, (char *)(buf), buflen);
  clog() << DEBUG << "Write syscall returned " << rc
         <<  " (len:" << buflen << ")"
         << std::endl;
  if (rc == -1) {
    if (errno == EAGAIN) {
      setStatus(RESEND);
    } else {
      delete[] buf;
      clog() << ERROR << "WRITE FAILURE"
             << strerror(errno)
             << std::endl;
      setStatus(DONE_FAILURE);
      m_errorMessage = std::string("Error while sending: ") 
        + std::string(strerror(errno));
      castor::exception::Exception ex(errno);
      ex.getMessage() << "Error while sending";
      throw ex;
    }
  } else {
    m_lastEventDate = time(0);
    clog() << DEBUG << "Data sent back to "
           << castor::ip << m_client.ipAddress()
           << std::endl;   
    pop();
  }

  delete[] buf;

}





