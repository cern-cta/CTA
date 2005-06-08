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
#include <arpa/inet.h>
#include <net.h>
#include <netdb.h>
#include <errno.h>

#include <cstring>
#include <string>
#include <sstream>
#include <iomanip>

#define WD 30
#define SETW std::setw(WD) << std::left <<

//-----------------------------------------------------------------------------
// constructors
//-----------------------------------------------------------------------------

castor::replier::NoMoreMessagesException::NoMoreMessagesException() : castor::exception::Exception(SEINTERNAL) {}

castor::replier::EndConnectionException::EndConnectionException() : castor::exception::Exception(SEINTERNAL) {}

castor::replier::ClientConnection::ClientConnection() throw() :
  BaseObject(), m_client(), m_messages(), m_fd(-1),
  m_lastEventDate(time(0)), m_status(INACTIVE), m_terminate(false),
  m_nextMessageId(1) {
    char *func = "cc::ClientConnection ";
    m_clientStr = buildClientStr(m_client);
    clog() << USAGE << SETW func  << this->toString() << " created" << std::endl;
};

castor::replier::ClientConnection::ClientConnection(ClientResponse cr) throw() :
  BaseObject(), m_client(cr.client), m_messages(), m_fd(-1),
  m_lastEventDate(time(0)), m_status(INACTIVE), m_terminate(false),
  m_nextMessageId(1)  {
    char *func = "cc::ClientConnection ";
    m_clientStr = buildClientStr(m_client);
    cr.messageId = m_nextMessageId;
    m_nextMessageId++;
    m_messages.push(cr);
    clog() << USAGE << SETW func  << this->toString() << " created and added msg:" 
	   << cr.messageId
	   << std::endl;
};

void castor::replier::ClientConnection::addMessage(ClientResponse cr) throw() {
  char *func = "cc::addMessage ";
  cr.messageId = m_nextMessageId;
  m_nextMessageId++;
  m_messages.push(cr);
  clog() << USAGE << SETW func  << this->toString() << " added msg:" 
	 << cr.messageId << " (terminate:" 
	 << (cr.isLast?1:0) << ")"
	 << std::endl;
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

  char *func = "cc::close ";
  ::close(m_fd);
  clog() << USAGE << SETW func  << this->toString() << " Deleting while "
	 << m_messages.size() << " messages are left"
	 << std::endl;
  while (!m_messages.empty()) {
    ClientResponse message = m_messages.front();
    clog() << DEBUG << SETW func  << this->toString() << " Deleting msg "
	   << message.messageId
	   << std::endl;
    delete message.response;
    m_messages.pop();
  }
}


castor::rh::Client castor::replier::ClientConnection::client()  throw() {
  return m_client;
}

bool castor::replier::ClientConnection::terminate()  throw() {
  return m_terminate;
}

int castor::replier::ClientConnection::lastEventDate()  throw() {
  return m_lastEventDate;
}

int castor::replier::ClientConnection::fd()  throw() {
  return m_fd;
}

std::string castor::replier::ClientConnection::errorMessage()  throw() {
  return m_errorMessage;
}

void castor::replier::ClientConnection::setErrorMessage(std::string msg) throw () {
  m_errorMessage = msg;
}

std::queue<castor::replier::ClientResponse> 
castor::replier::ClientConnection::messages() throw() {
  return m_messages;
}

bool castor::replier::ClientConnection::hasMessagesToSend() throw() {
  return  (m_messages.size() > 0);
}

void castor::replier::ClientConnection::setStatus(enum RCStatus stat)
  throw() {
  char *func = "cc::setStatus ";
  m_status = stat;
  m_lastEventDate = time(0);
  clog() << USAGE << SETW func  << this->toString() << " Setting status to "
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

  char *func = "cc::connect ";

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
  clog() << USAGE << SETW func  << this->toString() 
	 <<" connect syscall ok"
         << std::endl;   
  setStatus(CONNECTING);  
}

void castor::replier::ClientConnection::deleteNextMessage()  throw(castor::exception::Exception) {
  char *func = "cc::deleteNextMessage ";
  if (!m_messages.empty()) {
    ClientResponse message = m_messages.front();
    clog() << DEBUG << SETW func  << this->toString() << " Deleting msg "
	   << message.messageId
	   << std::endl;
    delete message.response;
    m_messages.pop();
  }
}

void castor::replier::ClientConnection::sendNextMessage()  throw(castor::exception::Exception) {
  char *func = "cc::sendNextMessage ";
  if (m_messages.empty()) {
    clog() << DEBUG << SETW func  << this->toString() 
	   << "No more messages in queue!" << std::endl;
     NoMoreMessagesException nme;
    throw nme;
  }
  send();
}




// Internal send method
void castor::replier::ClientConnection::send()
  throw(castor::exception::Exception) {

  char *func = "cc::send ";
  castor::io::biniostream *response;
  ClientResponse message;

  if (!m_messages.empty()) {
    message = m_messages.front();
    response = message.response;
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

  size_t written = 0;
  ssize_t rc = 0;
  while (written < buflen) {
    rc = write(m_fd, (char *)(buf + written), buflen - written);
    if (rc == -1) {
      if (errno == EAGAIN) {
        continue;
      } else {
        break;
      }
    }
    written += rc;
  }

  if (rc == -1) {
    if (errno == EAGAIN) {
      setStatus(RESEND);
    } else {
      delete[] buf;
      clog() << ERROR << SETW func  << this->toString() << " msg:" << message.messageId
	     << " WRITE FAILURE (isLast:"
	     << ((message.isLast)?1:0)
	     << ")rc:" << rc
	     <<  " (len:" << buflen << ") error: " << strerror(errno)
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
 
    clog() << USAGE << SETW func  << this->toString() 
	   << " msg:" << message.messageId 
	   << " Send successful (isLast:"
	   << ((message.isLast)?1:0)
	   << ") written:" << written
	   <<  " (len:" << buflen << ")"
           << std::endl;   
    /* XXX Should this be done here ? */
    this->deleteNextMessage();
  }

  delete[] buf;

}


std::string castor::replier::ClientConnection::toString() throw() {
   std::ostringstream sst;
   sst << m_clientStr 
       << "(" << this->fd() <<  "," << this->getStatusStr() << ")";
  return sst.str();
}


std::string castor::replier::ClientConnection::buildClientStr(castor::rh::Client client) {
  struct in_addr cad;
  cad.s_addr = client.ipAddress();
  std::ostringstream sst;
  sst << inet_ntoa(cad) << ":" << client.port();
  return  sst.str();
}
