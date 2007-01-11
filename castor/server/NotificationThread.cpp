/******************************************************************************
 *                      NotificationThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: NotificationThread.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2007/01/11 10:23:03 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/NotificationThread.hpp"
#include "castor/exception/Internal.hpp"
#include "marshall.h"

#if defined(_WIN32)
#include <time.h>
#include <winsock2.h>                   /* For struct servent */
#else
#include <sys/time.h>
#include <unistd.h>
#include <netdb.h>                      /* For struct servent */
#include <net.h>
#endif

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::NotificationThread::NotificationThread(int notifPort) :
  m_notifPort(notifPort), m_owner(0) {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::NotificationThread::run(void* param)
{
  m_owner = (SignalThreadPool*)param;
  int s = -1;

  try {

  struct sockaddr_in serverAddress, clientAddress;
  int on = 1;	/* for REUSEADDR */
  int ibind;

#if defined(_WIN32)
  WSADATA wsadata;
  if (WSAStartup(MAKEWORD (2, 0), &wsadata)) {
    serrno = SEINTERNAL;
    return(NULL);
  }
#endif

  /* Create a socket */
  if ((s = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "NotificationThread: failed to create socket";
    throw ex;
  }
  memset ((char *)&serverAddress, 0, sizeof(serverAddress));
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddress.sin_port = htons(m_notifPort);
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));

  /* Bind on it - we know that the port can be reused but not always immediately */
  ibind = 0;
  while (ibind++ < MAX_BIND_RETRY) {
    if (bind(s, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) >= 0)
      break;
  }
  if (ibind == MAX_BIND_RETRY) {
    castor::exception::Internal ex;
    ex.getMessage() << "NotificationThread: failed to bind to port " << m_notifPort;
    throw ex;
  }

  /* Say to our parent that we successfully started - not needed in principle */
  //m_owner->m_poolMutex->lock();

  //m_owner->nbNotifyThreads = 1;

  //m_owner->m_poolMutex->signal();
  //m_owner->m_poolMutex->release();

  /* Wait for a notification */
  while(true) {
    int magic;
    int nb_recv;
    char buf[HYPERSIZE + LONGSIZE];
    char *p;
    int nb_thread_wanted;
    socklen_t clientAddressLen;
    struct timeval timeout;
    fd_set read_handles;

    memset(buf, 0, sizeof(buf));
    clientAddressLen = sizeof(clientAddress);

    /* Use select() to avoid blocking on recvfrom() */
    FD_ZERO(&read_handles);
    FD_SET(s,&read_handles);
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    if (select(s + 1, &read_handles, NULL, NULL, &timeout) > 0) {

      /* Reading the header - blocks until there is something to read */
      nb_recv = recvfrom(s, buf, 1024, 0, (struct sockaddr *)&clientAddress, &clientAddressLen );

      if (nb_recv != (HYPERSIZE+LONGSIZE)) {
      	/* Ignore this packet */
      	continue;
      }

      p = buf;
      unmarshall_HYPER(p, magic);
      unmarshall_LONG(p, nb_thread_wanted);

      if (magic != NOTIFY_MAGIC) {
      	/* Not a packet for us (!?) */
      	continue;
      }

      /* Okay - we have received a notification - we signal our condition variable */
      try {
        m_owner->m_poolMutex->lock();

      	m_owner->m_notified += nb_thread_wanted;

      	/* We make sure that 0 <= singleService.notified <= NbThreadInactive */
      	if(m_owner->m_notified < 0) {
      	  m_owner->m_notified = 1;
      	}
      	int nbThreadInactive = m_owner->m_nbTotalThreads - m_owner->m_nbActiveThreads;
      	if(nbThreadInactive < 0) {
          nbThreadInactive = 0;
      	}
      	if (nbThreadInactive == 0) {
      	  /* All threads are already busy : try to get one counting on timing windows */
      	  m_owner->m_notified = 1;
      	} else {
      	  if (m_owner->m_notified > nbThreadInactive) {
      	    m_owner->m_notified = nbThreadInactive;
      	  }
      	}

      	if (m_owner->m_notified > 0) {
          m_owner->m_poolMutex->signal();
      	}

        m_owner->m_poolMutex->release();
      }
      catch (castor::exception::Exception any) {
        /* just ignore for this loop all mutex errors and try again */
        try {
          m_owner->m_poolMutex->release();
        } catch(...) {}
      }
    }

  /* And we continue for ever */
  }

  }
  catch (castor::exception::Exception any) {
    if (s >= 0) {
      netclose(s);
    }

    #if defined(_WIN32)
      WSACleanup();
    #endif

    try {
      m_owner->m_poolMutex->release();
    } catch(...) {}

    m_owner->clog() << WARNING << any.getMessage().str() << std::endl;
  }
}

