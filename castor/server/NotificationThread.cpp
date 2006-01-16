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
 * @(#)$RCSfile: NotificationThread.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/01/16 10:09:48 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/NotificationThread.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::NotificationThread::NotificationThread() :
  m_owner(0)
{
};

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
//castor::server::NotificationThread::~NotificationThread() throw()
//{
//}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::NotificationThread::init(void* param)
  m_owner = (SignalThreadPool*)param;
};

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::NotificationThread::run() throw()
{
  try {

  int s = -1;
  int port;
  struct sockaddr_in serverAddress, clientAddress;
  int on = 1;	/* for REUSEADDR */
  int ibind;
#if defined(_WIN32)
  WSADATA wsadata;
#endif
#define MAX_BIND_RETRY 5

#if defined(_WIN32)
  if (WSAStartup(MAKEWORD (2, 0), &wsadata)) {
    serrno = SEINTERNAL;
    return(NULL);
  }
#endif

  /* Get notification port */
  if (single_service_configNotifyPort(&port) != 0) {
    goto single_service_notifyThreadReturn;
  }

  /* Create a socket */
  if ((s = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    goto single_service_notifyThreadReturn;
  }
  memset ((char *)&serverAddress, 0, sizeof(serverAddress));
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddress.sin_port = htons(port);
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));

  /* Bind on it - we know that the port can be reused but not always immediately */
  ibind = 0;
  while (1) {
    ++ibind;
    if (bind(s, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) < 0) {
      if (ibind >= MAX_BIND_RETRY) {
	goto single_service_notifyThreadReturn;
      }
    } else {
      break;
    }
  }

  /* Say to our parent that we successfully started */
  if (Cthread_mutex_timedlock_ext(single_service_serviceLockCthreadStructure,SINGLE_SERVICE_MUTEX_TIMEOUT) != 0) {
    goto single_service_notifyThreadReturn;
  }

  singleService.nbNotifyThreads = 1;
  if (Cthread_cond_signal_ext(single_service_serviceLockCthreadStructure) != 0) {
    singleService.nbNotifyThreads = 0;
    goto single_service_notifyThreadReturn;
  }
  if (Cthread_mutex_unlock_ext(single_service_serviceLockCthreadStructure) != 0) {
    singleService.nbNotifyThreads = 0;
    goto single_service_notifyThreadReturn;
  }

  /* Wait for a notification */
  while(1) {
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

      if (magic != SINGLE_SERVICE_NOTIFY_MAGIC) {
	/* Not a packet for us (!?) */
	continue;
      }

      /* Okay - we have received a notification - we signal our condition variable */
      if (Cthread_mutex_timedlock_ext(single_service_serviceLockCthreadStructure,SINGLE_SERVICE_MUTEX_TIMEOUT) == 0) {
	int NbThreadInactive;

	singleService.notified += nb_thread_wanted;

	/* We make sure that 0 <= singleService.notified <= NbThreadInactive */
	if (singleService.notified < 0) {
	  /* Impossible */
	  singleService.notified = 1;
	}
	NbThreadInactive = singleService.nbTotalThreads - singleService.nbActiveThreads;
	if (NbThreadInactive < 0) {
	  /* Impossible */
	  NbThreadInactive = 0;
	}
	if (NbThreadInactive == 0) {
	  /* All threads are already busy : try to get one couting on timing windows */
	  singleService.notified = 1;
	} else {
	  if (singleService.notified > NbThreadInactive) {
	    singleService.notified = NbThreadInactive;
	  }
	}

	if (singleService.notified > 0) {
	  Cthread_cond_signal_ext(single_service_serviceLockCthreadStructure);
	}
	Cthread_mutex_unlock_ext(single_service_serviceLockCthreadStructure);
      }
    }

    /* And we continue */
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
      owner->getMutex()->release();
    } catch(...) {}
    // LOG
  }
}

