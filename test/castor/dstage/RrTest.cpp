/******************************************************************************
 *                      RrTest.cpp
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
 * @(#)$RCSfile: RrTest.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/09/17 09:09:07 $ $Author: bcouturi $
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/

/**
 * This program allows to stress test the RequestReplier CASTOR component.
 * It can be run as client or server: When specified withour parameter, 
 * it is run as server. When specifying an IP address on the command line,
 * it is run in client mode connecting to the server specified.
 *
 * It consists in a simple server, accepting requests from the client, 
 * and replying to client using the request replier.
 */


#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include "marshall.h"
#include "net.h"
#include <errno.h>
#include "serrno.h"
#include "Cpool_api.h"

#include "castor/Constants.hpp"
#include "castor/BaseObject.hpp"
#include "castor/IObject.hpp"
#include "castor/dstage/RequestReplier.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/rh/EndResponse.hpp"
#include "castor/rh/StringResponse.hpp"
#include "castor/rh/Client.hpp"

#include "RrTest.hpp"


#define PORT 25177
#define BSIZE 100
#define NB_THREADS 1

void *procreq(void *param);

class Request {
 public:
  int fd;
  struct sockaddr_in clientAddress;
};


void server_signalhandler(int sig) {
  if (sig == SIGINT || sig == SIGTERM) {
    printf("%d> SIGINT or SIGTERM received, exiting !\n", getpid());
    exit(0);
  } else if (sig == SIGPIPE) {
    printf("%d> SIGPIPE received !\n", getpid());
  } else if (sig == SIGCHLD) {
    int rc = 1;
    printf("%d> SIGCHLD received, waiting 4 zombies !\n", getpid());
    while ((rc = waitpid(-1, NULL, WNOHANG)) > 0) {
      printf("%d> child %d exited!\n", getpid(), rc);
    };
  }
}

void client_signalhandler(int sig) {
  fprintf(stderr, "Handler called: %d\n", sig);
  fprintf(stderr, "INT: %d, TERM: %d, PIPE: %d\n", SIGINT, SIGTERM, SIGPIPE);
  
  if (sig == SIGINT || sig == SIGTERM) {
    exit(0);
  }    
}


test::RrTest::RrTest() {
  initLog("rrtestlog", castor::SVC_STDMSG);
}

int test::RrTest::start() {

  int sd, rc;
  struct sockaddr_in serverAddress;
  struct sockaddr_in clientAddress;
  int len;
  int on = 1;
  int yes = 1;
  int ret_flags = 0;
  int i;
  int poolid, nbthreads = -1;

  signal(SIGPIPE, server_signalhandler);
  signal(SIGCHLD, server_signalhandler);
  signal(SIGTERM, server_signalhandler);
  signal(SIGINT, server_signalhandler);

  poolid = Cpool_create(NB_THREADS, &nbthreads);
  if (poolid < 0) {
    fprintf(stderr, "POOL CREATION error: %s !\n", sstrerror(serrno));
    return -1;    
  }

  /* printf("<%d> Pool created succesfully id:%d Nb threads:%d\n",
     getpid(),
     poolid,
     nbthreads); */

    
  /* Creating the socket */
  if ( (sd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    fprintf(stderr, "OPEN error: %s !\n", strerror(errno));
    return -1;
  }

  if (setsockopt(sd,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
    fprintf(stderr, "CLIENT setsockopt error: %s !\n", strerror(errno));
  }

    
  if (setsockopt(sd,SOL_SOCKET,SO_REUSEADDR, (char *) &on,sizeof(on))<0) {
    fprintf(stderr, "Could not setsockopt\n");
  }
    
  /* Binding the socket to the local server port */
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddress.sin_port = htons(PORT);
  
  if ((rc = bind(sd, (struct sockaddr *)&serverAddress,
		 sizeof(serverAddress))) < 0) {
    fprintf(stderr, "BIND error: %s\n", strerror(errno));
    return -1;
  }

    
  len = sizeof(struct sockaddr_in);    
  if (( rc = listen(sd, 10)) < 0) {
    fprintf(stderr, "LISTEN error: %s\n", strerror(errno));
    return -1;
  }
  
  //fprintf(stderr, "<%d> Waiting for connections\n", getpid());

  for (;;) {
    int csd;
    Request *req = new Request();
    len = sizeof(struct sockaddr_in);    
    if ((csd = accept(sd, (struct sockaddr *)&(req->clientAddress),
		      (socklen_t *)&len)) < 0) {
      fprintf(stderr, "ACCEPT error:%d  %s\n", csd,  strerror(errno));
      return -1;
    };
    req->fd = csd;
    Cpool_assign(poolid, procreq, req, -1);
  }
}


void *procreq(void *param) {

  char buf[12];
  int magic=0;
  int port=0;
  int data=0;
  int rc;

  if (param == NULL) {
    return NULL;
  }

 Request *req = (Request *)param;
 int csd = req->fd;
 struct sockaddr_in clientAddress = req->clientAddress;

 free(req);

 //fprintf(stderr, "<%d> Accepted new connections fd:%d\n", getpid(), csd);
  
  /* Reading the data */
  rc = recv(csd, buf, sizeof(buf), 0);
  if (rc < 0) {
    fprintf(stderr, "<%d> Could not read message: %s\n", getpid(),  strerror(errno));
    return NULL;
  }
  
  char *p = buf;
  unmarshall_LONG(p, magic);
  unmarshall_LONG(p, port);
  unmarshall_LONG(p, data);

  int ipAddress = clientAddress.sin_addr.s_addr;
  printf("<%d> Client magic:%xd data: %d waiting on %s:%d\n", getpid(), magic, data, inet_ntoa(clientAddress.sin_addr), port);

  close(csd);

  try {
    char tmp[30];
    sprintf(tmp, "%d", data);
    castor::rh::StringResponse response;
    response.setContent(tmp);
    castor::rh::EndResponse response2;

    // Creating a client on the free object store tp pass it to the request replier
    castor::rh::Client *cl = new castor::rh::Client();
    cl->setIpAddress(ntohl(ipAddress));
    cl->setPort(port);

    castor::dstage::RequestReplier *rr =
      castor::dstage::RequestReplier::getInstance();
    for(int j=0; j<10; j++) {
      rr->replyToClient(cl, &response);
      sleep(1);
    }

    rr->replyToClient(cl, &response2, true);
    delete(cl);
  } catch (castor::exception::Exception e) {
    std::cerr << "Exception while replying : "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;
  }
  return NULL; 
}


////////////////////////////////////////////////////////
//
//      TEST CLIENT CODE
//
////////////////////////////////////////////////////////

test::RrTestClient::RrTestClient(char *srvIp): 
  mListenPort(-1), mSocket(-1), mChallenge(-1) {

    int yes = 1;
    int on = 1;
    int sd, rc, len;
    struct sockaddr_in address;
    unsigned int soutlen = sizeof(struct sockaddr_in);
    struct sockaddr_in sout;

    mSrvIp = srvIp;
    mChallenge = getpid();
    
    /* Creating the socket */
    if ( (sd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      fprintf(stderr, "CLIENT Socket creation error: %s !\n", strerror(errno));
      exit(1);
    }
    
    if (setsockopt(sd,SOL_SOCKET,SO_REUSEADDR, (char *) &on,sizeof(on))<0) {
      fprintf(stderr, "CLIENT Could not setsockopt SO_REUSE: %s\n", strerror(errno));
    }

    if (setsockopt(sd,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
      fprintf(stderr, "CLIENT setsockopt error: %s !\n", strerror(errno));
    }
    
    /* Binding the socket to a local port */
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = 0;
    if ((rc = bind(sd, (struct sockaddr *)&address,
		   sizeof(address))) < 0) {
      fprintf(stderr, "CLIENT BIND error: %s\n", strerror(errno));
      exit(1);
    }
    
    /* Now starting to listen to server callbacks ! */
    if (( rc = ::listen(sd, 10)) < 0) {
      fprintf(stderr, "CLIENT LISTEN error: %s\n", strerror(errno));
      exit(1);
    }
    
    if (getsockname(sd, (struct sockaddr*)&sout, &soutlen) < 0) {
      fprintf(stderr, "CLIENT getsockname error: %s\n", strerror(errno));
      exit(1);
    }

    mListenPort = ntohs(sout.sin_port);
    mSocket = sd;    
    
    fprintf(stderr, "<%d> Listening port is %d (socket %d)\n", getpid(), mListenPort, mSocket);

}


int test::RrTestClient::start() {

  int sd, rc;
  struct sockaddr_in serverAddress;
  int len;
  int ret_flags = 0;
  int i;
  
  signal(SIGPIPE, client_signalhandler );
  signal(SIGTERM, client_signalhandler);
  signal(SIGINT,  client_signalhandler);
  signal(SIGHUP,  client_signalhandler);
  
  
  /* Creating the socket */
  if ( (sd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    fprintf(stderr, "CLIENT OPEN error: %s !\n", strerror(errno));
    exit(1);
  }
  
  /* Binding the socket to the local server port */
  memset(&serverAddress, 0, sizeof(serverAddress));
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_addr.s_addr = inet_addr(mSrvIp);
  serverAddress.sin_port = htons(PORT);
  
  len = sizeof(serverAddress);
  if ((rc = connect(sd, (struct sockaddr *)&serverAddress, len)) < 0) {
    fprintf(stderr, "CLIENT CONNECT error: %s\n",  strerror(errno));
    exit(1);
  };
    
  char buf[12];
  char *p = buf;

  marshall_LONG(p, 0xCA5106);
  marshall_LONG(p, mListenPort);
  marshall_LONG(p, mChallenge);

  rc = send(sd, buf, sizeof(buf), 0);
  if (rc < 0) {
    fprintf(stderr, "CLIENT send error: %s\n",  strerror(errno));
    exit(1);
  }

  close(sd);

  //std::cout << "<" << getpid() << "> Request sent, now listening for server response!" << std::endl;

  if (listen() != 0) {
    std::cerr << "Listen error" << std::endl;
    exit(1);
  }
  
  return 0;
}



int test::RrTestClient::listen() {

  struct sockaddr_in serverAddress;
  int len, rc;
  int ret_flags = 0;
  char buf[8];
  int magic;
  int port;

  len = sizeof(struct sockaddr_in);    

  int csd;
  
  //fprintf(stderr, "<%d> Waiting for new connections on Socket %d\n", getpid(), mSocket);
  /* Listening for connections */
  if ((csd = accept(mSocket, (struct sockaddr *)&serverAddress,
		    (socklen_t *)&len)) < 0) {
    fprintf(stderr, "ACCEPT error:%d/%d  %s\n", mSocket, csd,  strerror(errno));
    return -1;
  };
  
  //fprintf(stderr, "<%d> Accepted new connections\n", getpid());
  //printf("<%d> Recontacted by server %s\n", getpid(), inet_ntoa(serverAddress.sin_addr));
  //sleep(1);

  castor::io::ClientSocket s(csd);

  for(;;) {
    try {
      castor::IObject* result = s.readObject();

      if (castor::OBJ_EndResponse == result->type()) {
	std::cout << "Received END Response" << std::endl;
	break;
      } else {
	// cast response into Response*
	castor::rh::StringResponse* res =
	  dynamic_cast<castor::rh::StringResponse*>(result);
	if (0 == res) {
	  std::cerr << "Receive bad response type :"
		    << result->type();
	  delete res;
	}
	std::string txt =  res->content();
	//std::cout << "Received \"" << res->content() << "\"" << std::endl;
	if (atoi(txt.c_str()) != mChallenge) {
	  std::cout << "STATUSNOK Bad response " << res->content() << std::endl;
	} else {
	  std::cout << "STATUSOK " << res->content() << std::endl;
	}
      }
    }  catch (castor::exception::Exception e) {
      std::cerr << "Exception while reading object : "
		<< sstrerror(e.code()) << std::endl
		<< e.getMessage().str() << std::endl;
      exit(1);
    }
  }
  close(csd); /* Not needed, closed by the object */
  close(mSocket);
  return 0;
}


test::RrTestClient::~RrTestClient() {
  close(mSocket);
}

void *clientProcreq(void *param) {
  std::cout << "In clientProcreq" << std::endl; 
  
  for(;;) {
    std::cout << "NEWREQ" << std::endl; 
    test::RrTestClient rrtc((char *)param);
    rrtc.start();
    break;
  }
}

int main(int argc, char *argv[]) {

  if (argc <= 1) {
    // We are in server mode
   test::RrTest rrt;
    rrt.start();
  } else {
//     // We are in cliden mode
//     int nbthreads;
//     int poolid = Cpool_create(NB_THREADS, &nbthreads);
//     if (poolid < 0) {
//       fprintf(stderr, "POOL CREATION error: %s !\n", sstrerror(serrno));
//       return -1;    
//     }
//     std::cout << "Client pool created ok " << poolid << "/" << nbthreads <<  std::endl;
//     for(int i=0; i<NB_THREADS; i++) {
//       std::cout << "Assigning thread " << i  << " TO " << argv[1] << std::endl;
//       Cpool_assign(poolid, clientProcreq, argv[1], -1);    
//     }
//     for(;;) {
//       sleep(1000);
//     }
    clientProcreq(argv[1]);

  }
  return 0;

}
