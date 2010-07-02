/*
 * $Id: connect.c,v 1.22 2009/06/30 12:34:37 waldron Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* connect.c    Remote File I/O - connect to remote server              */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <syslog.h>             /* system logger                        */
#include "rfio.h"               /* remote file I/O definitions               */
#include <Cglobals.h>  /* thread local storage for global variables */
#include <Cnetdb.h>  /* thread-safe network database routines */
#include <osdep.h>
#include "Csec_api.h"
#include "Castor_limits.h"
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <common.h>
#include <string.h>

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

extern char     *getconfent();
extern char     *getenv();      /* get environmental variable value     */
char *rfio_lasthost (); /* returns last succesfully connected host     */
int rfio_newhost (char *); /* returns last succesfully connected host     */
int rfio_connect_with_port (char*,int,int*); /* Connect <node>'s rfio server on port <port> */

/** Parses the node name to check whether it contains the port number as well */
int rfio_nodeHasPort(char *node, char *host, int *port) {
  char *pos;
  char *nodecp;
  int tmpport;


  nodecp = strdup(node);
  if (nodecp == NULL) {
    serrno = ENOMEM;
    return -1;
  }

  pos = strchr(nodecp, ':');
  if (pos == NULL) {
    return 0;
  }

  if (pos - nodecp > 0) {
    *pos = '\0';
    strncpy(host, nodecp,CA_MAXHOSTNAMELEN);
    host[CA_MAXHOSTNAMELEN] = '\0';
  }

  tmpport =  atoi(pos+1);
  *port = tmpport;

  free(nodecp);
  return 1;
}

static int  last_host_key = -1; /* key to hold the last connect host name in TLS */

/* Connect <node>'s rfio server */
int rfio_connect(char    *node,                  /* remote host to connect               */
                 int     *remote)              /* connected host is remote or not      */
{
  return(rfio_connect_with_port(node,-1,remote));
}

/* Connect <node>'s rfio server on port <port> */
int rfio_connect_with_port(char    *node,                  /* remote host to connect               */
                           int port,                       /* port to use */
                           int     *remote)              /* connected host is remote or not      */
{
  register int    s;      /* socket descriptor                    */
  struct hostent  *hp;    /* host entry pointer                   */
  struct sockaddr_in sin; /* socket address (internet) struct     */
  char    *host;          /* host name chararcter string          */
  char    *p, *cp;        /* character string pointers            */
  register int    retrycnt; /* number of NOMORERFIO retries       */
  register int    retryint; /* interval between NOMORERFIO retries*/
  register int    crtycnt = 0; /* connect retry count             */
  register int    crtyint = 0; /* connect retry interval          */
  register int    crtyattmpt = 0; /* connect retry attempts done  */
  register int    crtycnts = 0 ;
  struct    stat statbuf ; /* NOMORERFIO stat buffer              */
  char    nomorebuf1[BUFSIZ], nomorebuf2[BUFSIZ]; /* NOMORERFIO buffers */
  char *last_host = NULL;
  int   last_host_len = 256;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif
  int timeout;
  int secure_connection = 0;
  char tmphost[CA_MAXHOSTNAMELEN+1];

  INIT_TRACE("RFIO_TRACE");

  /*BC First parse the node name to check whether it contains the port*/
  {
    int rc;

    rc = rfio_nodeHasPort(node, tmphost, &port);
    if (rc == 1) {
      TRACE(2, "rfio", "rfio_connect: Hostname includes port(%s, %s, %d)",
            node, tmphost, port);
      node = tmphost;
    }
  }

  /*BC End parse the node name to check whether it contains the port*/

  /*
   * Should we use an alternate name ?
   */

  /*
   * Under some circumstances (heavy load, use of inetd) the server fails
   * to accept a connection. A simple retry mechanism is therefore
   * implemented here.
   */
  if ( rfioreadopt(RFIO_NETRETRYOPT) != RFIO_NOTIME2RETRY ) {
    /*
     * If the retry count option is not specified use the default from
     * the config file. This option is used by the TACOS slave. IN2P3
     */
    crtycnt = rfioreadopt(RFIO_CONNECT_RETRY_COUNT_OPT);
    if ( crtycnt <= 0 ) {
      if ( (p = getenv("RFIO_CONRETRY")) != NULL ||
           (p = getconfent("RFIO", "CONRETRY", 0)) != NULL ) {
        if ((crtycnt = atoi(p)) <= 0) {
          crtycnt = 0;
        }
      }
    }
    serrno = 0 ;
    crtyint = rfioreadopt(RFIO_CONNECT_RETRY_INT_OPT);
    if ( crtyint <= 0 ) {
      if ( (p = getenv("RFIO_CONRETRYINT")) != NULL ||
           (p = getconfent("RFIO", "CONRETRYINT", 0)) != NULL) {
        if ((crtyint = atoi(p)) <= 0) {
          crtyint = 0;
        }
      }
    }
  }
  crtycnts = crtycnt ;
  /*
   * When the NOMORERFIO file exists, or if NOMORERFIO.host file exists,
   * the RFIO service is suspended. By default it will retry for ever every
   * DEFRETRYINT seconds.
   */
  if ( (p = getenv("RFIO_RETRY")) == NULL &&
       (p=getconfent("RFIO", "RETRY", 0)) == NULL) {
    retrycnt=DEFRETRYCNT;
  }
  else {
    retrycnt=atoi(p);
  }
  if ( (p = getenv("RFIO_RETRYINT")) == NULL &&
       (p=getconfent("RFIO", "RETRYINT", 0)) == NULL) {
    retryint=DEFRETRYINT;
  }
  else {
    retryint=atoi(p);
  }

  if ( (p = getenv("RFIO_CONNTIMEOUT")) == NULL &&
       (p = getconfent("RFIO", "CONNTIMEOUT", 0)) == NULL) {
    timeout=DEFCONNTIMEOUT;
  }
  else {
    timeout=atoi(p);
  }
  if ((p = getenv("SECURE_CASTOR"))) {
    if (strcasecmp(p, "YES") == 0)
      secure_connection++;
  }
  if (port < 0) {
    if (secure_connection) { /* Secure connection should be made to secure port */
      /* Try environment variable */
      TRACE(2, "srfio", "rfio_connect: getenv(%s)","SRFIO_PORT");
      if ((p = getenv("SRFIO_PORT")) != NULL) {
        TRACE(2, "srfio", "rfio_connect: *** Warning: using port %s", p);
        sin.sin_port = htons(atoi(p));
      } else {
        /* Try CASTOR configuration file */
        TRACE(2, "srfio", "rfio_connect: getconfent(%s,%s,0)","SRFIO","PORT");
        if ((p = getconfent("SRFIO","PORT",0)) != NULL) {
          TRACE(2, "srfio", "rfio_connect: *** Warning: using port %s", p);
          sin.sin_port = htons(atoi(p));
        } else {
          /* Use default port number */
	  TRACE(2, "srfio", "rfio_connect: using default port number %d", (int) SRFIO_PORT);
	  sin.sin_port = htons((u_short) SRFIO_PORT);
        }
      }
    } else {
      /* Connection is unsecure */
      /* Try environment variable */
      TRACE(2, "rfio", "rfio_connect: getenv(%s)","RFIO_PORT");
      if ((p = getenv("RFIO_PORT")) != NULL) {
        TRACE(2, "rfio", "rfio_connect: *** Warning: using port %s", p);
        sin.sin_port = htons(atoi(p));
      } else {
        /* Try CASTOR configuration file */
        TRACE(2, "rfio", "rfio_connect: getconfent(%s,%s,0)","RFIO","PORT");
        if ((p = getconfent("RFIO","PORT",0)) != NULL) {
          TRACE(2, "rfio", "rfio_connect: *** Warning: using port %s", p);
          sin.sin_port = htons(atoi(p));
        } else {
          /* Use default port number */
	  TRACE(2, "rfio", "rfio_connect: using default port number %d", (int) RFIO_PORT);
	  sin.sin_port = htons((u_short) RFIO_PORT);
        }
      }
    }
  } else {
    TRACE(2, "rfio", "rfio_connect: *** Warning: using forced port %d", port);
    sin.sin_port = htons(port);
  }
  sin.sin_family = AF_INET;

  /* Warning : the return value of getconfent is a pointer to a thread-specific     */
  /* content - overwriten at each getconfent. If we do not want to have a corrupted */
  /* host value, the following call to getconfent have to be the last one.          */
  /* Note that subsequent calls to getconfent after this routine are not of concern */
  /* because we return a socket value, and do not depend afterwards on this static  */
  /* thread-specific address used in getconfent().                                  */
  if ( rfioreadopt(RFIO_NETOPT) != RFIO_NONET ) {
    if ((host = getconfent("NET",node,1)) == NULL) {
      host = node;
    }
    else {
      TRACE(3,"rfio","set of hosts: %s",host);
    }
  }
  else {
    host = node;
  }

  serrno = 0; /* reset the errno could be SEENTRYNFND */
  rfio_errno = 0;

  TRACE(1, "rfio", "rfio_connect: connecting(%s)",host);

  cp = strtok(host," \t") ;
  if (cp == NULL ) {
    TRACE(1,"rfio","host specified incorrect");
    serrno = SENOSHOST;
    END_TRACE();
    return(-1);
  }

 conretryall:
  TRACE(2, "rfio", "rfio_connect: Cgethostbyname(%s)", cp);
  hp = Cgethostbyname(cp);
  if (hp == NULL) {
    if (strchr(cp,'.') != NULL) { /* Dotted notation */
      TRACE(2, "rfio", "rfio_connect: using %s", cp);
      sin.sin_addr.s_addr = htonl(inet_addr(cp));
    }
    else {
      TRACE(2, "rfio", "rfio_connect: %s: no such host",cp);
      serrno = SENOSHOST;     /* No such host                 */
      END_TRACE();
      return(-1);
    }
  }
  else {
    sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
  }

  strcpy(nomorebuf1, NOMORERFIO);
  sprintf(nomorebuf2, "%s.%s", nomorebuf1, cp);
 retry:
  if (!stat(nomorebuf1,&statbuf)) {
    if (retrycnt-- >=0) {
      syslog(LOG_ALERT, "rfio: connect: all RFIO service suspended (pid=%d)\n", getpid());
      sleep(retryint);
      goto retry;
    } else {
      syslog(LOG_ALERT, "rfio: connect: all RFIO service suspended (pid=%d), retries exhausted\n", getpid());
      serrno=SERTYEXHAUST;
      return(-1);
    }
  }
  if (!stat(nomorebuf2, &statbuf)) {
    if (retrycnt-- >=0) {
      syslog(LOG_ALERT, "rfio: connect: RFIO service to <%s> suspended (pid=%d)\n", cp, getpid());
      sleep(retryint);
      goto retry;
    } else {
      syslog(LOG_ALERT, "rfio: connect: RFIO service to <%s> suspended (pid=%d), retries exhausted\n", cp, getpid());
      serrno=SERTYEXHAUST;
      return(-1);
    }
  }

 conretry:
  TRACE(2, "rfio", "rfio_connect: socket(%d, %d, %d)",
        AF_INET, SOCK_STREAM, 0);
  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    TRACE(2, "rfio", "rfio_connect: socket(): ERROR occured (%s)", neterror());
    END_TRACE();
    return(-1);
  }
  TRACE(2, "rfio", "rfio_connect: netconnect_timeout(%d, %x, %d, %d)", s, &sin, sizeof(struct sockaddr_in),timeout);
  if (netconnect_timeout(s, (struct sockaddr *)&sin, sizeof(struct sockaddr_in), timeout) < 0)  {
    TRACE(2, "rfio", "rfio_connect: connect(): ERROR occured (%s)", neterror());
      if (errno == ECONNREFUSED || serrno == ECONNREFUSED)
	{
	  syslog(LOG_ALERT, "rfio: connect: %d failed to connect %s", getpid(), cp);
	  if (crtycnt-- > 0) {
	    if (crtyint) sleep(crtyint);
	    syslog(LOG_ALERT, "rfio: connect: %d retrying to connect %s", getpid(), cp);
	    /*
	     * connect() returns "Invalid argument when called twice,
	     * so socket needs to be closed and recreated first
	     */

	    (void) close(s);
	    crtyattmpt ++ ;
	    goto conretry;
	  }
	  if ( ( cp = strtok(NULL," \t")) != NULL ) {
	    crtycnt =  crtycnts ;
	    syslog(LOG_ALERT, "rfio: connect: after ECONNREFUSED, changing host to %s", cp) ;
	    TRACE(3,"rfio","rfio: connect: after ECONNREFUSED, changing host to %s", cp) ;
	    (void) close(s);
	    goto conretryall;
	  }
	}
      if (errno==EHOSTUNREACH || errno==ETIMEDOUT || serrno == SETIMEDOUT )
	{
	  if ( ( cp = strtok(NULL," \t")) != NULL ) {
	    crtycnt =  crtycnts ;
	      if (errno == EHOSTUNREACH)
		syslog(LOG_ALERT, "rfio: connect: after EHOSTUNREACH, changing host to %s", cp);
	      else
		syslog(LOG_ALERT, "rfio: connect: after ETIMEDOUT, changing host to %s", cp);

	    (void) close(s);
	    goto conretryall;
	  }

	}
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  TRACE(3,"rfio", "rfio_connect: OK");
  if (crtyattmpt) {
    syslog(LOG_ALERT, "rfio: connect: %d recovered connection after %d secs with %s",
           getpid(), crtyattmpt*crtyint,cp) ;
  }
  TRACE(4,"rfio", "rfio_connect: calling isremote on node %s", node);
  {
    int sav_serrno = serrno;
    *remote = isremote( sin.sin_addr, node ) ;
    serrno = sav_serrno; /* Failure or not os isremote(), we continue */
  }
  TRACE(4,"rfio", "rfio_connect: after isremote");
  Cglobals_get(&last_host_key, (void**)&last_host, last_host_len);
  strcpy(last_host, cp); /* remember to fetch back remote errs     */
  TRACE(2, "rfio", "rfio_connect: connected");
  TRACE(2, "rfio", "rfio_connect: calling setnetio()");
  if (setnetio() < 0) {
    close(s);
    END_TRACE();
    return(-1);
  }

  TRACE(1, "rfio", "rfio_connect: return socket %d", s);
  if (secure_connection) {
    /* Performing authentication */
    {
      Csec_context_t ctx;
      int rc;

      TRACE(1, "srfio", "Going to establish security context !");
      if (Csec_client_initContext(&ctx, CSEC_SERVICE_TYPE_HOST, NULL) <0) {
        TRACE(2, "srfio", "Could not initiate context: %s", Csec_getErrorMessage());
      }

      rc = Csec_client_establishContext(&ctx, s);
      if (rc != 0) {
        TRACE(2, "srfio", "Could not establish context: %s", Csec_getErrorMessage());
        close(s);
        END_TRACE();
        return(-1);
      }

      Csec_clearContext(&ctx);

      TRACE(1, "srfio", "client establish context returned %s", rc);
    }
  }
  END_TRACE();
  return(s);
}

char    *rfio_lasthost() /* returns last succesfully connected host     */
{
  char *last_host = NULL;
  int   last_host_len = 256;
  Cglobals_get(&last_host_key, (void**)&last_host, last_host_len);
  TRACE(4, "rfio", "connect: last_host_name: %s", last_host);
  return(last_host);
}

/* returns last succesfully connected host     */
int rfio_newhost(char *newhost)
{
  char *last_host = NULL;
  int   last_host_len = 256;

  if (newhost == NULL || newhost[0] == '\0') return(-1);

  Cglobals_get(&last_host_key, (void**)&last_host, last_host_len);
  TRACE(4, "rfio", "connect: last_host_name: changed from %s to %s", last_host, newhost);
  strncpy(last_host, newhost, 256);
  last_host[255] = '\0';
  return(0);
}
