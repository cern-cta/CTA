/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: expertd.c,v $ $Revision: 1.4 $ $Date: 2005/01/07 09:18:01 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/time.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif
#include "Cinit.h"
#include "Cnetdb.h"
#include "Cpool_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "expert.h"
#include "expert_daemon.h"

#define MAXSTRLENGTH 33
int being_shutdown;
char db_pwd[MAXSTRLENGTH];
char db_srvr[MAXSTRLENGTH];
char db_user[MAXSTRLENGTH];
char func[16];
int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;

expertd_main(main_args)
struct main_args *main_args;
{
  int c;
  FILE *fopen(), *cf;
  char cfbuf[80];
  void *doit(int);
  char domainname[CA_MAXHOSTNAMELEN+1];
  struct sockaddr_in from;
  int fromlen = sizeof(from);
  char *getconfent();
  int i;
  int ipool;
  int on = 1; /* for REUSEADDR */
  char *p;
  char *p_p, *p_s, *p_u;
  fd_set readfd, readmask;
  int rqfd;
  int s;
  struct sockaddr_in sin;
  struct servent *sp;
  int thread_index;
  struct timeval timeval;
  char dlfconfigfile[CA_MAXPATHLEN+1];
  pid_t c_pid;
  char prtbuf[256];

  jid = getpid();
  strcpy (func, "expertd");
  explogit (func, "started\n");
  gethostname (localhost, CA_MAXHOSTNAMELEN+1);
  if (strchr (localhost, '.') == NULL) {
    if (Cdomainname (domainname, sizeof(domainname)) < 0) {
      explogit (func, "Unable to get domainname\n");
      exit (EXP_SYERR);
    }
    strcat (localhost, ".");
    strcat (localhost, domainname);
  }

  FD_ZERO (&readmask);
  FD_ZERO (&readfd);
#if ! defined(_WIN32)
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
#endif

  /* open request socket */

  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    explogit (func, EXP02, "socket", neterror());
    return (EXP_CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
  if ((p = getenv ("EXPERT_PORT")) || (p = getconfent ("EXPERT", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if (sp = getservbyname ("expert", "tcp")) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)EXPERT_PORT);
  }

  sprintf(prtbuf, "port: %d\n", ntohs(sin.sin_port));
  explogit(func, prtbuf);

  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    explogit (func,  "setsockopt error\n");
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    explogit (func, EXP02, "bind", neterror());
    return (EXP_CONFERR);
  }
  if (listen (s, 5) != 0) {
    explogit (func, "listen error\n");
    return (EXP_CONFERR);
  }

  FD_SET (s, &readmask);

  /* main loop */
  while (1) {

    if (FD_ISSET (s, &readfd)) {
      FD_CLR (s, &readfd);
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
      /* Fork new handler */
      c_pid = fork();
      if (c_pid < 0) {
	explogit (func, "can not fork\n");
	goto cont;
      }
      if (c_pid == (pid_t)0) {
	/* This is a child */
	if (s !=0 && s !=1)
	  close (s);
	doit(rqfd);
	exit (0);
      }
      /* This is a parent */
      close (rqfd);

   }
  cont:
    /* Check to see whether any child process has finished */
    while ((c_pid = waitpid((pid_t)-1, (int *) 0, WNOHANG)) > 0) {
    }
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CHECKI;
    timeval.tv_usec = 0;
    if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
      FD_ZERO (&readfd);
    }
  }
}

main()
{
#if ! defined(_WIN32)
  if ((maxfds = Cinitdaemon ("expertd", NULL)) < 0)
    exit (EXP_SYERR);
  exit (expertd_main (NULL));
#else
  if (Cinitservice ("expert", &expertd_main))
    exit (EXP_SYERR);
#endif
}

void *
doit(s)
int s;
{
  int c;
  char *clienthost;
  int magic;
  char* req_data;
  int req_type = 0;
  int data_len;

  if ((c = getreq (s, &magic, &req_type,
                   &req_data, &data_len, &clienthost)) == 0)
    procreq (magic, req_type, req_data, data_len, clienthost, s);
  else if (c > 0)
    sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, c);
  else
    netclose (s);
  return (NULL);
}

getreq(s, magic, req_type, req_data, data_len, clienthost)
     int s;
     int *magic;
     int *req_type;
     char **req_data;
     int *data_len;
     char **clienthost;
{
  struct sockaddr_in from;
  int fromlen = sizeof(from);
  struct hostent *hp;
  int l;
  int msglen;
  int n;
  char *rbp;
  char req_hdr[3*LONGSIZE];

  l = netread_timeout (s, req_hdr, sizeof(req_hdr), EXP_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG (rbp, n);
    *magic = n;
    unmarshall_LONG (rbp, n);
    *req_type = n;
    unmarshall_LONG (rbp, msglen);

    l = msglen - sizeof(req_hdr);
    *data_len = l;
    *req_data = (char*)malloc(l);
    if (*req_data == NULL) {
      explogit (func, "memory allocation failure\n");
      return (ENOMEM);
    }
    n = netread_timeout (s, *req_data, l, EXP_TIMEOUT);

    if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
      explogit (func, EXP02, "getpeername", neterror());
      free(*req_data);
      return (SEINTERNAL);
    }
    hp = Cgethostbyaddr ((char *)(&from.sin_addr),
                         sizeof(struct in_addr), from.sin_family);
    if (hp == NULL)
      *clienthost = inet_ntoa (from.sin_addr);
    else
      *clienthost = hp->h_name ;
    return (0);
  } else {
    if (l > 0)
      explogit (func, EXP04, l);
    else if (l < 0) {
      explogit (func, EXP02, "netread", strerror(errno));
    }
    return (SEINTERNAL);
  }
}


procreq(magic, req_type, req_data, data_len, clienthost, s)
     int magic;
     int req_type;
     char *req_data;
     int data_len;
     char *clienthost;
     int s;
{
  int c;

  switch (req_type) {

  case EXP_EXECUTE:
  case EXP_RQ_FILESYSTEM:
  case EXP_RQ_MIGRATOR:
  case EXP_RQ_RECALLER:
  case EXP_RQ_GC:
  case EXP_RQ_REPLICATION:
    c = exp_srv_execute (req_type, magic, req_data, clienthost, s);
    break;

  default:
    explogit (func, EXP98, "illegal request\n");
    sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, EEXPILLREQ);
    c = SEINTERNAL;
    break;
  }
  /*
        Free memory allocated by getreq.
  */
  free (req_data);
  return (0);
}
