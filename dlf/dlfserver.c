/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlfserver.c,v $ $Revision: 1.6 $ $Date: 2004/01/28 15:05:04 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
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
#include "dlf.h"
#include "dlf_server.h"

#define MAXSTRLENGTH 33
int being_shutdown;
char db_pwd[MAXSTRLENGTH];
char db_srvr[MAXSTRLENGTH];
char db_user[MAXSTRLENGTH];
char func[16];
int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;
struct dlf_srv_thread_info dlf_srv_thread_info[DLF_NBTHREADS];

dlf_main(main_args)
     struct main_args *main_args;
{
  int c;
  FILE *fopen(), *cf;
  char cfbuf[80];
  struct dlf_dbfd dbfd;
  void *doit(void *);
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

  char prtbuf[256];

  jid = getpid();
  strcpy (func, "dlfserver");
  dlflogit (func, "started\n");
  gethostname (localhost, CA_MAXHOSTNAMELEN+1);
  if (strchr (localhost, '.') == NULL) {
    if (Cdomainname (domainname, sizeof(domainname)) < 0) {
      dlflogit (func, "Unable to get domainname\n");
      exit (SYERR);
    }
    strcat (localhost, ".");
    strcat (localhost, domainname);
  }
  /* get login info from the dlf config file */

  if (strncmp (DLFCONFIG, "%SystemRoot%\\", 13) == 0 &&
      (p = getenv ("SystemRoot")))
    sprintf (dlfconfigfile, "%s%s", p, strchr (DLFCONFIG, '\\'));
  else
    strcpy (dlfconfigfile, DLFCONFIG);
  if ((cf = fopen (dlfconfigfile, "r")) == NULL) {
    dlflogit (func, "Can't open configuration file: %s\n", dlfconfigfile);
    return (CONFERR);
  }
  if (fgets (cfbuf, sizeof(cfbuf), cf) &&
      strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n"))) {
    strncpy (db_user, p_u, MAXSTRLENGTH-1);
    db_user[MAXSTRLENGTH-1] = 0;
    /* get the password, provided that it may be empty */
    if (cfbuf[strlen(p_u)+1] == '@') {
      /*  empty password */
      *db_pwd = 0;
    } else {
      if ((p_p = strtok (NULL, "@\n")) != NULL) {
        strncpy (db_pwd, p_p, MAXSTRLENGTH-1);
        db_user[MAXSTRLENGTH-1] = 0;
      } else {
        dlflogit (func, "Configuration file %s incorrect\n",
                  dlfconfigfile);
        return (CONFERR);
      }
    }
    if ((p_s = strtok (cfbuf+strlen(db_user)+strlen(db_pwd)+2, "\n")) != NULL) {
      strncpy (db_srvr, p_s, MAXSTRLENGTH-1);
      db_srvr[MAXSTRLENGTH-1] = 0;
    } else {
      dlflogit (func, "Configuration file %s incorrect : no server\n",
                dlfconfigfile);
      return (CONFERR);      
    }
  } else {
    dlflogit (func, "Configuration file %s incorrect : no user name.\n",
              dlfconfigfile);
    return (CONFERR);
  }
  (void) fclose (cf);

  (void) dlf_init_dbpkg ();

  /* create a pool of threads */

  if ((ipool = Cpool_create (DLF_NBTHREADS, NULL)) < 0) {
    dlflogit (func, DLF02, "Cpool_create", sstrerror(serrno));
    return (SYERR);
  }
  for (i = 0; i < DLF_NBTHREADS; i++) {
    dlf_srv_thread_info[i].s = -1;
    dlf_srv_thread_info[i].dbfd.idx = i;
  }

  FD_ZERO (&readmask);
  FD_ZERO (&readfd);
#if ! defined(_WIN32)
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
#endif

  /* open request socket */

  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    dlflogit (func, DLF02, "socket", neterror());
    return (CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
  if ((p = getenv ("DLF_PORT")) || (p = getconfent ("DLF", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if (sp = getservbyname ("dlf", "tcp")) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)DLF_PORT);
  }

  sprintf(prtbuf, "port: %d\n", ntohs(sin.sin_port));
  dlflogit(func, prtbuf);

  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    dlflogit (func,  "setsockopt error\n");
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    dlflogit (func, DLF02, "bind", neterror());
    return (CONFERR);
  }
  if (listen (s, 5) != 0) {
    dlflogit (func, "listen error\n");
    return (CONFERR);
  }

  FD_SET (s, &readmask);

  /* main loop */
  while (1) {
    if (being_shutdown) {
      int nb_active_threads = 0;
      for (i = 0; i < DLF_NBTHREADS; i++) {
        if (dlf_srv_thread_info[i].s >= 0) {
          nb_active_threads++;
          continue;
        }
        if (dlf_srv_thread_info[i].db_open_done)
          (void) dlf_closedb (&dlf_srv_thread_info[i].dbfd);
      }
      if (nb_active_threads == 0)
        return (0);
    }
    if (FD_ISSET (s, &readfd)) {
      FD_CLR (s, &readfd);
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
      if ((thread_index = Cpool_next_index (ipool)) < 0) {
        dlflogit (func, DLF02, "Cpool_next_index", sstrerror(serrno));
        if (serrno == SEWOULDBLOCK) {
          sendrep (rqfd, DLF_RC, serrno);
          continue;
        } else
          return (SYERR);
      }
      dlf_srv_thread_info[thread_index].s = rqfd;
      if (Cpool_assign (ipool, &doit,
                        &dlf_srv_thread_info[thread_index], 1) < 0) {
        dlf_srv_thread_info[thread_index].s = -1;
        dlflogit (func, DLF02, "Cpool_assign", sstrerror(serrno));
        return (SYERR);
      }
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
  if ((maxfds = Cinitdaemon ("dlfserver", NULL)) < 0)
    exit (SYERR);
  exit (dlf_main (NULL));
#else
  if (Cinitservice ("dlf", &dlf_main))
    exit (SYERR);
#endif
}

void *
doit(arg)
     void *arg;
{
  int c;
  char *clienthost;
  int magic;
  char* req_data;
  int req_type = 0;
  int data_len;
  struct dlf_srv_thread_info *thip = (struct dlf_srv_thread_info *) arg;

  if ((c = getreq (thip->s, &magic, &req_type,
                   &req_data, &data_len, &clienthost)) == 0)
    procreq (magic, req_type, req_data, data_len, clienthost, thip);
  else if (c > 0)
    sendrep (thip->s, DLF_RC, c);
  else
    netclose (thip->s);
  thip->s = -1;
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

  l = netread_timeout (s, req_hdr, sizeof(req_hdr), DLF_TIMEOUT);
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
      dlflogit (func, "memory allocation failure\n");
      return (ENOMEM);
    }
    n = netread_timeout (s, *req_data, l, DLF_TIMEOUT);
    if (being_shutdown) {
      free(*req_data);
      return (EDLFNACT);
    }
    if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
      dlflogit (func, DLF02, "getpeername", neterror());
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
      dlflogit (func, DLF04, l);
    else if (l < 0) {
      dlflogit (func, DLF02, "netread", strerror(errno));
    }
    return (SEINTERNAL);
  }
}


procreq(magic, req_type, req_data, data_len, clienthost, thip)
     int magic;
     int req_type;
     char *req_data;
     int data_len;
     char *clienthost;
     struct dlf_srv_thread_info *thip;
{
  int c;

  /* connect to the database if not done yet */

  if (! thip->db_open_done) {

    if (req_type != DLF_SHUTDOWN) {
      if (dlf_opendb (db_srvr, db_user, db_pwd, &thip->dbfd) < 0) {
        c = serrno;
        sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
        sendrep (thip->s, DLF_RC, c);
        free (req_data);
        return;
      }
      thip->db_open_done = 1;
    }
  }

  switch (req_type) {
  case DLF_SHUTDOWN:
    c = dlf_srv_shutdown (magic, req_data, clienthost, thip);
    break;
  case DLF_STORE:
    c = proc_entermsgreq (magic, req_type, req_data, data_len,
                          clienthost, thip);
    break;
  case DLF_GETMSGTEXTS:
    c = dlf_srv_getmsgtexts (magic, req_data, clienthost, thip);
    break;
  case DLF_ENTERFAC:
    c = dlf_srv_enterfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_GETFAC:
    c = dlf_srv_getfacilites(magic, req_data, clienthost, thip);
    break;
  case DLF_ENTERTXT:
    c = dlf_srv_entertext (magic, req_data, clienthost, thip);
    break;
  case DLF_MODFAC:
    c = dlf_srv_modfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_DELFAC:
    c = dlf_srv_delfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_MODTXT:
    c = dlf_srv_modtext (magic, req_data, clienthost, thip);
    break;
  case DLF_DELTXT:
    c = dlf_srv_deltext (magic, req_data, clienthost, thip);
    break;

  default:
    sendrep (thip->s, MSG_ERR, DLF03, req_type);
    c = SEINTERNAL;
    break;
  }
  if (req_type != DLF_STORE) {
    /*
      Free memory allocated by getreq.
      When request is DLF_STORE memory is freed by proc_entermsgreq()
    */
    free (req_data);
  }
  sendrep (thip->s, DLF_RC, c);
}

proc_entermsgreq(magic, req_type, req_data, data_len, clienthost, thip)
     int magic;
     int req_type;
     char *req_data;
     int data_len;
     char *clienthost;
     struct dlf_srv_thread_info *thip;
{
  int c;
  int last_message;
  int new_req_type = -1;
  int rc = 0;
  fd_set readfd, readmask;
  struct timeval timeval;

  int mag;
  char *r_data;
  int d_len;
  char *c_host;

  /* wait for requests and process them */
  mag = magic;
  r_data = req_data;
  d_len = data_len;
  c_host = clienthost;

  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    if (req_type == DLF_STORE) {
      if (c = dlf_srv_entermessage (mag, r_data, d_len, c_host,
                                    thip, &last_message)){
        return(c);
      }
    }
    else {
      return (EINVAL);
    }
    free (r_data);
    if (last_message) break;

    sendrep (thip->s, DLF_IRC, 0);
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = DLF_READRQTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      break;
    }
    if (rc = getreq (thip->s, &mag, &new_req_type, &r_data,
                     &d_len, &c_host) != 0) {
      break;
    }
    if (new_req_type != req_type)
      break;
  }
  return (rc);
}
