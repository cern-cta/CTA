/*
 * Copyright (C) 1999-2002 by CERN IT-DS/HSM
 * All rights reserved
 */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "Cinit.h"
#include "Cnetdb.h"
#include "Cpool_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "Cupv.h"
#include "Cupv_server.h"
#include "Cregexp.h"
#include "Cgetopt.h"
#include "patchlevel.h"

#ifdef UPVCSEC
#include "Csec_api.h"
#endif

int being_shutdown = 0;

char cupvconfigfile[CA_MAXPATHLEN+1];
int maxfds;
struct Cupv_srv_thread_info *cupv_srv_thread_info;

void Cupv_signal_handler(int sig)
{
  if (sig == SIGINT) {
    cupvlogit("MSG=\"Caught SIGINT, immediate stop\"");
    exit(0);
  } else if (sig == SIGTERM) {
    cupvlogit("MSG=\"Caught SIGTERM, shutting down\"");
    being_shutdown = 1;
  }
}

int Cupv_main(struct main_args *main_args)
{
  char logfile[CA_MAXPATHLEN + 1];
  int c;
  struct Cupv_dbfd dbfd;
  void *doit(void *);
  char *dp;
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  char *getconfent();
  int i;
  int ipool;
  int nbthreads = CUPV_NBTHREADS;
  int on = 1;	/* for REUSEADDR */
  char *p;
  const char *buf;
  int daemonize = 1;
  fd_set readfd, readmask;
  int rqfd;
  int s;
  struct sockaddr_in sin;
  struct servent *sp;
  int thread_index;
  struct timeval timeval;

  cupvconfigfile[0] = '\0';
  strcpy(logfile, CUPVLOGFILE);

  /* Process command line options if any */
  while ((c = getopt (main_args->argc, main_args->argv, "fc:l:t:")) != EOF) {
    switch (c) {
    case 'f':
      daemonize = 0;
      break;
    case 'c':
      strncpy (cupvconfigfile, optarg, sizeof(cupvconfigfile));
      cupvconfigfile[sizeof(cupvconfigfile) - 1] = '\0';
      break;
    case 'l':
      strncpy (logfile, optarg, sizeof(logfile));
      logfile[sizeof(logfile) - 1] = '\0';
      break;
    case 't':
      if ((nbthreads = strtol (optarg, &dp, 10)) < 0 ||
	  nbthreads >= CUPV_MAXNBTHREADS || *dp != '\0') {
	fprintf(stderr, "Invalid number of threads: %s\n", optarg);
	exit (USERR);
      }
      break;
    }
  }

  if (daemonize) {
    if ((maxfds = Cinitdaemon ("cupvd", NULL)) < 0)
      exit (SYERR);
  } else {
    maxfds = getdtablesize();
    for (i = 3; i < maxfds; i++)
      close (i);
  }

  /* Open the logging interface */
  openlog("cupvd", logfile);

  cupvlogit("MSG=\"User Privilege Validator Daemon Started\" "
	    "Version=\"%d.%d.%d-%d\"",
	    MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);

  /* Set the location of the upv login file */
  if (!*cupvconfigfile) {
    if (strncmp (CUPVCONFIG, "%SystemRoot%\\", 13) == 0 &&
	(p = getenv ("SystemRoot")) &&
	(buf = strchr (CUPVCONFIG, '\\')))
      sprintf (cupvconfigfile, "%s%s", p, buf);
    else
      strcpy (cupvconfigfile, CUPVCONFIG);
  }

  (void) Cupv_init_dbpkg ();
  memset (&dbfd, 0, sizeof(dbfd));
  dbfd.idx = nbthreads;
  if (Cupv_opendb (&dbfd) < 0)
    return (SYERR);
  (void) Cupv_closedb (&dbfd);

  /* Create a pool of threads */
  if ((ipool = Cpool_create (nbthreads, NULL)) < 0) {
    cupvlogit("MSG=\"Error: Unable to create thread pool\" "
	      "Function=\"Cpool_create\" Error=\"%s\" File=\"%s\" Line=%d",
	      sstrerror(serrno), __FILE__, __LINE__);
    return (SYERR);
  }
  if ((cupv_srv_thread_info =
       calloc (nbthreads, sizeof(struct Cupv_srv_thread_info))) == NULL) {
    cupvlogit("MSG=\"Error: Failed to allocate memory\" "
	      "Function=\"calloc\" Error=\"%s\" File=\"%s\" Line=%d",
	      strerror(errno), __FILE__, __LINE__);
    return (SYERR);
  }
  for (i = 0; i < nbthreads; i++) {
    (cupv_srv_thread_info + i)->s              = -1;
    (cupv_srv_thread_info + i)->dbfd.idx       = i;
    (cupv_srv_thread_info + i)->dbfd.connected = 0;
  }

  FD_ZERO (&readmask);
  FD_ZERO (&readfd);
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ, SIG_IGN);
  signal (SIGTERM,Cupv_signal_handler);
  signal (SIGINT,Cupv_signal_handler);

  /* Spen request socket */
  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    cupvlogit("MSG=\"Error: Failed to create listening socket\" "
	      "Function=\"socket\" Error=\"%s\" File=\"%s\" Line=%d",
	      neterror(), __FILE__, __LINE__);
    return (CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
#ifdef UPVCSEC
  if ((p = getenv ("SCUPV_PORT")) || (p = getconfent ("SCUPV", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if (sp = getservbyname ("sCupv", "tcp")) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)SCUPV_PORT);
  }
#else
  if ((p = getenv ("CUPV_PORT")) || ((p = getconfent ("CUPV", "PORT", 0)))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if ((sp = getservbyname ("Cupv", "tcp"))) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)CUPV_PORT);
  }
#endif
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0) {
    cupvlogit("MSG=\"Error: Failed to set socket option\" "
	      "Function=\"setsockopt\" Option=\"SO_REUSEADDR\" Error=\"%s\" "
	      "File=\"%s\" Line=%d",
	      neterror(), __FILE__, __LINE__);
  }
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    cupvlogit("MSG=\"Error: Failed to bind listening socket\" "
	      "Function=\"socket\" Error=\"%s\" File=\"%s\" Line=%d",
	      neterror(), __FILE__, __LINE__);
    return (CONFERR);
  }
  listen (s, 5) ;

  FD_SET (s, &readmask);

  /* Main loop */
  while (1) {
    if (being_shutdown) {
      int nb_active_threads = 0;
      for (i = 0; i < nbthreads; i++) {
	if ((cupv_srv_thread_info + i)->s >= 0) {
	  nb_active_threads++;
	  continue;
	}
	if ((cupv_srv_thread_info + i)->dbfd.connected)
	  (void) Cupv_closedb (&(cupv_srv_thread_info + i)->dbfd);
      }
      if (nb_active_threads == 0)
	return (0);
    }
    if (FD_ISSET (s, &readfd)) {
      FD_CLR (s, &readfd);
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
      if ((thread_index = Cpool_next_index (ipool)) < 0) {
        cupvlogit("MSG=\"Error: Failed to determine next available thread "
		  "to process request\" Function=\"Cpool_next_index\" "
		  "Error=\"%s\" File=\"%s\" Line=%d",
		  sstrerror(serrno), __FILE__, __LINE__);
	if (serrno == SEWOULDBLOCK) {
	  sendrep (rqfd, CUPV_RC, serrno);
	  continue;
	} else
	  return (SYERR);
      }
      (cupv_srv_thread_info + thread_index)->s = rqfd;
      if (Cpool_assign (ipool, &doit,
			cupv_srv_thread_info + thread_index, 1) < 0) {
	(cupv_srv_thread_info + thread_index)->s = -1;
        cupvlogit("MSG=\"Error: Failed to assign request to thread\" "
		  "Function=\"Cpool_assign\" Error=\"%s\" File=\"%s\" Line=%d",
		  sstrerror(serrno), __FILE__, __LINE__);
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

int main(int argc,
         char **argv)
{

  struct main_args main_args;

  main_args.argc = argc;
  main_args.argv = argv;
  exit (Cupv_main (&main_args));
}

int getreq(struct Cupv_srv_thread_info *thip,
           int *magic,
           int *req_type,
           char *req_data)
{
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  struct hostent *hp;
  struct timeval tv;
  int l;
  int msglen;
  int n;
  char *rbp;
  char req_hdr[3*LONGSIZE];

  /* Record the start time of this request */
  gettimeofday(&tv, NULL);
  thip->reqinfo.starttime =
    ((double)tv.tv_sec * 1000) + ((double)tv.tv_usec / 1000);

  l = netread_timeout (thip->s, req_hdr, sizeof(req_hdr), CUPV_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG (rbp, n);
    *magic = n;
    unmarshall_LONG (rbp, n);
    *req_type = n;
    unmarshall_LONG (rbp, msglen);
    if (msglen > REQBUFSZ) {
      cupvlogit("MSG=\"Error: Request too large\" MaxSize=%d "
		"File=\"%s\" Line=%d", REQBUFSZ, __FILE__, __LINE__);
      return (-1);
    }
    l = msglen - sizeof(req_hdr);
    n = netread_timeout (thip->s, req_data, l, CUPV_TIMEOUT);
    if (being_shutdown) {
      return (ECUPVNACT);
    }
    if (getpeername (thip->s, (struct sockaddr *) &from, &fromlen) < 0) {
      cupvlogit("MSG=\"Error: Failed to getpeername\" "
		"Function=\"getpeername\" Error=\"%s\" File=\"%s\" Line=%d",
		neterror(), __FILE__, __LINE__);
      return (SEINTERNAL);
    }
    hp = Cgethostbyaddr ((char *)(&from.sin_addr),
			 sizeof(struct in_addr), from.sin_family);
    if (hp == NULL) {
      thip->reqinfo.clienthost = inet_ntoa (from.sin_addr);
    } else {
      thip->reqinfo.clienthost = hp->h_name;
    }
    return (0);
  } else {
    if (l > 0) {
      cupvlogit("MSG=\"Error: Netread failure\" Function=\"netread\" "
		"Error=\"1\" File=\"%s\" Line=%d",
		__FILE__, __LINE__);
    } else if (l < 0) {
      cupvlogit("MSG=\"Error: Failed to netread\" Function=\"netread\" "
		"Error=\"%s\" File=\"%s\" Line=%d",
		neterror(), __FILE__, __LINE__);
    }
    return (SEINTERNAL);
  }
}

int proclistreq(int magic,
                int req_type,
                char *req_data,
                struct Cupv_srv_thread_info *thip)
{
  int c;
  int new_req_type = -1;
  fd_set readfd, readmask;
  struct timeval timeval;
  DBLISTPTR dblistptr;
  int endlist = 0;

  /* Wait for list requests and process them */
  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    switch (req_type) {
    case CUPV_LIST:
      if ((c = Cupv_srv_list (req_data, thip, &thip->reqinfo, endlist,
			      &dblistptr)))
	return (c);
      break;
    }
    if(endlist) break;
    sendrep (thip->s, CUPV_IRC, 0);
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CUPV_LISTTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      endlist = 1;
      continue;
    }
    if ((c = getreq (thip, &magic, &new_req_type, req_data) < 0)) {
      endlist = 1;
      continue;
    }
    if (new_req_type != req_type)
      endlist = 1;
  }
  return (c);
}

void procreq(int magic,
             int req_type,
             char *req_data,
             struct Cupv_srv_thread_info *thip)
{
  int c;

  /* Connect to the database if not done yet */
  if (! (&thip->dbfd)->connected ) {
    if (Cupv_opendb (&thip->dbfd) < 0) {
      c = serrno;
      sendrep (thip->s, MSG_ERR, "Database open error: %d\n", c);
      sendrep (thip->s, CUPV_RC, c);
      return;
    }
  }

  switch (req_type) {
  case CUPV_ADD:
    c = Cupv_srv_add (req_data, thip, &thip->reqinfo);
    break;
  case CUPV_DELETE:
    c = Cupv_srv_delete (req_data, thip, &thip->reqinfo);
    break;
  case CUPV_MODIFY:
    c = Cupv_srv_modify (req_data, thip, &thip->reqinfo);
    break;
  case CUPV_CHECK:
    c = Cupv_srv_check (req_data, thip, &thip->reqinfo);
    break;
  case CUPV_LIST:
    c = proclistreq (magic, req_type, req_data, thip);
    break;
  default:
    sendrep (thip->s, MSG_ERR, CUP03, req_type);
    c = SEINTERNAL;
  }
  sendrep (thip->s, CUPV_RC, c);
}

void *doit(void *arg)
{
  int c;
  int magic;
  char req_data[REQBUFSZ-3*LONGSIZE];
  int req_type = 0;
  struct Cupv_srv_thread_info *thip = (struct Cupv_srv_thread_info *) arg;

#ifdef UPVCSEC
  Csec_server_reinitContext(&(thip->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
  if (Csec_server_establishContext(&(thip->sec_ctx),thip->s) < 0) {
    cupvlogit("MSG=\"Error: Could not establish security context\" "
	      "Error=\"%s\"", Csec_getErrorMessage());
    netclose (thip->s);
    thip->s = -1;
    return (NULL);
  }
  /* Connection could be done from another castor service */
  if ((c = Csec_server_isClientAService(&(thip->sec_ctx))) >= 0) {
    cupvlogit("MSG=\"CSEC: Client is castor service\" Type=%d", c)
      thip->Csec_service_type = c;
  }
  else {
    char *username;
    if (Csec_server_mapClientToLocalUser(&(thip->sec_ctx), &username,
                                         &(thip->Csec_uid),
                                         &(thip->Csec_gid)) == 0) {
      cupvlogit("MSG=\"Mapping to local user successful\" CsecUid=%d "
		"CsecGid=%d Username=\"%s\"",
		thip->Csec_uid, thip->Csec->gid, username);
      thip->Csec_service_type = -1;
    }
    else {
      cupvlogit("MSG=\"Error: Could not map to local user\n");
      netclose (thip->s);
      return (NULL);
    }
  }
#endif

  /* Initialize the request info structure. */
  thip->reqinfo.uid        = 0;
  thip->reqinfo.gid        = 0;
  thip->reqinfo.username   = NULL;
  thip->reqinfo.clienthost = NULL;
  thip->reqinfo.logbuf[0]  = '\0';

  if ((c = getreq (thip, &magic, &req_type, req_data)) == 0) {

    /* Generate a unique request id to identify this new request. */
    Cuuid_t cuuid = nullCuuid;
    Cuuid_create(&cuuid);
    thip->reqinfo.reqid[CUUID_STRING_LEN] = 0;
    Cuuid2string(thip->reqinfo.reqid, CUUID_STRING_LEN + 1, &cuuid);

    procreq (magic, req_type, req_data, thip);
  } else if (c > 0) {
    sendrep (thip->s, CUPV_RC, c);
  } else {
    netclose (thip->s);
  }
  thip->s = -1;
  return (NULL);
}

/* Checks a regular expression */
int Cupv_check_regexp_syntax(char *tobechecked,
                             struct Cupv_srv_request_info *reqinfo) {

  char tmp[CA_MAXREGEXPLEN + 1];
  int i=0;
  int beginok = 0, endok =0;

  if (strlen(tobechecked) == 0) {
    return(0);
  }

  if (tobechecked[0] == REGEXP_START_CHAR) {
    beginok = 1;
  }

  while(tobechecked[i] != 0 && i <= CA_MAXREGEXPLEN) {
    i++;
  }

  if (tobechecked[i-1] == REGEXP_END_CHAR) {
    endok = 1;
  }

  cupvlogit("MSG=\"Checking regular expression syntax\" REQID=%s "
            "Expression=\"%s\" BeginningOk=%d EndOk=%d",
            reqinfo->reqid, tobechecked, beginok, endok);

  /* Checking that the buffer can hold the complete address */
  if (i + (!beginok) + (!endok) > CA_MAXREGEXPLEN) {
    serrno = EINVAL;
    return(-1);
  }

  if (!beginok) {
    tmp[0] = REGEXP_START_CHAR;
    tmp[1] = '\0';
    strcat(tmp, tobechecked);
  } else {
    strcpy(tmp, tobechecked);
  }

  if (!endok) {
    strcat(tmp, REGEXP_END_STR);
  }

  strcpy(tobechecked, tmp);

  return(0);
}

/* Checks a regular expression */
int Cupv_check_regexp(char *tobechecked) {

  Cregexp_t *rex;

  if ((rex = Cregexp_comp(tobechecked)) == NULL) {
    return(-1);
  } else {
    free((char *)rex);
    return(0);
  }
}

/* Checks a request against a rule */
/* The rule Cupv_userpriv is the one considered as regular expressions */
int Cupv_compare_priv(struct Cupv_userpriv *requested,
                      struct Cupv_userpriv *rule) {

  Cregexp_t *rex;
  Cregexp_t *rex2;

  /* Checking uid & gid */
  if (requested->uid != rule->uid || requested->gid != rule->gid) {
    return(-1);
  }

  /* Checking srchost */
  if ((rex = Cregexp_comp(rule->srchost)) == NULL) {
    return(-1);
  } else {
    if (Cregexp_exec(rex, requested->srchost) != 0) {
      free((char *)rex);
      return(-1);
    }
  }

  free((char *)rex);

  /* Checking tgthost */
  if ((rex2 = Cregexp_comp(rule->tgthost)) == NULL) {
    return(-1);
  } else {
    if (Cregexp_exec(rex2, requested->tgthost) != 0) {
      free((char *)rex2);
      return(-1);
    }
  }

  free((char *)rex2);
  if ((rule->privcat & requested->privcat) != requested->privcat) {
    return(-1);
  } else {
    return(0);
  }
}

/* Checks that a UPV admin action is authorized */
/* It returns 0 if authorization is granted,
   -1 if there was an error,
   1 if the authorization was not granted... */
int Cupv_util_check(struct Cupv_userpriv *requested,
                    struct Cupv_srv_thread_info *thip) {

  int bol =  1;
  struct Cupv_userpriv db_entry;
  struct Cupv_userpriv filter;
  int c;
  DBLISTPTR dblistptr;

  memset (&dblistptr, 0, sizeof(DBLISTPTR));

  /* Action is authorized is the user is root on local machine */
  if (requested->uid == 0
      && (strcmp(requested->srchost, requested->tgthost) == 0)) {
    cupvlogit("MSG=\"Access GRANTED - user is root on localhost\" REQID=%s",
              thip->reqinfo.reqid);
    return(0);
  }

  /* Initializing the db_entry structure with uid/gid, so that the
   * Cupv_list_privilege functions returns all the rows for that uid/gid
   */
  filter.uid = requested->uid;
  filter.gid = requested->gid;
  filter.srchost[0] = 0;
  filter.tgthost[0] = 0;
  filter.privcat = -1;

  /* Looping on corresponding entries to check authorization */
  while ((c = Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, &filter,
                                         0, &dblistptr)) == 0) {

    if (c < 0) {
      cupvlogit("MSG=\"Error: Access DENIED - Problem accessing DB\" REQID=%s",
                thip->reqinfo.reqid);
      return(-1);
    }

    if (Cupv_compare_priv(requested, &db_entry) == 0) {
      cupvlogit("MSG=\"Access GRANTED - Authorization found in DB\" REQID=%s",
                thip->reqinfo.reqid);

      /* Calling list_privilege_entry with endlist = 1 to free the resources*/
      Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, requested, 1,
                                 &dblistptr);

      return(0);
    }

    bol = 0;
  }

  /* Nothing was found, return 1 */
  cupvlogit("MSG=\"Access DENIED - NO Authorization found in DB\" REQID=%s",
            thip->reqinfo.reqid);

  /* Calling list_privilege_entry with endlist = 1 to free the resources*/
  Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, requested, 1,
                             &dblistptr);
  return(1);
}



