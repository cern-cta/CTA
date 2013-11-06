/*
 * $Id: vmgr_main.c,v 1.13 2009/07/23 12:22:06 waldron Exp $
 *
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
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
#include <unistd.h>

#include "Cinit.h"
#include "Cdomainname.h"
#include "Cnetdb.h"
#include "Cpool_api.h"
#include "Cupv_api.h"
#include "getconfent.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_server.h"
#include "patchlevel.h"

#ifdef VMGRCSEC
#include "Csec_api.h"
#endif

/* prototypes */
static void *procconnection(void *);

int being_shutdown = 0;
char vmgrconfigfile[CA_MAXPATHLEN+1];
char localhost[CA_MAXHOSTNAMELEN+1];
struct vmgr_srv_thread_info *vmgr_srv_thread_info;

void vmgr_signal_handler(int sig)
{
  if (sig == SIGINT) {
    vmgrlogit("MSG=\"Caught SIGINT, immediate stop\"");
    exit(0);
  } else if (sig == SIGTERM) {
    vmgrlogit("MSG=\"Caught SIGTERM, shutting down\"");
    being_shutdown = 1;
  }
}

int vmgr_main(struct main_args *main_args)
{
  char logfile[CA_MAXPATHLEN + 1];
  int c = 0;
  struct vmgr_dbfd dbfd;
  char *dp = NULL;
  char domainname[CA_MAXHOSTNAMELEN+1];
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  int i = 0;
  int ipool = 0;
  int nbthreads = VMGR_NBTHREADS;
  int on = 1;        /* for REUSEADDR */
  char *p = NULL;
  int rqfd = 0;
  int s = 0;
  struct sockaddr_in sin;
  struct servent *sp = NULL;
  int thread_index = 0;
  int daemonize = 1;
  fd_set readfd;
  struct timeval timeval;

  vmgrconfigfile[0] = '\0';
  strcpy(logfile, "/var/log/castor/vmgrd.log");

  /* process command line options if any */
  while ((c = getopt (main_args->argc, main_args->argv, "fc:l:t:")) != EOF) {
    switch (c) {
    case 'f':
      daemonize = 0;
      break;
    case 'c':
      strncpy (vmgrconfigfile, optarg, sizeof(vmgrconfigfile));
      vmgrconfigfile[sizeof(vmgrconfigfile) - 1] = '\0';
      break;
    case 'l':
      strncpy (logfile, optarg, sizeof(logfile));
      logfile[sizeof(logfile) - 1] = '\0';
      break;
    case 't':
      if ((nbthreads = strtol (optarg, &dp, 10)) < 0 ||
          nbthreads >= VMGR_MAXNBTHREADS || *dp != '\0') {
        fprintf(stderr, "Invalid number of threads: %s\n", optarg);
        exit (USERR);
      }
      break;
    }
  }

  if (daemonize) {
    if (Cinitdaemon ("vmgrd", NULL) < 0) {
      exit (SYERR);
    }
  } else {
    for (i = 3; i < getdtablesize(); i++)
      close (i);
  }

  /* Open the logging interface */
  openlog("vmgrd", logfile);

  vmgrlogit("MSG=\"Volume Manager Daemon Started\" "
            "Version=\"%d.%d.%d-%d\"",
            MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);

  gethostname (localhost, CA_MAXHOSTNAMELEN+1);
  if (strchr (localhost, '.') == NULL) {
    if (Cdomainname (domainname, sizeof(domainname)) < 0) {
      vmgrlogit("MSG=\"Error: Unable to get domainname\" Function=\"Cdomainname\" "
                "Error=\"%s\" File=\"%s\" Line=%d",
                sstrerror(serrno), __FILE__, __LINE__);
      exit (SYERR);
    }
    strcat (localhost, ".");
    strcat (localhost, domainname);
  }

  /* Set the location of the volume manager login file */
  if (!*vmgrconfigfile) {
    strcpy (vmgrconfigfile, "/etc/castor/VMGRCONFIG");
  }

  (void) vmgr_init_dbpkg ();
  memset (&dbfd, 0, sizeof(dbfd));
  dbfd.idx = nbthreads;
  if (vmgr_opendb (&dbfd) < 0)
    return (SYERR);
  (void) vmgr_closedb (&dbfd);

  /* Create a pool of threads */
  if ((ipool = Cpool_create (nbthreads, NULL)) < 0) {
    vmgrlogit("MSG=\"Error: Unable to create thread pool\" "
              "Function=\"Cpool_create\" Error=\"%s\" File=\"%s\" Line=%d",
              sstrerror(serrno), __FILE__, __LINE__);
    return (SYERR);
  }
  if ((vmgr_srv_thread_info =
       calloc (nbthreads, sizeof(struct vmgr_srv_thread_info))) == NULL) {
    vmgrlogit("MSG=\"Error: Failed to allocate memory\" "
              "Function=\"calloc\" Error=\"%s\" File=\"%s\" Line=%d",
              strerror(errno), __FILE__, __LINE__);
    return (SYERR);
  }
  for (i = 0; i < nbthreads; i++) {
    (vmgr_srv_thread_info + i)->s              = -1;
    (vmgr_srv_thread_info + i)->dbfd.idx       = i;
    (vmgr_srv_thread_info + i)->dbfd.connected = 0;
  }

  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
  signal (SIGTERM,vmgr_signal_handler);
  signal (SIGINT,vmgr_signal_handler);

  /* Open request socket */
  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    vmgrlogit("MSG=\"Error: Failed to create listening socket\" "
              "Function=\"socket\" Error=\"%s\" File=\"%s\" Line=%d",
              neterror(), __FILE__, __LINE__);
    return (CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
#ifdef VMGRCSEC
  if ((p = getenv ("SVMGR_PORT")) || (p = getconfent ("SVMGR", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if (sp = getservbyname ("svmgr", "tcp")) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)SVMGR_PORT);
  }
#else
  if ((p = getenv ("VMGR_PORT")) || (p = getconfent ("VMGR", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if ((sp = getservbyname ("vmgr", "tcp"))) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)VMGR_PORT);
  }
#endif
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0) {
    vmgrlogit("MSG=\"Error: Failed to set socket option\" "
              "Function=\"setsockopt\" Option=\"SO_REUSEADDR\" Error=\"%s\" "
              "File=\"%s\" Line=%d",
              neterror(), __FILE__, __LINE__);
  }
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    vmgrlogit("MSG=\"Error: Failed to bind listening socket\" "
              "Function=\"socket\" Error=\"%s\" File=\"%s\" Line=%d",
              neterror(), __FILE__, __LINE__);
    close(s);
    return (CONFERR);
  }
  listen (s, 5) ;

  /* Main loop */
  while (1) {
    if (being_shutdown) {
      int nb_active_threads = 0;
      for (i = 0; i < nbthreads; i++) {
        if ((vmgr_srv_thread_info + i)->s >= 0) {
          nb_active_threads++;
          continue;
        }
        if ((vmgr_srv_thread_info + i)->dbfd.connected)
          (void) vmgr_closedb (&(vmgr_srv_thread_info + i)->dbfd);
      }
      if (nb_active_threads == 0)
        close(s);
        return (0);
    }

    FD_ZERO (&readfd);
    FD_SET (s, &readfd);
    timeval.tv_sec = CHECKI;
    timeval.tv_usec = 0;
    if (select (s+1, &readfd, NULL, NULL, &timeval) > 0) {
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
      if ((thread_index = Cpool_next_index (ipool)) < 0) {
        vmgrlogit("MSG=\"Error: Failed to determine next available thread "
                  "to process request\" Function=\"Cpool_next_index\" "
                  "Error=\"%s\" File=\"%s\" Line=%d",
                  sstrerror(serrno), __FILE__, __LINE__);
        if (serrno == SEWOULDBLOCK) {
          sendrep (rqfd, VMGR_RC, serrno);
          continue;
        } else {
          return (SYERR);
        }
      }
      (vmgr_srv_thread_info + thread_index)->s = rqfd;
      if (Cpool_assign (ipool, &procconnection,
                        vmgr_srv_thread_info + thread_index, 1) < 0) {
        (vmgr_srv_thread_info + thread_index)->s = -1;
        vmgrlogit("MSG=\"Error: Failed to assign request to thread\" "
                  "Function=\"Cpool_assign\" Error=\"%s\" File=\"%s\" Line=%d",
                  sstrerror(serrno), __FILE__, __LINE__);
        return (SYERR);
      }
    }
  }
}

int main(int argc,
         char **argv)
{
  struct main_args main_args;

  main_args.argc = argc;
  main_args.argv = argv;
  exit (vmgr_main (&main_args));
}

int getreq(struct vmgr_srv_thread_info *thip,
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

  serrno = 0;
  l = netread_timeout (thip->s, req_hdr, sizeof(req_hdr), VMGR_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG (rbp, n);
    *magic = n;
    unmarshall_LONG (rbp, n);
    *req_type = n;
    unmarshall_LONG (rbp, msglen);
    if (msglen > REQBUFSZ) {
      vmgrlogit("MSG=\"Error: Request too large\" MaxSize=%d "
                "File=\"%s\" Line=%d", REQBUFSZ, __FILE__, __LINE__);
      return (-1);
    }
    l = msglen - sizeof(req_hdr);
    n = netread_timeout (thip->s, req_data, l, VMGR_TIMEOUT);
    if (being_shutdown) {
      return (EVMGRNACT);
    }
    if (getpeername (thip->s, (struct sockaddr *) &from, &fromlen) < 0) {
      vmgrlogit("MSG=\"Error: Failed to getpeername\" "
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
      vmgrlogit("MSG=\"Error: Netread failure\" Function=\"netread\" "
                "Error=\"1\" File=\"%s\" Line=%d",
                __FILE__, __LINE__);
    } else if (l < 0) {
      vmgrlogit("MSG=\"Error: Failed to netread\" Function=\"netread\" "
                "Error=\"%s\" File=\"%s\" Line=%d",
                neterror(), __FILE__, __LINE__);
    }
    return (SEINTERNAL);
  }
}

int proclistreq(int magic,
                int req_type,
                char *req_data,
                struct vmgr_srv_thread_info *thip)
{
  int c = 0;
  int endlist = 0;
  int new_req_type = -1;
  int rc = 0;
  struct vmgr_tape_info_byte_u64 tape;

  /* Wait for list requests and process them */
  while (1) {
    switch (req_type) {
    case VMGR_LISTDENMAP:
      if ((c = vmgr_srv_listdenmap (magic, req_data, thip, &thip->reqinfo,
                                    endlist)))
        return (c);
      break;
    case VMGR_LISTDENMAP_BYTE_U64:
      if ((c = vmgr_srv_listdenmap_byte_u64 (magic, req_data, thip,
                                             &thip->reqinfo, endlist)))
        return (c);
      break;
    case VMGR_LISTDGNMAP:
      if ((c = vmgr_srv_listdgnmap (req_data, thip, &thip->reqinfo, endlist)))
        return (c);
      break;
    case VMGR_LISTLIBRARY:
      if ((c = vmgr_srv_listlibrary (req_data, thip, &thip->reqinfo, endlist)))
        return (c);
      break;
    case VMGR_LISTMODEL:
      if ((c = vmgr_srv_listmodel (magic, req_data, thip, &thip->reqinfo,
                                   endlist)))
        return (c);
      break;
    case VMGR_LISTPOOL:
      if ((c = vmgr_srv_listpool (req_data, thip, &thip->reqinfo, endlist)))
        return (c);
      break;
    case VMGR_LISTTAPE:
      if ((c = vmgr_srv_listtape (magic, req_data, thip, &thip->reqinfo, &tape,
                                  endlist)))
        return (c);
      break;
    case VMGR_LISTTAPE_BYTE_U64:
      if ((c = vmgr_srv_listtape_byte_u64 (magic, req_data, thip,
                                           &thip->reqinfo, &tape, endlist)))
        return (c);
      break;
    }
    if (endlist) break;
    sendrep (thip->s, VMGR_IRC, 0);
    {
      fd_set readfd;
      struct timeval timeval;

      FD_ZERO (&readfd);
      FD_SET (thip->s, &readfd);
      timeval.tv_sec = VMGR_LISTTIMEOUT;
      timeval.tv_usec = 0;
      if(select(thip->s+1, &readfd, NULL, NULL, &timeval) <= 0) {
        endlist = 1;
        continue;
      }
    }
    if ((rc = getreq (thip, &magic, &new_req_type, req_data)) != 0) {
      endlist = 1;
      continue;
    }
    if (new_req_type != req_type)
      endlist = 1;
  }
  return (rc);
}

void procreq(const int magic,
             const int req_type,
             char *const req_data,
             struct vmgr_srv_thread_info *thip)
{
  int c = 0;

  /* Connect to the database if not done yet */
  if (! (&thip->dbfd)->connected ) {
    if (Cupv_seterrbuf (thip->errbuf, PRTBUFSZ)) {
      c = SEINTERNAL;
      sendrep (thip->s, MSG_ERR, "Cupv_seterrbuf error: %s\n",
               sstrerror(serrno));
      sendrep (thip->s, VMGR_RC, c);
      return;
    }
    if (!being_shutdown) {
      if (vmgr_opendb (&thip->dbfd) < 0) {
        c = serrno;
        sendrep (thip->s, MSG_ERR, "Database open error: %d\n", c);
        sendrep (thip->s, VMGR_RC, c);
        return;
      }
    }
  }

  switch (req_type) {
  case VMGR_DELTAPE:
    sendrep (thip->s, MSG_ERR, VMG02, "vmgrdeletetape",
             "VMGR is not compatible with this version of VMGRDELTAPE: please "
             "use a version equal to or greater than v2.1.9-4");
    c = SEINTERNAL;
    break;
  case VMGR_ENTTAPE:
    c = vmgr_srv_entertape (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_GETTAPE:
    c = vmgr_srv_gettape (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_MODTAPE:
    c = vmgr_srv_modifytape (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYTAPE:
    c = vmgr_srv_querytape (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYTAPE_BYTE_U64:
    c = vmgr_srv_querytape_byte_u64 (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_UPDTAPE:
    c = vmgr_srv_updatetape (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELMODEL:
    c = vmgr_srv_deletemodel (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTMODEL:
    c = vmgr_srv_entermodel (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_MODMODEL:
    c = vmgr_srv_modifymodel (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYMODEL:
    c = vmgr_srv_querymodel (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELPOOL:
    c = vmgr_srv_deletepool (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTPOOL:
    c = vmgr_srv_enterpool (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_MODPOOL:
    c = vmgr_srv_modifypool (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYPOOL:
    c = vmgr_srv_querypool (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_TPMOUNTED:
    c = vmgr_srv_tpmounted (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELDENMAP:
    c = vmgr_srv_deletedenmap (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTDENMAP:
    c = vmgr_srv_enterdenmap (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTDENMAP_BYTE_U64:
    c = vmgr_srv_enterdenmap_byte_u64 (magic, req_data, thip, &thip->reqinfo);
    break;
  case VMGR_LISTDENMAP:
  case VMGR_LISTDENMAP_BYTE_U64:
  case VMGR_LISTDGNMAP:
  case VMGR_LISTLIBRARY:
  case VMGR_LISTMODEL:
  case VMGR_LISTPOOL:
  case VMGR_LISTTAPE:
  case VMGR_LISTTAPE_BYTE_U64:
    c = proclistreq (magic, req_type, req_data, thip);
    break;
  case VMGR_RECLAIM:
    c = vmgr_srv_reclaim (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELLIBRARY:
    c = vmgr_srv_deletelibrary (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTLIBRARY:
    c = vmgr_srv_enterlibrary (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_MODLIBRARY:
    c = vmgr_srv_modifylibrary (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYLIBRARY:
    c = vmgr_srv_querylibrary (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELDGNMAP:
    c = vmgr_srv_deletedgnmap (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_ENTDGNMAP:
    c = vmgr_srv_enterdgnmap (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELTAG:
    c = vmgr_srv_deltag (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_GETTAG:
    c = vmgr_srv_gettag (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_SETTAG:
    c = vmgr_srv_settag (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_DELTAPEAFTERCHK:
    c = vmgr_srv_deletetape (req_data, thip, &thip->reqinfo);
    break;
  case VMGR_QRYTAPEBLKSIZE:
    c = vmgr_srv_qrytapeblksz (magic, req_data, thip, &thip->reqinfo);
    break;
  default:
    sendrep (thip->s, MSG_ERR, VMG03, req_type);
    c = SEINTERNAL;
  }
  sendrep (thip->s, VMGR_RC, c);
}

void *procconnection(void *const arg) {
  int c;
  int magic;
  char req_data[REQBUFSZ-3*LONGSIZE];
  int req_type = 0;
  struct vmgr_srv_thread_info *thip = (struct vmgr_srv_thread_info *) arg;

#ifdef VMGRCSEC
  char *username;
  Csec_server_reinitContext(&(thip->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
  if (Csec_server_establishContext(&(thip->sec_ctx),thip->s) < 0) {
    vmgrlogit("MSG=\"Error: Could not establish security context\" "
              "Error=\"%s\"", Csec_getErrorMessage());
    close (thip->s);
    thip->s = -1;
    return (NULL);
  }
  /* Connection could be done from another castor service */
  if ((c = Csec_server_isClientAService(&(thip->sec_ctx))) >= 0) {
    vmgrlogit("MSG=\"CSEC: Client is castor service\" Type=%d", c)
      thip->Csec_service_type = c;
    thip->Csec_service_type = c;
  }
  else {
    if (Csec_server_mapClientToLocalUser(&(thip->sec_ctx), &username,
                                         &(thip->Csec_uid),
                                         &(thip->Csec_gid)) == 0) {
      vmgrlogit("MSG=\"Mapping to local user successful\" CsecUid=%d "
                "CsecGid=%d Username=\"%s\"",
                thip->Csec_uid, thip->Csec->gid, username);
      thip->Csec_service_type = -1;
    }
    else {
      vmgrlogit("MSG=\"Error: Could not map to local user\n");
      close (thip->s);
      thip->s = -1;
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
    thip->reqinfo.requuid[CUUID_STRING_LEN] = 0;
    Cuuid2string(thip->reqinfo.requuid, CUUID_STRING_LEN + 1, &cuuid);

    procreq (magic, req_type, req_data, thip);
  } else if (c > 0) {
    sendrep (thip->s, VMGR_RC, c);
  } else {
    close (thip->s);
  }
  thip->s = -1;
  return (NULL);
}
