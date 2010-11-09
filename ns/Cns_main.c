/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
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
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "Cinit.h"
#include "Cnetdb.h"
#include "Cns.h"
#include "Cns_server.h"
#include "Cpool_api.h"
#include "Csec_api.h"
#include "Cupv_api.h"
#include "Cdomainname.h"
#include "marshall.h"
#include "net.h"
#include "patchlevel.h"
#include "serrno.h"
#include <dlfcn.h>

int procsessreq(int magic, char *req_data, const char *clienthost, struct Cns_srv_thread_info *thip);
int procsessreq(int magic, char *req_data, const char *clienthost, struct Cns_srv_thread_info *thip);
int proctransreq(int magic, char *req_data, const char *clienthost, struct Cns_srv_thread_info *thip);
int procdirreq(int magic, int req_type, char *req_data, const char *clienthost, struct Cns_srv_thread_info *thip);
int Cns_init_dbpkg();

int being_shutdown = 0;
char func[16];
int jid;
char lcgdmmapfile[CA_MAXPATHLEN+1];
char localdomain[CA_MAXHOSTNAMELEN+1];
char localhost[CA_MAXHOSTNAMELEN+1];
char logfile[CA_MAXPATHLEN+1];
char nsconfigfile[CA_MAXPATHLEN+1];
int maxfds;
int rdonly;
struct Cns_srv_thread_info *Cns_srv_thread_info;

void Cns_signal_handler(int sig)
{
  strncpy (func, "Cns_serv", 16);
  if (sig == SIGINT) {
    nslogit(func, "Caught SIGINT, immediate stop\n");
    exit(0);
  } else if (sig == SIGTERM) {
    nslogit (func, "Caught SIGTERM, shutting down\n");
    being_shutdown = 1;
  }
}

int Cns_main(struct main_args *main_args)
{
  int c;
  struct Cns_dbfd dbfd;
  struct Cns_file_metadata direntry;
  void *doit(void *);
  char *dp;
  int s;
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  char *getconfent();
  int i;
  int ipool;
  int nbthreads = CNS_NBTHREADS;
  int on = 1; /* for REUSEADDR */
  char *p;
  const char *buf;
  fd_set readfd, readmask;
  int rqfd;
  int security = 0; /* Flag set to 1 when security is enable */

  struct sockaddr_in sin;
  struct servent *sp;
  int thread_index;
  struct timeval timeval;

  jid = getpid();
  strncpy (func, "Cns_serv", 16);
  nsconfigfile[0] = '\0';
  strcpy (logfile, NSLOGFILE);

  /* process command line options if any */

  while ((c = getopt (main_args->argc, main_args->argv, "c:l:m:rt:s")) != EOF) {
    switch (c) {
    case 'c':
      strncpy (nsconfigfile, optarg, sizeof(nsconfigfile));
      nsconfigfile[sizeof(nsconfigfile) - 1] = '\0';
      break;
    case 'l':
      strncpy (logfile, optarg, sizeof(logfile));
      logfile[sizeof(logfile) - 1] = '\0';
      break;
    case 'm':
      strncpy (lcgdmmapfile, optarg, sizeof(lcgdmmapfile));
      lcgdmmapfile[sizeof(lcgdmmapfile) - 1] = '\0';
      break;
    case 'r':
      rdonly++;
      break;
    case 't':
      if ((nbthreads = strtol (optarg, &dp, 10)) < 0 ||
          nbthreads >= CNS_MAXNBTHREADS || *dp != '\0') {
        nslogit (func, "Invalid number of threads: %s\n",
                 optarg);
        exit (USERR);
      }
      break;
    case 's':
      security = 1;
      break;
    }
  }

  nslogit (func, "started in %s mode (%s %d.%d.%d-%d)\n", (security ? "secure" : "non secure"),
           CNS_SCE, MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);
  gethostname (localhost, CA_MAXHOSTNAMELEN+1);
  if (Cdomainname (localdomain, sizeof(localdomain)) < 0) {
    nslogit (func, "Unable to get domainname\n");
    exit (SYERR);
  }
  if (strchr (localhost, '.') == NULL) {
    strcat (localhost, ".");
    strcat (localhost, localdomain);
  }

  /* set the location of the name server login file */
  if (!*nsconfigfile) {
    if (strncmp (NSCONFIG, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")) &&
        (buf = strchr (NSCONFIG, '\\')))
      sprintf (nsconfigfile, "%s%s", p, buf);
    else
      strcpy (nsconfigfile, NSCONFIG);
  }

  (void) Cns_init_dbpkg ();
  memset (&dbfd, 0, sizeof(dbfd));
  dbfd.idx = nbthreads;
  if (Cns_opendb (&dbfd) < 0)
    return (SYERR);

  /* create entry in the catalog for "/" if not already done */
  if (Cns_get_fmd_by_fullid (&dbfd, (u_signed64) 0, "/", &direntry, 0, NULL) < 0) {
    if (serrno != ENOENT)
      return (SYERR);
    nslogit (func, "creating /\n");
    memset (&direntry, 0, sizeof(direntry));
    direntry.fileid = 2;
    strcpy (direntry.name, "/");
    direntry.filemode = S_IFDIR | 0755;
    direntry.atime = time (0);
    direntry.mtime = direntry.atime;
    direntry.ctime = direntry.atime;
    direntry.status = '-';
    (void) Cns_start_tr (0, &dbfd);
    if (Cns_insert_fmd_entry (&dbfd, &direntry) < 0) {
      (void) Cns_abort_tr (&dbfd);
      (void) Cns_closedb (&dbfd);
      return (SYERR);
    }
    (void) Cns_end_tr (&dbfd);
  }
  (void) Cns_closedb (&dbfd);

  /* create a pool of threads */

  if ((ipool = Cpool_create (nbthreads, NULL)) < 0) {
    nslogit (func, NS002, "Cpool_create", sstrerror(serrno));
    return (SYERR);
  }
  if ((Cns_srv_thread_info =
       calloc (nbthreads, sizeof(struct Cns_srv_thread_info))) == NULL) {
    nslogit (func, NS002, "calloc", strerror(errno));
    return (SYERR);
  }
  for (i = 0; i < nbthreads; i++) {
    (Cns_srv_thread_info + i)->s              = -1;
    (Cns_srv_thread_info + i)->dbfd.idx       = i;
    (Cns_srv_thread_info + i)->dbfd.connected = 0;
  }

  FD_ZERO (&readmask);
  FD_ZERO (&readfd);
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
  signal (SIGTERM,Cns_signal_handler);
  signal (SIGINT,Cns_signal_handler);

  /* open request socket */

  serrno = 0;
  if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
    nslogit (func, NS002, "socket", neterror());
    return (CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
  if (security) {
    if ((p = getenv (CNS_SPORT_ENV)) || ((p = getconfent (CNS_SCE, "SEC_PORT", 0)))) {
      sin.sin_port = htons ((unsigned short)atoi (p));
    } else if ((sp = getservbyname (CNS_SEC_SVC, "tcp"))) {
      sin.sin_port = sp->s_port;
    } else {
      sin.sin_port = htons ((unsigned short)CNS_SEC_PORT);
    }
    /* Temporary workaround till we come up with something more clever to fix
     * bug related with the KRB5 and GSI mixed libraries
     */
    void *handle = dlopen ("libCsec_plugin_KRB5.so", RTLD_LAZY);
    if (!handle) {
      fprintf (stderr, "%s\n", dlerror());
    }
  } else {
    if ((p = getenv (CNS_PORT_ENV)) || ((p = getconfent (CNS_SCE, "PORT", 0)))) {
      sin.sin_port = htons ((unsigned short)atoi (p));
    } else if ((sp = getservbyname (CNS_SVC, "tcp"))) {
      sin.sin_port = sp->s_port;
    } else {
      sin.sin_port = htons ((unsigned short)CNS_PORT);
    }
  }
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  serrno = 0;
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    nslogit (func, NS002, "setsockopt", neterror());
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    nslogit (func, NS002, "bind", neterror());
    return (CONFERR);
  }
  listen (s, 5) ;

  FD_SET (s, &readmask);

  /* main loop */

  while (1) {
    if (being_shutdown) {
      int nb_active_threads = 0;
      for (i = 0; i < nbthreads; i++) {
        if ((Cns_srv_thread_info + i)->s >= 0) {
          nb_active_threads++;
          continue;
        }
        if ((Cns_srv_thread_info + i)->dbfd.connected)
          (void) Cns_closedb (&(Cns_srv_thread_info + i)->dbfd);
        (void) Csec_clearContext (&(Cns_srv_thread_info + i)->sec_ctx);
      }
      if (nb_active_threads == 0)
        return (0);
    }
    if (FD_ISSET (s, &readfd)) {
      FD_CLR (s, &readfd);
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
      {
        int on = 1;
        /* Set socket option */
        setsockopt(rqfd, SOL_SOCKET, SO_KEEPALIVE,(char *) &on, sizeof(on));
      }
#endif
      if ((thread_index = Cpool_next_index (ipool)) < 0) {
        nslogit (func, NS002, "Cpool_next_index",
                 sstrerror(serrno));
        if (serrno == SEWOULDBLOCK) {
          sendrep (rqfd, CNS_RC, serrno);
          continue;
        } else
          return (SYERR);
      }
      (Cns_srv_thread_info + thread_index)->s = rqfd;
      (Cns_srv_thread_info + thread_index)->secure = security;
      if (Cpool_assign (ipool, &doit,
                        Cns_srv_thread_info + thread_index, 1) < 0) {
        (Cns_srv_thread_info + thread_index)->s = -1;
        nslogit (func, NS002, "Cpool_assign", sstrerror(serrno));
        return (SYERR);
      }
    }
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CHECKI;
    timeval.tv_usec = 0;
    if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      FD_ZERO (&readfd);
    }
  }
}

int main(int argc,
         char **argv)
{
  struct main_args main_args;

  if ((maxfds = Cinitdaemon ("nsd", NULL)) < 0)
    exit (SYERR);
  main_args.argc = argc;
  main_args.argv = argv;
  exit (Cns_main (&main_args));
}

int getreq(struct Cns_srv_thread_info *thip,
           int *magic,
           int *req_type,
           char **req_data,
           const char **clienthost)
{
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  struct hostent *hp;
  struct timeval tv;
  int l;
  unsigned int msglen;
  int n;
  char *rbp;
  char req_hdr[3*LONGSIZE];

  /* record the start time of this request */

  gettimeofday(&tv, NULL);
  thip->starttime = ((double)tv.tv_sec * 1000) + ((double)tv.tv_usec / 1000);

  serrno = 0;
  l = netread_timeout (thip->s, req_hdr, sizeof(req_hdr), CNS_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG (rbp, n);
    *magic = n;
    unmarshall_LONG (rbp, n);
    *req_type = n;
    unmarshall_LONG (rbp, msglen);
    if (msglen > ONE_MB) {
      nslogit (func, NS046, ONE_MB);
      return (E2BIG);
    }
    l = msglen - sizeof(req_hdr);
    if (msglen > REQBUFSZ && (*req_data = malloc (l)) == NULL) {
      return (ENOMEM);
    }
    n = netread_timeout (thip->s, *req_data, l, CNS_TIMEOUT);
    if (being_shutdown) {
      return (ENSNACT);
    }
    if (getpeername (thip->s, (struct sockaddr *) &from, &fromlen) < 0) {
      nslogit (func, NS002, "getpeername", neterror());
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
      nslogit (func, NS004, l);
    else if (l < 0) {
      nslogit (func, NS002, "netread", neterror());
      if (serrno == SETIMEDOUT)
        return (SETIMEDOUT);
    }
    return (SEINTERNAL);
  }
}

int procdirreq(int magic,
               int req_type,
               char *req_data,
               const char *clienthost,
               struct Cns_srv_thread_info *thip)
{
  int bod = 1;
  int c;
  struct Cns_class_metadata class_entry;
  DBLISTPTR dblistptr;
  int endlist = 0;
  struct Cns_file_metadata fmd_entry;
  struct Cns_symlinks lnk_entry;
  int new_req_type = -1;
  int rc = 0;
  fd_set readfd, readmask;
  char *req_data2;
  struct Cns_seg_metadata smd_entry;
  DBLISTPTR smdlistptr;
  struct timeval timeval;
  struct Cns_user_metadata umd_entry;

  memset (&class_entry, 0, sizeof(struct Cns_class_metadata));
  memset (&lnk_entry,   0, sizeof(struct Cns_symlinks));
  memset (&smd_entry,   0, sizeof(struct Cns_seg_metadata));

  memset (&dblistptr, 0, sizeof(DBLISTPTR));
  if (req_type == CNS_OPENDIR) {
    memset (&smdlistptr, 0, sizeof(DBLISTPTR));
    if ((c = Cns_srv_opendir (magic, req_data, clienthost, thip)))
      return (c);
  } else if (req_type == CNS_LISTCLASS) {
    if ((c = Cns_srv_listclass (req_data, clienthost, thip,
                                &class_entry, endlist, &dblistptr)))
      return (c);
  } else if (req_type == CNS_LISTLINKS) {
    if ((c = Cns_srv_listlinks (req_data, clienthost, thip,
                                &lnk_entry, endlist, &dblistptr)))
      return (c);
  } else {
    if ((c = Cns_srv_listtape (magic, req_data, clienthost, thip,
                               &fmd_entry, &smd_entry, endlist, &dblistptr)))
      return (c);
  }
  sendrep (thip->s, CNS_IRC, 0);

  /* wait for readdir/listclass/listtape requests and process them */

  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CNS_DIRTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0)
      endlist = 1;
    req_data2 = req_data;
    if ((rc = getreq (thip, &magic, &new_req_type, &req_data2, &clienthost)))
      endlist = 1;
    if (req_type == CNS_OPENDIR) {
      if (new_req_type != CNS_READDIR)
        endlist = 1;
      c = Cns_srv_readdir (magic, req_data2, clienthost, thip,
                           &fmd_entry, &smd_entry, &umd_entry,
                           endlist, &dblistptr, &smdlistptr, &bod);
    } else if (req_type == CNS_LISTCLASS) {
      if (new_req_type != CNS_LISTCLASS)
        endlist = 1;
      c = Cns_srv_listclass (req_data2, clienthost, thip,
                             &class_entry, endlist, &dblistptr);
    } else if (req_type == CNS_LISTLINKS) {
      if (new_req_type != CNS_LISTLINKS)
        endlist = 1;
      c = Cns_srv_listlinks (req_data2, clienthost, thip,
                             &lnk_entry, endlist, &dblistptr);
    } else {
      if (new_req_type != CNS_LISTTAPE)
        endlist = 1;
      c = Cns_srv_listtape (magic, req_data2, clienthost, thip,
                            &fmd_entry, &smd_entry, endlist, &dblistptr);
    }
    if (req_data2 != req_data)
      free (req_data2);
    if (c)
      return (c);
    if (endlist) break;
    sendrep (thip->s, CNS_IRC, 0);
  }
  return (rc);
}

int procreq(int magic,
            int req_type,
            char *req_data,
            const char *clienthost,
            struct Cns_srv_thread_info *thip)
{
  int c;
#ifdef USE_VOMS
  char **fqan = NULL;
  int nbfqans = 0;
#endif

  /* connect to the database if not done yet */

  if (! (&thip->dbfd)->connected ) {
    if (Cupv_seterrbuf (thip->errbuf, PRTBUFSZ)) {
      c = SEINTERNAL;
      sendrep (thip->s, MSG_ERR, "Cupv_seterrbuf error: %s\n",
               sstrerror(serrno));
      return (c);
    }
    if (!being_shutdown) {
      if (Cns_opendb (&thip->dbfd) < 0) {
        c = serrno;
        sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
        return (c);
      }
      nslogit (func, "database connection established\n");
    }
  }

#ifdef VIRTUAL_ID
  if (thip->Csec_uid == -1) {
#ifdef USE_VOMS
    fqan = Csec_server_get_client_fqans (&thip->sec_ctx, &nbfqans);
    if (nbfqans > 1) nbfqans = 1;
#endif
    if ((c = getidmap (&thip->dbfd, thip->Csec_auth_id, nbfqans, fqan,
                       &thip->Csec_uid, &thip->Csec_gid))) {
      sendrep (thip->s, MSG_ERR, "Could not get virtual id: %s !\n",
               sstrerror (c));
      return (SENOMAPFND);
    }
  }
#endif
  switch (req_type) {
  case CNS_ACCESS:
    c = Cns_srv_access (req_data, clienthost, thip);
    break;
  case CNS_CHCLASS:
    c = Cns_srv_chclass (req_data, clienthost, thip);
    break;
  case CNS_CHDIR:
    c = Cns_srv_chdir (req_data, clienthost, thip);
    break;
  case CNS_CHMOD:
    c = Cns_srv_chmod (req_data, clienthost, thip);
    break;
  case CNS_CHOWN:
    c = Cns_srv_chown (req_data, clienthost, thip);
    break;
  case CNS_CREAT:
    c = Cns_srv_creat (magic, req_data, clienthost, thip);
    break;
  case CNS_DELCLASS:
    c = Cns_srv_deleteclass (req_data, clienthost, thip);
    break;
  case CNS_DELCOMMENT:
    c = Cns_srv_delcomment (req_data, clienthost, thip);
    break;
  case CNS_DELETE:
    c = Cns_srv_delete (req_data, clienthost, thip);
    break;
  case CNS_DROPSEGS:
    c = Cns_srv_dropsegs (req_data, clienthost, thip);
    break;
  case CNS_DELSEGBYCOPYNO:
    c = Cns_srv_delsegbycopyno (req_data, clienthost, thip);
    break;
  case CNS_ENTCLASS:
    c = Cns_srv_enterclass (req_data, clienthost, thip);
    break;
  case CNS_GETACL:
    c = Cns_srv_getacl (req_data, clienthost, thip);
    break;
  case CNS_GETCOMMENT:
    c = Cns_srv_getcomment (req_data, clienthost, thip);
    break;
  case CNS_GETPATH:
    c = Cns_srv_getpath (req_data, clienthost, thip);
    break;
  case CNS_GETSEGAT:
    c = Cns_srv_getsegattrs (magic, req_data, clienthost, thip);
    break;
  case CNS_LCHOWN:
    c = Cns_srv_lchown (req_data, clienthost, thip);
    break;
  case CNS_LISTCLASS:
    c = procdirreq (magic, req_type, req_data, clienthost, thip);
    break;
  case CNS_LISTTAPE:
    c = procdirreq (magic, req_type, req_data, clienthost, thip);
    break;
  case CNS_LSTAT:
    c = Cns_srv_lstat (req_data, clienthost, thip);
    break;
  case CNS_MKDIR:
    c = Cns_srv_mkdir (magic, req_data, clienthost, thip);
    break;
  case CNS_MODCLASS:
    c = Cns_srv_modifyclass (req_data, clienthost, thip);
    break;
  case CNS_OPENDIR:
    c = procdirreq (magic, req_type, req_data, clienthost, thip);
    break;
  case CNS_QRYCLASS:
    c = Cns_srv_queryclass (req_data, clienthost, thip);
    break;
  case CNS_READLINK:
    c = Cns_srv_readlink (req_data, clienthost, thip);
    break;
  case CNS_RENAME:
    c = Cns_srv_rename (req_data, clienthost, thip);
    break;
  case CNS_RMDIR:
    c = Cns_srv_rmdir (req_data, clienthost, thip);
    break;
  case CNS_SETACL:
    c = Cns_srv_setacl (req_data, clienthost, thip);
    break;
  case CNS_SETATIME:
    c = Cns_srv_setatime (req_data, clienthost, thip);
    break;
  case CNS_SETCOMMENT:
    c = Cns_srv_setcomment (req_data, clienthost, thip);
    break;
  case CNS_SETFSIZE:
    c = Cns_srv_setfsize (magic, req_data, clienthost, thip);
    break;
  case CNS_SETFSIZECS:
    c = Cns_srv_setfsizecs (magic, req_data, clienthost, thip);
    break;
  case CNS_SETSEGAT:
    c = Cns_srv_setsegattrs (magic, req_data, clienthost, thip);
    break;
  case CNS_STAT:
    c = Cns_srv_stat (req_data, clienthost, thip);
    break;
  case CNS_STATCS:
    c = Cns_srv_statcs (req_data, clienthost, thip);
    break;
  case CNS_SYMLINK:
    c = Cns_srv_symlink (req_data, clienthost, thip);
    break;
  case CNS_UNDELETE:
    c = Cns_srv_undelete (req_data, clienthost, thip);
    break;
  case CNS_UNLINK:
    c = Cns_srv_unlink (req_data, clienthost, thip);
    break;
  case CNS_UNLINKBYVID:
    c = Cns_srv_unlinkbyvid (req_data, clienthost, thip);
    break;
  case CNS_UTIME:
    c = Cns_srv_utime (req_data, clienthost, thip);
    break;
  case CNS_REPLACESEG:
    c = Cns_srv_replaceseg (magic, req_data, clienthost, thip);
    break;
  case CNS_REPLACETAPECOPY:
    c = Cns_srv_replacetapecopy (magic, req_data, clienthost, thip);
    break;
  case CNS_UPDATESEG_CHECKSUM:
    c = Cns_srv_updateseg_checksum (magic, req_data, clienthost, thip);
    break;
  case CNS_UPDATESEG_STATUS:
    c = Cns_srv_updateseg_status (req_data, clienthost, thip);
    break;
  case CNS_UPDATEFILE_CHECKSUM:
    c = Cns_srv_updatefile_checksum (req_data, clienthost, thip);
    break;
  case CNS_STARTTRANS:
    c = proctransreq (magic, req_data, clienthost, thip);
    break;
  case CNS_ENDTRANS:
    c = Cns_srv_endtrans (req_data, clienthost, thip);
    break;
  case CNS_ABORTTRANS:
    c = Cns_srv_aborttrans (req_data, clienthost, thip);
    break;
  case CNS_LISTLINKS:
    c = procdirreq (magic, req_type, req_data, clienthost, thip);
    break;
  case CNS_SETFSIZEG:
    c = Cns_srv_setfsizeg (magic, req_data, clienthost, thip);
    break;
  case CNS_STATG:
    c = Cns_srv_statg (req_data, clienthost, thip);
    break;
  case CNS_LASTFSEQ:
    c = Cns_srv_lastfseq (magic, req_data, clienthost, thip);
    break;
  case CNS_BULKEXIST:
    c = Cns_srv_bulkexist (req_data, clienthost, thip);
    break;
  case CNS_TAPESUM:
    c = Cns_srv_tapesum (req_data, clienthost, thip);
    break;
  case CNS_STARTSESS:
    c = procsessreq (magic, req_data, clienthost, thip);
    break;
  case CNS_ENDSESS:
    c = Cns_srv_endsess (req_data, clienthost, thip);
    break;
  case CNS_DU:
    c = Cns_srv_du (req_data, clienthost, thip);
    break;
  case CNS_GETGRPID:
    c = Cns_srv_getgrpbynam (req_data, clienthost, thip);
    break;
  case CNS_GETGRPNAM:
    c = Cns_srv_getgrpbygid (req_data, clienthost, thip);
    break;
  case CNS_GETIDMAP:
    c = Cns_srv_getidmap (req_data, clienthost, thip);
    break;
  case CNS_GETUSRID:
    c = Cns_srv_getusrbynam (req_data, clienthost, thip);
    break;
  case CNS_GETUSRNAM:
    c = Cns_srv_getusrbyuid (req_data, clienthost, thip);
    break;
  case CNS_MODGRPMAP:
    c = Cns_srv_modgrpmap (req_data, clienthost, thip);
    break;
  case CNS_MODUSRMAP:
    c = Cns_srv_modusrmap (req_data, clienthost, thip);
    break;
  case CNS_RMGRPMAP:
    c = Cns_srv_rmgrpmap (req_data, clienthost, thip);
    break;
  case CNS_RMUSRMAP:
    c = Cns_srv_rmusrmap (req_data, clienthost, thip);
    break;
  case CNS_GETLINKS:
    c = Cns_srv_getlinks (req_data, clienthost, thip);
    break;
  case CNS_ENTGRPMAP:
    c = Cns_srv_entergrpmap (req_data, clienthost, thip);
    break;
  case CNS_ENTUSRMAP:
    c = Cns_srv_enterusrmap (req_data, clienthost, thip);
    break;
  case CNS_PING:
    c = Cns_srv_ping(req_data, clienthost, thip);
    break;
  default:
    sendrep (thip->s, MSG_ERR, NS003, req_type);
    c = SEOPNOTSUP;
  }
  return (c);
}

void *
doit(arg)
     void *arg;
{
  int c;
  const char *clienthost = NULL;
  const char *clientip = NULL;
  int magic;
  char *req_data;
  char reqbuf[REQBUFSZ-3*LONGSIZE];
  int req_type = 0;
  struct Cns_srv_thread_info *thip = (struct Cns_srv_thread_info *) arg;

  /* There is a field in the Cns_srv_thread to specify if the socket is the secure or not        */
  /* It should be removed once the unsecure mode is not supported anymore, and next "if" as well */
  char username[CA_MAXUSRNAMELEN+1];
  int clientport;
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  struct hostent *hp;
  char ipbuf[INET_ADDRSTRLEN];

  if (thip->secure) {
    if (getpeername (thip->s, (struct sockaddr *) &from, &fromlen) < 0) {
      nslogit (func, NS002, "getpeername", neterror());
      return NULL;
    }
    hp = Cgethostbyaddr ((char *)(&from.sin_addr),
                         sizeof(struct in_addr), from.sin_family);
    if (hp == NULL)
      clienthost = inet_ntoa (from.sin_addr);
    else
      clienthost = hp->h_name;

    clientport = ntohs(from.sin_port);

    clientip = inet_ntop(AF_INET, &from.sin_addr, ipbuf, sizeof(ipbuf));
    nslogit (func, "incoming request from: %s, %s [%d]\n", clienthost, clientip, clientport);

    Csec_server_reinitContext (&thip->sec_ctx, CSEC_SERVICE_TYPE_HOST, NULL);
    if (Csec_server_establishContext (&thip->sec_ctx, thip->s) < 0) {
      nslogit (func, "could not establish security context with %s [%d]: %s!\n",
               clientip, clientport, Csec_getErrorMessage());
      sendrep (thip->s, CNS_RC, ESEC_NO_CONTEXT);
      thip->s = -1;
      return NULL;
    }
    Csec_server_getClientId (&thip->sec_ctx, &thip->Csec_mech, &thip->Csec_auth_id);
    if (Csec_mapToLocalUser (thip->Csec_mech, thip->Csec_auth_id,
                             username,CA_MAXUSRNAMELEN, &thip->Csec_uid, &thip->Csec_gid) < 0) {
      nslogit (func, "could not map to local user from %s [%d]: %s !\n",
               clientip, clientport, sstrerror (serrno));
      sendrep (thip->s, CNS_RC, serrno);
      thip->s = -1;
      return NULL;
    }
    nslogit (func, "users principal %s - mapped to %s\n", thip->Csec_auth_id, username);
  }

  /*******************************************************************************************/
  /* This code should be uncommented once the services run in trusted hosts                  */
  /* Trusted host concept should be supported. At the moment that unsecure                   */
  /* mode will be stopped the stager won't be able to contact the ns unless the CSEC_MECH=ID */
  /* is provided and that means no security.                                                 */
  /*******************************************************************************************/

  /* if (strcmp (thip->Csec_mech, "ID") == 0 ||
     Csec_isIdAService (thip->Csec_mech, thip->Csec_auth_id) >= 0) {
     if (isTrustedHost (thip->s, localhost, localdomain, CNS_SCE, "TRUST")) {
     if (Csec_server_getAuthorizationId (&thip->sec_ctx,
     &thip->Csec_mech, &thip->Csec_auth_id) < 0) {
     thip->uid = 0;
     thip->_gid = 0;
     #ifndef VIRTUAL_ID
     } else if (Csec_mapToLocalUser (thip->Csec_mech, thip->Csec_auth_id,
     NULL, 0, &thip->uid, &thip->Csec) < 0) {
     nslogit (func, "Could not map to local user: %s !\n",
     sstrerror (serrno));
     sendrep (thip->s, CNS_RC, serrno);
     thip->s = -1;
     return NULL;
     #else
     } else {
     thip->uid = (uid_t) -1;
     thip->gid = (gid_t) -1;
     #endif
     }
     } else {
     nslogit (func, "Host is not trusted\n");
     sendrep (thip->s, CNS_RC, EACCES);
     thip->s = -1;
     return NULL;
     }
     #ifndef VIRTUAL_ID
     } else if (Csec_mapToLocalUser (thip->Csec_mech, thip->Csec_auth_id,
     NULL, 0, &thip->uid, &thip->gid) < 0) {
     nslogit (func, "Could not map to local user: %s !\n",
     sstrerror (serrno));
     sendrep (thip->s, CNS_RC, serrno);
     thip->s = -1;
     return NULL;
     #else
     } else {
     thip->Csec_uid = (uid_t) -1;
     thip->Csec_gid = (gid_t) -1;
     #endif
     }
  */
  req_data = reqbuf;
  if ((c = getreq (thip, &magic, &req_type, &req_data, &clienthost)) == 0) {
    c = procreq (magic, req_type, req_data, clienthost, thip);
    sendrep (thip->s, CNS_RC, c);
  } else if (c > 0) {
    sendrep (thip->s, CNS_RC, c);
  } else {
    netclose (thip->s);
  }

  if (req_data != reqbuf)
    free (req_data);
  thip->s = -1;
  return (NULL);

}

int procsessreq(int magic,
                char *req_data,
                const char *clienthost,
                struct Cns_srv_thread_info *thip)
{
  int req_type = -1;
  int rc = 0;
  fd_set readfd, readmask;
  char *req_data2;
  struct timeval timeval;

  (void) Cns_srv_startsess (req_data, clienthost, thip);
  sendrep (thip->s, CNS_IRC, 0);

  /* wait for requests and process them */

  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CNS_TRANSTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      return (SEINTERNAL);
    }
    req_data2 = req_data;
    if ((rc = getreq (thip, &magic, &req_type, &req_data2, &clienthost))) {
      if (req_data2 != req_data)
        free (req_data2);
      return (rc);
    }
    rc = procreq (magic, req_type, req_data2, clienthost, thip);
    if (req_data2 != req_data)
      free (req_data2);
    if (req_type == CNS_ENDSESS) break;
    sendrep (thip->s, CNS_IRC, rc);
  }
  return (rc);
}

int proctransreq(int magic,
                 char *req_data,
                 const char *clienthost,
                 struct Cns_srv_thread_info *thip)
{
  int req_type = -1;
  int rc = 0;
  fd_set readfd, readmask;
  char *req_data2;
  struct timeval timeval;

  (void) Cns_srv_starttrans (magic, req_data, clienthost, thip);
  sendrep (thip->s, CNS_IRC, 0);

  /* wait for requests and process them */

  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CNS_TRANSTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      (void) Cns_srv_aborttrans (req_data, clienthost, thip);
      return (SEINTERNAL);
    }
    req_data2 = req_data;
    if ((rc = getreq (thip, &magic, &req_type, &req_data2, &clienthost))) {
      (void) Cns_srv_aborttrans (req_data2, clienthost, thip);
      if (req_data2 != req_data)
        free (req_data2);
      return (rc);
    }
    rc = procreq (magic, req_type, req_data2, clienthost, thip);
    if (req_data2 != req_data)
      free (req_data2);
    if (req_type == CNS_ENDTRANS || req_type == CNS_ABORTTRANS) break;
    if (rc && req_type != CNS_ACCESS &&
        req_type != CNS_DU && req_type != CNS_GETACL &&
        req_type != CNS_GETCOMMENT && req_type != CNS_GETLINKS &&
        req_type != CNS_GETPATH && req_type != CNS_LSTAT &&
        req_type != CNS_READLINK && req_type != CNS_STAT &&
        req_type != CNS_STATCS && req_type != CNS_STATG &&
        req_type != CNS_GETGRPID && req_type != CNS_GETGRPNAM &&
        req_type != CNS_GETUSRID && req_type != CNS_GETUSRNAM &&
        req_type != CNS_LASTFSEQ && req_type != CNS_PING &&
        req_type != CNS_BULKEXIST && req_type != CNS_TAPESUM) break;
    sendrep (thip->s, CNS_IRC, rc);
  }
  return (rc);
}
