/*
 * $Id: rfio_serv.c,v 1.39 2009/08/18 09:43:00 waldron Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rfio_serv.c  SHIFT remote file access super server                   */

#define RFIO_KERNEL     1               /* KERNEL part of the programs  */

#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "Castor_limits.h"
#include "common.h"
#include "rfio.h"                       /* Remote file I/O              */
#include "rfio_constants.h"
#include "rfioacct.h"
#include "rfio_calls.h"
#include "u64subr.h"
#include <signal.h>   /* Signal handling  */
#include <Cnetdb.h>
#include <syslog.h>   /* System logger  */
#include <log.h>                        /* Genralized error logger      */
#include <sys/time.h>                   /* time definitions             */
#include <sys/param.h>                  /* System parameters            */
#include <sys/wait.h>   /* wait, wait3, wait4 (BSD) */
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#if defined(linux)
#include <sys/types.h>
#include <sys/wait.h>                   /* wait, wait3, wait4 (BSD)     */
#include <sys/resource.h>
#endif /* linux */

#include <Cpwd.h>
#include <Cgrp.h>
static struct passwd stagersuperuser;
static int have_stagersuperuser = 0; /* Default is no alternate super-user */

int exit_code_from_last_child = -1;
int have_a_child = 0;

#include <Csec_api.h>
Csec_context_t ctx;
uid_t peer_uid;
gid_t peer_gid;
int Csec_service_type;

extern char     *getconfent();

#define SO_BUFSIZE      20*1024         /* Default socket buffer size   */

extern FILE     *srpopen();             /* server remote popen()        */
extern int      sropen();               /* server remote open()         */
extern int      sropen_v3();            /* server remote open()         */
extern int      srclose();              /* server remote close()        */
extern int      srclose_v3();           /* server remote close()        */
extern int      srwrite();              /* server remote write()        */
extern int      srwrite_v3();           /* server remote write()        */
extern int      srread();               /* server remote read()         */
extern int      srread_v3();            /* server remote read()         */
extern int      srreadahead();          /* server remote read() ahead   */
extern int      srlseek();              /* server remote lseek()        */
extern int      srlseek_v3();           /* server remote lseek()        */
extern int      srpreseek();            /* server remote preseek()      */
extern int      srstat();               /* server remote stat()         */
extern int      srfstat();              /* server remote fstat()        */
extern void     serrmsg();              /* server remote errmsg()       */
extern int      srrequest();            /* server read request          */
extern int      srmkdir();              /* server remote mkdir()        */
extern int      srrmdir();              /* server remote rmdir()        */
extern int      srrename();             /* server remote rename()       */
extern int      srlockf();              /* server remote lockf()        */
extern int      srchmod();              /* server remote chmod()        */

extern int      sropen64();             /* server remote open()         */
extern int      sropen64_v3();          /* server remote open()         */
extern int      srclose64_v3();         /* server remote close()        */
extern int      srwrite64();            /* server remote write()        */
extern int      srwrite64_v3();         /* server remote write()        */
extern int      srread64();             /* server remote read()         */
extern int      srread64_v3();          /* server remote read()         */
extern int      srreadahd64();          /* server remote read() ahead   */
extern int      srlseek64();            /* server remote lseek()        */
extern int      srpreseek64();          /* server remote preseek()      */
extern int      srstat64();             /* server remote stat()         */
extern int      srlockf64();            /* server remote lockf()        */
extern int      srfstat64();            /* server remote fstat()        */
extern int      srchown();              /* server remote chown()        */
extern int      srfchmod();             /* server remote fchmod()       */
extern int      srfchown();             /* server remote fchown()       */
extern int      srlstat() ;             /* server remote lstat()        */
extern int      srlstat64() ;           /* server remote lstat()        */
extern int      srsymlink() ;           /* server remote symlink()      */
extern int      srreadlink() ;          /* server remote readlink()     */
extern DIR     *sropendir();            /* server remote opendir()      */
extern int      srreaddir();            /* server remote readdir()      */
extern int      srrewinddir();          /* server remote rewinddir()    */
extern int      srclosedir();           /* server remote closedir()     */
extern int      sraccess();             /* server remote access()       */
extern int      srxyopen();             /* server remote xyopen()       */
extern int      srxyclos();             /* server remote xyclos()       */
extern int      srxywrit();             /* server remote xywrit()       */
extern int      srxyread();             /* server remote xyread()       */
extern int      setnetio();             /* set network characteristics  */

static int      standalone=0;   /* standalone flag                      */
static char     logfile[CA_MAXPATHLEN+1];   /* log file name buffer                 */

FILE            *streamf ;      /* FILE pointer for popen() calls       */

int     doit();                 /* doit() forward reference             */
void    check_child_exit();

#if defined(linux)
void    reaper (int);           /* reaper() forward reference           */
#endif /* linux */

fd_set readfd, readmask;
struct timeval timeval;

#ifndef WTERMSIG
#define WTERMSIG(x)     (((union wait *)&(x))->w_termsig)
#endif /* WTERMSIG */
#ifndef WSTOPSIG
#define WSTOPSIG(x)     (((union wait *)&(x))->w_stopsig)
#endif /* WSTOPSIG */

static int setsock_ceiling = 256 * 1024;
int max_rcvbuf;
int max_sndbuf;
char *forced_filename = NULL;
int forced_umask = -1;
int ignore_uid_gid = 0;
u_signed64 subrequest_id = 0;
void *handler_context = NULL;

/* Use when option -Z is set : then this is put to zero */
/* only if the close hook was executed successfully */
int forced_mover_exit_error = 1;


int set_rcv_sockparam(int s,
                      int value)
{
  if (setsockopt(s,SOL_SOCKET,SO_RCVBUF,(char *)&value, sizeof(value)) < 0) {
    if (errno != ENOBUFS)
      {
        log(LOG_ERR, "setsockopt rcvbuf(): %s\n",strerror(errno));
        exit(1);
      }
    else
      return(-1);
  }
  /* else */
  return(value);
}

int set_snd_sockparam(int s,
                      int value)
{
  if (setsockopt(s,SOL_SOCKET,SO_SNDBUF,(char *)&value, sizeof(value)) < 0) {
    if (errno != ENOBUFS)
      {
        log(LOG_ERR, "setsockopt sndbuf(): %s\n",strerror(errno));
        exit(1);
      }
    else
      return(-1);
  }
  /* else */
  return(value);
}

int main (int     argc,
          char    **argv)
{
  extern int      opterr, optind;         /* required by getopt(3)*/
  extern char     *optarg;                /* required by getopt(3)*/
  register int    option;
  int      loglevel = LOG_INFO;      /* Default log level    */
  int      debug = 0;                /* Debug flag           */
  int      port = 0;                 /* Non-standard port    */
  int      logging = 0;              /* Default not to log   */
  int      singlethread = 0;         /* Single threaded      */
  int      lfname = 0;               /* log file given       */
  int      nodetach = 0;             /* Default detach proces*/
  int      Socket_parent = -1;       /* Somebody's giving us a file desciptor ? */
  int      Socket_parent_port = -1;  /* Somebody's giving us a yet binded port !? */
  int      once_only = 0;            /* Process only one request */
  char     *curdir = NULL;           /* Current directory    */
  char     *p;
  uid_t    uid = 0;      /* User id of the user issuing the request*/
  gid_t    gid = 0;        /* Group id of the user issuing the request*/
  register int  s, ns;
  int      i, pid;
  struct   sockaddr_in sin, from;
  socklen_t fromlen;
  char     localhost[MAXHOSTNAMELEN];     /* Local host name      */
  int             mode;
  register int    maxfds=0;               /* max. # of file descr.*/
#if defined(linux)
  struct sigaction sa;
#endif
#ifdef STAGERSUPERUSER
  struct group *this_group;             /* Group structure */
  struct passwd *this_passwd;             /* password structure pointer */
#endif
  int select_status;
  int select_timeout = CHECKI;

  strcpy(logfile, "syslog"); /* default logfile */
  opterr++;

  while ((option = getopt(argc,argv,"sdltf:p:P:D:M:nS:1R:T:UZ:u:g:")) != EOF) {
    switch (option) {
    case 'd':
      debug++;
      break;
    case 's':
      standalone++;
      break;
    case 'f':
      lfname++;
      strcpy(logfile,optarg);
      break;
    case 'l':
      logging++;
      break;
    case 't':
      singlethread++;
      break;
    case 'T':
      select_timeout=atoi(optarg);
      break;
    case 'M':
      forced_umask=atoi(optarg);
      break;
    case 'p':
      port=atoi(optarg);
      break;
    case 'D':
      curdir = optarg;
      break;
    case 'n':
      nodetach++;
      break;
    case 'S':
      Socket_parent=atoi(optarg);
      break;
    case 'P':
      Socket_parent_port=atoi(optarg);
      break;
    case '1':
      once_only++;
      break;
    case 'U':
      ignore_uid_gid++;
      break;
    case 'Z':
      subrequest_id = strtou64(optarg);
      break;
    case 'u':
      uid = strtou64(optarg);
      break;
    case 'g':
      gid = strtou64(optarg);
      break;
    default:
      fprintf(stderr,"Unknown option '%c'\n", option);
      exit(1);
    }
  }
  if (optind < argc) {
    forced_filename = argv[optind];
  }

  /*
   * Ignoring SIGXFSZ signals before logging anything
   */
  signal(SIGXFSZ,SIG_IGN);

  if (debug) {
    loglevel = LOG_DEBUG;
  }
  if (logging && !lfname) {
    strcpy(logfile, RFIOLOGFILE);
  }
  if (!(strcmp(logfile, "stderr"))) {
    strcpy(logfile, "");
  }
  if (standalone) {
    /*
     * Trap SIGCLD, SIGCHLD
     */
#if defined(linux)
    sa.sa_handler = reaper;
    sa.sa_flags = SA_RESTART;
    sigaction (SIGCHLD, &sa, NULL);
#endif /* linux */

    maxfds=getdtablesize();

    /*
     * disassociate controlling terminal
     */
    if (!debug) {
      for (i=0; i< maxfds; i++) {
        if (i != Socket_parent) {
          (void) close(i);
        }
      }

      /* Redirect standard files to /dev/null */
      freopen( "/dev/null", "r", stdin);
      freopen( "/dev/null", "w", stdout);
      freopen( "/dev/null", "w", stderr);
      for (i = 3; i< maxfds; i++) {
        if (i != Socket_parent) {
          (void) close(i);
        }
      }

      /*
       * Finally fork ourselves if option -n not specified
       */
      if ( !nodetach ) {
        pid = fork();
        if (pid == -1) {
          perror("main fork");
          exit(1);
        }
        if (pid > 0) exit(0);
        setpgrp();
      } /* if( !nodetach ) */
    } /* if( !debug ) */

    (void) initlog("rfiod", loglevel, logfile);
#if defined(__DATE__) && defined (__TIME__)
    log(LOG_ERR, "%s generated on %s %s %d %d\n",argv[0],__DATE__,__TIME__, uid, gid);
#else
    log(LOG_ERR, "%s\n", argv[0]);
#endif /* __DATE__ && __TIME__ */

    if (gethostname(localhost,sizeof(localhost))) {
      log(LOG_ERR, "gethostname(): %s\n",strerror(errno));
      exit(1);
    }
    log(LOG_ERR, "starting on %s\n", localhost);
    if (forced_filename) {
      log(LOG_ERR, "forced filename %s\n", forced_filename);
    }

    if (curdir) {
      if (chdir(curdir))
        log(LOG_ERR, "Can't set the current directory to `%s`\n", curdir);
      else
        log(LOG_ERR, "Current directory set to '%s'\n", curdir);
    }

    if (Socket_parent >= 0) {
      log(LOG_INFO, "Socket inherited from parent, file descriptor %d\n", Socket_parent);
      s = Socket_parent;
    } else {
      if( (s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        log(LOG_ERR, "socket(): %s\n",strerror(errno));
        exit(1);
      }
    }
    if (Socket_parent_port >= 0) {
      log(LOG_INFO, "Socket already bound to port %d\n", Socket_parent_port);
      port = Socket_parent_port;
    } else {
      if (!port) {

        /* If CSEC define and security requested (set uid and gid) and user not root */
        if (uid > 0 && gid > 0) {
          if ((p = getenv ("SRFIO_PORT")) || (p = getconfent ("SRFIO", "PORT", 0))) {
            sin.sin_port = htons ((unsigned short)atoi (p));
          } else {
            log(LOG_INFO, "using default port number %d\n", (int) SRFIO_PORT);
            sin.sin_port = htons((u_short) SRFIO_PORT);
          }
        } else {
          if ((p = getenv ("RFIO_PORT")) || (p = getconfent ("RFIO", "PORT", 0))) {
            sin.sin_port = htons ((unsigned short)atoi (p));
          } else {
            log(LOG_INFO, "using default port number %d\n", (int) RFIO_PORT);
            sin.sin_port = htons((u_short) RFIO_PORT);
          }
        }
      } else {
        sin.sin_port = htons(port);
      }

      sin.sin_addr.s_addr = htonl(INADDR_ANY);
      sin.sin_family = AF_INET;
      {  /* Re-usable port */
        int bool = 1;
        setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&bool, sizeof(bool));
      }
      if( bind(s, (struct sockaddr*)&sin, sizeof(sin)) < 0 ) {
        log(LOG_ERR, "bind(): %s\n",strerror(errno));
        exit(1);
      }
      listen(s, 5);
    }

    if ( s != INVALID_SOCKET ) FD_SET (s, &readmask);

    max_rcvbuf = setsock_ceiling;
    max_sndbuf = setsock_ceiling;

    for (i = setsock_ceiling ; i >= 16 * 1024 ; i >>= 1) {
      if (set_rcv_sockparam(s, i) == i) {
        max_rcvbuf = i;
        break;
      }
    }
    for (i = setsock_ceiling ; i >= 16 * 1024 ; i >>= 1) {
      if (set_snd_sockparam(s,i) == i) {
        max_sndbuf = i;
        break;
      }
    }

    log(LOG_DEBUG,"setsockopt maxsnd=%d, maxrcv=%d\n", max_sndbuf, max_rcvbuf);

#ifdef STAGERSUPERUSER
    /* Note: default is have_stagersuperuser = 0, e.g. none */

    log(LOG_INFO,"Checking existence of alternate super-user \"%s\" in password file\n", STAGERSUPERUSER);
    if ((this_passwd = Cgetpwnam(STAGERSUPERUSER)) == NULL) {
      log(LOG_ERR, "Cannot Cgetpwnam(\"%s\") (%s)\n",STAGERSUPERUSER,strerror(errno));
      log(LOG_ERR, "Please check existence of account \"%s\" in password file\n", STAGERSUPERUSER);
      log(LOG_ERR, "No alternate super-user in action\n");
    } else {
      stagersuperuser = *this_passwd;
#ifdef STAGERSUPERGROUP
      log(LOG_INFO,"Checking existence of alternate super-group \"%s\" in group file\n", STAGERSUPERGROUP);
      if ((this_group = Cgetgrnam(STAGERSUPERGROUP)) == NULL) {
        log(LOG_ERR, "Cannot Cgetgrnam(\"%s\") (%s)\n",STAGERSUPERGROUP,strerror(errno));
        log(LOG_ERR, "Please check existence of group \"%s\" in group file\n", STAGERSUPERGROUP);
        log(LOG_ERR, "No alternate super-user in action\n");
      } else {
        /* We check that this group is the primary group of the yet found stagersuperuser account */
        log(LOG_INFO,"Checking consistency for alternate super-user\n");
        if (stagersuperuser.pw_gid != this_group->gr_gid) {
          log(LOG_ERR, "\"%s\"'s gid (%d) is not the primary group of account \"%s\" (%d) - No alternate super-user defined\n", STAGERSUPERGROUP, (int) this_group->gr_gid, STAGERSUPERUSER, (int) stagersuperuser.pw_uid);
          log(LOG_ERR, "Please check existence of primary account (\"%s\",\"%s\")=(%d,%d) in password file\n", STAGERSUPERUSER, STAGERSUPERGROUP, (int) stagersuperuser.pw_uid, (int) this_group->gr_gid);
          log(LOG_ERR, "No alternate super-user in action\n");
        } else {
          /* An alternate super-user has been defined */
          log(LOG_INFO, "Allowing the alternate super-user privileges for account: (\"%s\",\"%s\")=(%d,%d)\n", STAGERSUPERUSER, STAGERSUPERGROUP, (int) stagersuperuser.pw_uid, (int) stagersuperuser.pw_gid);
          have_stagersuperuser = 1;
        }
      }
#else
      log(LOG_INFO,"Checking existence of alternate super-group %d in group file\n", (int) stagersuperuser.pw_gid);
      if ((this_group = Cgetgrgid(stagersuperuser.pw_gid)) == NULL) {
        log(LOG_ERR, "Cannot Cgetgrgid(%d) (%s)\n",(int) stagersuperuser.pw_gid,strerror(errno));
        log(LOG_ERR, "Please check existence of group %d in group file\n", (int) stagersuperuser.pw_gid);
        log(LOG_ERR, "No alternate super-user in action\n");
      } else {
        /* An alternate super-user has been defined */
        log(LOG_INFO, "Allowing the alternate super-user privileges for account: (\"%s\",\"%s\")=(%d,%d)\n", STAGERSUPERUSER, this_group->gr_name, (int) stagersuperuser.pw_uid, (int) stagersuperuser.pw_gid);
        have_stagersuperuser = 1;
      }
#endif
    }
#else
    log(LOG_INFO,"No alternate super-user configured (STAGERSUPERUSER)\n");
#endif

    for (;;) {
      check_child_exit((subrequest_id>0 ? have_a_child : 0)); /* check childs [pid,status] */
      if ( (s != INVALID_SOCKET) && FD_ISSET (s, &readfd)) {
        fromlen = sizeof(from);
        ns = accept(s, (struct sockaddr *)&from, &fromlen);
        if( ns < 0 ) {
          log(LOG_DEBUG, "accept(): %s\n",strerror(errno));
          goto select_continue;
        }
        log(LOG_DEBUG, "accepting requests\n");
        if( getpeername(ns, (struct sockaddr*)&from, &fromlen) < 0 ) {
          log(LOG_ERR, "getpeername: %s\n",strerror(errno));
          (void) close(ns);
          goto select_continue;
        }
        {
          char *p;

          if (((p = getenv("RFIOD_TCP_NODELAY")) != NULL) ||
              ((p = getconfent("RFIOD", "TCP_NODELAY", 0)) != NULL)
              ) {
            if ((strcmp(p,"YES") == 0) || (strcmp(p,"yes") == 0)) {
              int rcode = 1;
              if ( setsockopt(ns,IPPROTO_TCP,TCP_NODELAY,(char *)&rcode,sizeof(rcode)) == -1 ) {
                log(LOG_ERR, "setsockopt(..,TCP_NODELAY,...): %s\n",strerror(errno));
              }
            }
          }
        }
        if (!singlethread) {
          pid = fork();
          switch (pid) {
          case -1:
            log(LOG_ERR,"fork(): %s \n",strerror(errno));
            break;
          case 0:                          /* Child  */
            close(s);
            mode = 0;
            doit(ns, &from, mode, uid, gid);
            break;
          }
          have_a_child = 1;
          close(ns);                          /* Parent */
          if ( subrequest_id > 0 ) {
            /*
             * If we are started by the stagerjob we don't allow for
             * more than one connection. Close the listen socket and loop back
             * to wait for the child we just forked.
             */
            close(Socket_parent);
            s = Socket_parent = INVALID_SOCKET;
            continue;
          }
        } else { /* singlethread */
          mode = 1;
          doit(ns, &from, mode, uid, gid);
        }
        FD_CLR (ns, &readfd);
      }
    select_continue:
      if ( s != INVALID_SOCKET ) {
        memcpy (&readfd, &readmask, sizeof(readmask));
        timeval.tv_sec = (once_only && have_a_child) ? 1 : select_timeout;  /* must set each time for linux */
        timeval.tv_usec = 0;
        select_status = select (s + 1, &readfd, NULL, NULL, &timeval);
      } else select_status = 0;
      if ( select_status < 0 ) {
        log(LOG_DEBUG,"select error No %d (%s)\n", errno, strerror(errno));
        if (once_only) {
          if (have_a_child && exit_code_from_last_child >= 0) {
            log(LOG_DEBUG,"Exiting with status %d\n", exit_code_from_last_child);
            exit(exit_code_from_last_child);
          } else if (! have_a_child) {
            /* error and no child : we assume very old client */
            log(LOG_DEBUG,"Exiting with status %d\n", 0);
            exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
          }
        }
        FD_ZERO (&readfd);
      }
      if (select_status == 0 && once_only) { /* Timeout */
        if (have_a_child && exit_code_from_last_child >= 0) {
          log(LOG_DEBUG,"Exiting with status %d\n", exit_code_from_last_child);
          exit(exit_code_from_last_child);
        } else if (! have_a_child) {
          /* timeout and no child : we assume very old client */
          log(LOG_DEBUG,"Exiting with status %d\n", 0);
          exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
        }
      }
    }
  } else {       /* !standalone */

    (void) initlog("rfiod", loglevel, logfile);
    fromlen = sizeof(from);
    log(LOG_DEBUG, "accepting requests\n");
    if (getpeername(0,(struct sockaddr *)&from, &fromlen)<0) {
      log(LOG_ERR, "getpeername: %s\n",strerror(errno));
      exit(1);
    }
    mode = 0;
    doit(0, &from, mode, uid, gid);
  }
  exit(0);
}


void reaper(int dummy)
{
  (void)dummy;
}

int doit(int      s,
         struct sockaddr_in *fromp,
         int mode,
         uid_t uid,
         gid_t gid)
{
  int      request, status;        /* Request Id  number               */
  int      fd = -1;                /* Local fd      -> -1              */
  DIR      *dirp = NULL;           /* Local dir ptr -> NULL            */
  struct   hostent *hp;
  int      lun;
  int      access, yes;
  struct   rfiostat info;
  int      is_remote = 0;              /* Is requestor in another site ? */
  char     from_host[MAXHOSTNAMELEN];  /* Where the request comes from   */
  int      from_port;                  /* Port of the client socket      */
  char     * p1 ;

#if defined(linux)
  struct sigaction sa;
#endif
  char tmpbuf[21], tmpbuf2[21];

#define CLIENT_NAME_SIZE 1000
  char *Csec_mech;
  char *Csec_auth_id;

  /* Check that the uid and gid is set and user is not root */
  /* Condition to be replaced when trusted host is supported */
  if (uid > 0 && gid > 0) {
    /* Perfom the authentication */
    char username[CA_MAXUSRNAMELEN+1];

    log(LOG_INFO, "The user is %d in the group %d\n", uid, gid);

    hp =  Cgethostbyaddr((char *)(&fromp->sin_addr), sizeof(struct in_addr), fromp->sin_family);
    if ( hp == NULL) {
      strcpy(from_host,(char *)inet_ntoa(fromp->sin_addr));
    } else {
      strcpy(from_host,hp->h_name);
    }

    from_port=ntohs(fromp->sin_port);
    log(LOG_INFO, "Connection from  %s [%d]\n", from_host, from_port);

    if (Csec_server_initContext(&ctx, CSEC_SERVICE_TYPE_HOST, NULL)<0) {
      log(LOG_ERR, "Could not initialize context with %s [%d]: %s\n", from_host,from_port, Csec_getErrorMessage());
      closesocket(s);
      exit(1);
    }

    if (Csec_server_establishContext(&ctx, s)<0) {
      log(LOG_ERR, "Could not establish context with %s [%d]: %s\n",  from_host,from_port,Csec_getErrorMessage());
      closesocket(s);
      exit(1);
    }

    /* Getting the client identity */
    Csec_server_getClientId(&ctx, &Csec_mech, &Csec_auth_id);
    log(LOG_INFO, "The client principal is %s %s\n", Csec_mech,Csec_auth_id);

    /* Connection could be done from another castor service */

    //   if ((Csec_service_type = Csec_isIdAService(Csec_mech, Csec_auth_id)) >= 0) {
    //If one we consider working with trusted hosts the code should be added here
    //IsTrustedHost then Csec_server_getAuthorizationId & mapToLocalUser
    //   log(LOG_INFO, "CSEC: Client is castor service type: %d\n", Csec_service_type);
    //  }
    //  else {
    if (Csec_mapToLocalUser(Csec_mech, Csec_auth_id,
                            username, CA_MAXUSRNAMELEN,
                            &peer_uid, &peer_gid) < 0) {
      log(LOG_ERR, "CSEC: Could not map user %s/%s from %s [%d]\n", Csec_mech, Csec_auth_id,from_host,from_port);
    }
    //  closesocket(s);
    //  exit(1);
    // }
    /*Checking if the user just mapped match with the same that started the request */
    if(peer_uid != uid) {
      log(LOG_ERR, "CSEC: The user do not match with the initial oner %s/%d\n", Csec_mech, peer_uid);
      closesocket(s);
      exit(1);
    } else {
      log(LOG_INFO, "Comparing gid %d and peer_gid %d \n",gid ,peer_gid);
      if(peer_gid != gid) {
        log(LOG_ERR, "CSEC: The group id of this group is not valid %s/%d\n", Csec_mech, peer_gid);
        closesocket(s);
        exit(1);
      }
    }

    log(LOG_INFO, "CSEC: Client is %s (%d/%d)\n",
        username,
        peer_uid,
        peer_gid);
    Csec_service_type = -1;
  }

  /*
   * Initializing the info data structure.
   */
  info.readop= 0;
  info.writop= 0 ;
  info.flusop= 0 ;
  info.statop= 0 ;
  info.seekop= 0 ;
  info.presop= 0 ;
  info.aheadop=0 ;
  info.mkdiop= 0 ;
  info.renaop= 0 ;
  info.lockop= 0 ;
  info.rnbr= (off64_t)0 ;
  info.wnbr= (off64_t)0 ;
  /*
   * Use to solve an UltraNet bug
   */
  if (setnetio() <0) {
    shutdown(s, 2);
    close(s);
    exit(1);
  }

  if ( (p1 = getconfent("RFIOD","KEEPALIVE",0)) != NULL && !strcmp(p1,"YES") ) {
    yes = 1;
    if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE,(char *)&yes, sizeof (yes) ) == -1) {
      log(LOG_ERR,"setsockopt(SO_KEEPALIVE) failed\n");
    }
    /*
     * Trap SIGPIPE
     */
    sa.sa_handler = reaper;
    sa.sa_flags = SA_RESTART;
    sigaction (SIGPIPE, &sa, NULL);

  }
  else {
    /*
     * Ignoring SIGPIPE and SIGXFSZ signals.
     */
    (void) signal(SIGPIPE,SIG_IGN) ;
    (void) signal(SIGXFSZ,SIG_IGN) ;
  }
  /*
   * Getting the client host name.
   */
  hp =  Cgethostbyaddr((char *)(&fromp->sin_addr), sizeof(struct in_addr), fromp->sin_family);
  if ( hp == NULL) {
    strcpy(from_host,(char *)inet_ntoa(fromp->sin_addr));
    log(LOG_INFO, "doit(%d): connection from %s\n", s, inet_ntoa(fromp->sin_addr));
  }
  else {
    strcpy(from_host,hp->h_name);
    log(LOG_INFO, "doit(%d): connection from %s\n", s, hp->h_name);
  }
  /*
   * Detect wether client is in or out of site
   */
  {
    int sav_serrno = serrno;
    if ( isremote(fromp->sin_addr,from_host) )
      is_remote++;
    serrno = sav_serrno; /* Failure or not of isremote(), we continue */
  }

#if defined(linux)
  if ( (p1 = getconfent(from_host,"RFIOD_SCHED",0)) != NULL ) {
    int priority, sched_rc;
    priority = atoi(p1);
    log(LOG_INFO,"Trying to set scheduling priority to %d\n",priority);
    sched_rc = setpriority(PRIO_PROCESS,0,priority);
    if ( sched_rc == -1 ) log(LOG_ERR,"setpriority(%d,%d,%d): %s\n",
                              PRIO_PGRP,0,priority,strerror(errno));
  }
#endif /* linux */

  /*
   * Loop on request.
   */
  for (;;) {
    int bet ;
    request = srrequest(s, &bet);
    if ( (request==RQST_OPEN || request==RQST_OPENDIR ||
          request==RQST_XYOPEN) && !bet && is_remote ) {
      log(LOG_ERR,"Attempt to call daemon with expired magic from outside site\n");
      shutdown(s, 2);
      close(s);
      if (mode) return(1); else  exit(1);
    }
    if (request < 0) {
      log(LOG_INFO,"drop_socket(%d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
          s, info.readop, info.aheadop, info.writop, info.flusop, info.statop,
          info.seekop, info.lockop);
      log(LOG_INFO,"drop_socket(%d): %s bytes read and %s bytes written\n",
          s, u64tostr(info.rnbr,tmpbuf,0),u64tostr(info.wnbr,tmpbuf2,0)) ;
      log(LOG_ERR, "fatal error on socket %d: %s\n", s, strerror(errno));

      shutdown(s, 2);
      close(s);
      if (mode) return(1); else  exit(1);
    }
    switch (request) {
    case 0:
      log(LOG_INFO,
          "close_socket(%d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
          s, info.readop, info.aheadop, info.writop, info.flusop, info.statop,
          info.seekop, info.lockop);
      log(LOG_INFO,"close_socket(%d): %s bytes read and %s bytes written\n",
          s, u64tostr(info.rnbr,tmpbuf,0), u64tostr(info.wnbr,tmpbuf2,0)) ;
      log(LOG_ERR, "connection %d dropped by remote end\n", s);
#if defined(SACCT)
      rfioacct(0,0,0,s,0,0,status,errno,&info,NULL,NULL);
#endif /* SACCT */

      shutdown(s, 2);
      if( close(s) < 0 )
        log(LOG_ERR, "Error closing socket fildesc=%d, errno=%d\n", s, errno);
      else
        log(LOG_INFO, "Closing socket fildesc=%d\n", s);
      if( mode ) return(1); else  exit(1);
    case RQST_CHKCON :
      log(LOG_DEBUG, "request type : check connect\n");
      srchk(s) ;
      shutdown(s, 2); close(s);
      if (mode) return(0); else  exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_OPEN  :
      log(LOG_DEBUG, "request type <open()>\n");
      fd = sropen(s,(bet?is_remote:0),(bet?from_host:(char *)NULL), bet);
      log(LOG_DEBUG, "ropen() returned: %d\n",fd);
      break;
    case RQST_OPEN64  :
      log(LOG_DEBUG, "request type <open64()>\n");
      fd = sropen64(s, is_remote, from_host);
      log(LOG_DEBUG, "ropen64() returned: %d\n",fd);
      break;
    case RQST_OPENDIR :
      log(LOG_DEBUG, "request type <opendir()>\n");
      dirp = sropendir(s,is_remote,from_host,bet);
      log(LOG_DEBUG, "ropendir() returned %x\n",dirp);
      break;
    case RQST_CLOSE  :
      log(LOG_DEBUG, "request type <close()>\n");
      status = srclose(s, &info, fd);
      log(LOG_DEBUG,"close() returned %d\n",status);
      fd = -1;
      shutdown(s, 2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_CLOSEDIR  :
      log(LOG_DEBUG, "request type <closedir()>\n");
      status = srclosedir(s,&info,dirp);
      log(LOG_DEBUG,"closedir() returned %d\n",status);
      dirp = NULL;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_READ  :
      info.readop ++ ;
      log(LOG_DEBUG, "request type <read()>\n");
      status = srread(s, &info, fd);
      log(LOG_DEBUG, "rread() returned: %d\n",status);
      break;
    case RQST_READ64  :
      info.readop ++ ;
      log(LOG_DEBUG, "request type <read64()>\n");
      status = srread64(s, &info, fd);
      log(LOG_DEBUG, "rread64() returned: %d\n",status);
      break;
    case RQST_READAHEAD  :
      info.aheadop ++ ;
      log(LOG_DEBUG, "request type <readahead()>\n");
      status = srreadahead(s, &info, fd);
      log(LOG_DEBUG, "rreadahead() returned: %d\n",status);
      break;
    case RQST_READAHD64  :
      info.aheadop ++ ;
      log(LOG_DEBUG, "request type <readahd64()>\n");
      status = srreadahd64(s, &info, fd);
      log(LOG_DEBUG, "rreadahd64() returned: %d\n",status);
      break;
    case RQST_READDIR :
      info.readop++;
      log(LOG_DEBUG, "request type <readdir()>\n");
      status = srreaddir(s,&info,dirp);
      log(LOG_DEBUG, "rreaddir() returned: %d\n",status);
      break;
    case RQST_WRITE  :
      info.writop ++ ;
      log(LOG_DEBUG, "request type <write()>\n");
      status = srwrite(s, &info, fd);
      log(LOG_DEBUG, "rwrite() returned: %d\n",status);
      break;
    case RQST_WRITE64  :
      info.writop ++ ;
      log(LOG_DEBUG, "request type <write64()>\n");
      status = srwrite64(s, &info, fd);
      log(LOG_DEBUG, "rwrite64() returned: %d\n",status);
      break;
    case RQST_FCHMOD :
      log(LOG_DEBUG, "request type <fchmod()>\n");
      status = srfchmod(s, from_host, is_remote, fd) ;
      log(LOG_DEBUG, "fchmod() returned %d\n",status);
      break;
    case RQST_FCHOWN :
      log(LOG_DEBUG, "request type <fchown()>\n");
      status = srfchown(s, from_host, is_remote, fd) ;
      log(LOG_DEBUG, "fchown() returned %d\n",status);
      break;
    case RQST_FSTAT :
      info.statop ++ ;
      log(LOG_DEBUG, "request type <fstat()>\n");
      status = srfstat(s, &info, fd);
      log(LOG_DEBUG, "fstat() returned %d\n",status);
      break;
    case RQST_FSTAT64 :
      log(LOG_DEBUG, "request type <fstat64()>\n");
      status = srfstat64(s, &info, fd);
      log(LOG_DEBUG, "fstat64() returned %d\n",status);
      break;
    case RQST_MSTAT_SEC:
    case RQST_STAT_SEC:
    case RQST_MSTAT:
    case RQST_STAT :
      log(LOG_DEBUG, "request type <stat()>\n");
      status = srstat(s,(bet?is_remote:0),(bet?from_host:(char *)NULL),bet);
      log(LOG_DEBUG, "stat() returned %d\n",status);
      if (request==RQST_STAT || request==RQST_STAT_SEC) {
        shutdown(s, 2); close(s);
        if(mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      }  /* if request == RQST_STAT  */
      break ;
    case RQST_LSTAT_SEC:
    case RQST_LSTAT :
      log(LOG_DEBUG, "request type <lstat()>\n");
      status = srlstat(s,(bet?is_remote:0),(bet?from_host:(char *)NULL),bet);
      log(LOG_DEBUG, "lstat() returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_LSEEK :
      info.seekop ++ ;
      log(LOG_DEBUG, "request type <lseek()>\n");
      status = srlseek(s, &info, fd);
      log(LOG_DEBUG, "lseek() returned %d\n",status);
      break;
    case RQST_LSEEK64 :
    case RQST_LSEEK64_V3 :
      info.seekop ++ ;
      log(LOG_DEBUG, "request type <lseek64()>\n");
      status = srlseek64(s, request, &info, fd);
      log(LOG_DEBUG, "lseek64() returned %d\n",status);
      break;
    case RQST_PRESEEK :
      info.presop ++ ;
      log(LOG_DEBUG, "request type <preseek()>\n");
      status = srpreseek(s, &info, fd);
      log(LOG_DEBUG, "preseek() returned %d\n",status);
      break;
    case RQST_PRESEEK64 :
      info.presop ++ ;
      log(LOG_DEBUG, "request type <preseek64()>\n");
      status = srpreseek64(s, &info, fd);
      log(LOG_DEBUG, "preseek64() returned %d\n",status);
      break;
    case RQST_ERRMSG :
      log(LOG_DEBUG, "request type <errmsg()>\n");
      srerrmsg(s);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_MSYMLINK :
    case RQST_SYMLINK :
      log(LOG_DEBUG, "request type <symlink()>\n");
      status = srsymlink(s,request,(bet?is_remote:0),(bet?from_host:(char *)NULL)) ;
      log(LOG_DEBUG, "srsymlink() returned %d\n", status) ;
      if (request==RQST_SYMLINK) {
        shutdown(s,2); close(s);
        if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      }
      break;
    case RQST_READLINK:
      log(LOG_DEBUG, "request type <readlink()>\n");
      status = srreadlink(s,from_host,is_remote) ;
      shutdown(s,2); close(s);
      log(LOG_DEBUG, "srreadlink() returned %d\n", status) ;
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_REWINDDIR:
      info.seekop ++;
      log(LOG_DEBUG, "request type <rewinddir()>\n");
      status = srrewinddir(s,&info,dirp);
      log(LOG_DEBUG, "srrewinddir() returned %d\n",status);
      break;
    case RQST_STATFS :
      log(LOG_DEBUG, "request type <statfs()>\n");
      status = srstatfs(s) ;
      log(LOG_DEBUG, "statfs() returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_POPEN :
      log(LOG_DEBUG, "request type <popen()>\n");
      streamf = srpopen(s, from_host, (bet?is_remote:0) ) ;
      log(LOG_DEBUG, "srpopen() returned %x\n", streamf ) ;
      break ;
    case RQST_FREAD :
      log(LOG_DEBUG,"request type <fread()>\n");
      status = srfread(s,streamf) ;
      log(LOG_DEBUG, "rfread() returned %d\n",status);
      break ;
    case RQST_FWRITE :
      log(LOG_DEBUG,"request type <fwrite()\n");
      status = srfwrite(s,streamf);
      log(LOG_DEBUG, "rfwrite() returned %d\n",status);
      break ;
    case RQST_PCLOSE :
      log(LOG_DEBUG,"request type <pclose()>\n");
      status = srpclose(s,streamf) ;
      log(LOG_DEBUG,"pclose() returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_ACCESS :
      log(LOG_DEBUG,"request type <access()>\n");
      status = sraccess(s, from_host, (bet?is_remote:0)) ;
      log(LOG_DEBUG,"raccess returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_MKDIR :
      log(LOG_DEBUG,"request type <mkdir()>\n");
      status = srmkdir(s,from_host,is_remote) ;
      log(LOG_DEBUG,"rmkdir returned %d\n", status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_RMDIR :
      log(LOG_DEBUG,"request type <rmdir()>\n");
      status = srrmdir(s,from_host,is_remote) ;
      log(LOG_DEBUG,"rrmdir returned %d\n", status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_CHMOD:
      log(LOG_DEBUG,"request type <chmod()>\n");
      status = srchmod(s,from_host,is_remote) ;
      log(LOG_DEBUG,"rchmod returned %d\n", status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_CHOWN:
      log(LOG_DEBUG,"request type <chown()>\n");
      status = srchown(s,from_host,is_remote) ;
      log(LOG_DEBUG,"rchown returned %d\n", status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_RENAME:
      log(LOG_DEBUG,"request type <rename()>\n");
      status = srrename(s,from_host,is_remote) ;
      log(LOG_DEBUG,"rrename returned %d\n", status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_LOCKF:
      log(LOG_DEBUG,"request type <lockf()>\n");
      status = srlockf(s,fd) ;
      log(LOG_DEBUG,"rlockf returned %d\n", status);
      break;
    case RQST_LOCKF64:
      log(LOG_DEBUG,"request type <lockf64()>\n");
      status = srlockf64(s, &info, fd) ;
      log(LOG_DEBUG,"rlockf64 returned %d\n", status);
      break;
    case RQST_END :
      log(LOG_DEBUG,"request type : end rfiod\n") ;
#if defined(SACCT)
      rfioacct(RQST_END,0,0,s,0,0,status,errno,&info,NULL,NULL);
#endif /* SACCT */
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break ;
    case RQST_OPEN_V3:
      log(LOG_DEBUG,"request type : open_v3\n");
      fd = sropen_v3(s,(bet?is_remote:0),(bet?from_host:(char *)NULL), bet);
      log(LOG_DEBUG,"ropen_v3 returned %d\n",fd);
      break;
    case RQST_CLOSE_V3: /* Should not be received here but inside read_v3 and write_v3 functions */
      log(LOG_DEBUG,"request type : close_v3\n");
      status = srclose_v3(s,&info,fd);
      log(LOG_DEBUG,"rclose_v3 returned %d\n", status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_READ_V3:
      log(LOG_DEBUG,"request type : read_v3\n");
      status = srread_v3(s,&info,fd);
      log(LOG_DEBUG,"rread_v3 returned %d\n",status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_WRITE_V3:
      log(LOG_DEBUG,"request type : write_v3\n");
      status = srwrite_v3(s,&info,fd);
      log(LOG_DEBUG,"rwrite_v3 returned %d\n",status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_LSEEK_V3:
      info.seekop++;
      log(LOG_DEBUG, "request type lseek_v3()\n");
      status = srlseek_v3(s, &info, fd);
      log(LOG_DEBUG, "lseek_v3() returned %d\n",status);
      break;
    case RQST_OPEN64_V3:
      log(LOG_DEBUG,"request type : open64_v3\n");
      fd = sropen64_v3(s, is_remote, from_host);
      log(LOG_DEBUG,"ropen64_v3 returned %d\n",fd);
      break;
    case RQST_CLOSE64_V3: /* Should not be received here but inside read_v3 and write_v3 functions */
      log(LOG_DEBUG,"request type : close64_v3\n");
      status = srclose64_v3(s,&info,fd);
      log(LOG_DEBUG,"rclose64_v3 returned %d\n", status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_READ64_V3:
      log(LOG_DEBUG,"request type : read64_v3\n");
      status = srread64_v3(s,&info,fd);
      log(LOG_DEBUG,"rread64_v3 returned %d\n",status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_WRITE64_V3:
      log(LOG_DEBUG,"request type : write64_v3\n");
      status = srwrite64_v3(s,&info,fd);
      log(LOG_DEBUG,"rwrite64_v3 returned %d\n",status);
      fd = -1;
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    case RQST_XYOPEN  :
      log(LOG_DEBUG, "request type <xyopen()>\n");
      status = srxyopen(s, &lun, &access,(bet?is_remote:0),(bet?from_host:NULL),bet);
      log(LOG_DEBUG, "xyopen(%d,%d) returned: %d\n",lun,access,status);
      break;
    case RQST_XYCLOS  :
      log(LOG_DEBUG, "request type <xyclos(%d)>\n",lun);
      status = srxyclos(s, &info, lun);
      log(LOG_DEBUG,"xyclos() returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
    case RQST_XYREAD  :
      info.readop ++ ;
      log(LOG_DEBUG, "request type <xyread()>\n");
      status = srxyread(s, &info, lun, access);
      log(LOG_DEBUG, "xyread() returned: %d\n",status);
      break;
    case RQST_XYWRIT  :
      info.writop ++ ;
      log(LOG_DEBUG, "request type <xywrit(%d, %d)>\n", lun, access);
      status = srxywrit(s, &info, lun, access);
      log(LOG_DEBUG, "xywrit() returned: %d\n",status);
      break;
    case RQST_MSTAT64:
    case RQST_STAT64 :
      log(LOG_DEBUG, "request type <stat64()>\n");
      status = srstat64(s, is_remote, from_host);
      log(LOG_DEBUG, "stat64() returned %d\n",status);
      if (request == RQST_STAT64) {
        shutdown(s, 2); close(s);
        if(mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      }  /* if request == RQST_STAT64  */
      break ;
    case RQST_LSTAT64 :
      log(LOG_DEBUG, "request type <lstat64()>\n");
      status = srlstat64(s, is_remote, from_host);
      log(LOG_DEBUG, "lstat64() returned %d\n",status);
      shutdown(s,2); close(s);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    default :
      log(LOG_ERR, "unknown request type %x(hex)\n", request);
      if (mode) return(0); else exit(((subrequest_id > 0) && (forced_mover_exit_error != 0)) ? 1 : 0);
      break;
    }  /* End of switch (request) */
  }  /* End of for (;;) */
}

void check_child_exit(int block)
{
  int child_pid;
  int term_status;

  if ( block == 1 ) {
    child_pid = wait(&term_status);
    exit_code_from_last_child = WEXITSTATUS(term_status);
    if (WIFEXITED(term_status)) {
      log(LOG_ERR,"Waiting for end of child %d, status %d\n", child_pid, exit_code_from_last_child);
    } else if (WIFSIGNALED(term_status)) {
      log(LOG_ERR,"Waiting for end of child %d, uncaught signal %d\n", child_pid, WTERMSIG(term_status));
    } else {
      log(LOG_ERR,"Waiting for end of child %d, stopped\n", child_pid);
    }
  } else {
    while ((child_pid = waitpid(-1, &term_status, WNOHANG)) > 0) {
      exit_code_from_last_child = WEXITSTATUS(term_status);
      if (WIFEXITED(term_status)) {
        log(LOG_ERR,"Waiting for end of child %d, status %d\n", child_pid, exit_code_from_last_child);
      } else if (WIFSIGNALED(term_status)) {
        log(LOG_ERR,"Waiting for end of child %d, uncaught signal %d\n", child_pid, WTERMSIG(term_status));
      } else {
        log(LOG_ERR,"Waiting for end of child %d, stopped\n", child_pid);
      }
    }
  }
  return;
}
