/*
 * $Id: send2stgd_api.c,v 1.1 2000/01/26 08:37:18 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2stgd_api.c,v $ $Revision: 1.1 $ $Date: 2000/01/26 08:37:18 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#if !defined(_WIN32)
#include <sys/stat.h>
#include <signal.h>
#include <sys/wait.h>
#endif
#include "marshall.h"
#include "rfio.h"
#include "net.h"
#include "serrno.h"
#include "osdep.h"
#include "stage.h"
extern int rfio_errno;
int nb_ovl;
#if (defined(_AIX) && defined(_IBMR2)) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
struct sigaction sa;
#endif

int dosymlink _PROTO((char *, char *));
void dounlink _PROTO((char *));
#ifndef _WIN32
void wait4child _PROTO(());
#endif

int DLL_DECL send2stgd(host, reqp, reql, want_reply, user_repbuf, user_repbuf_len)
     char *host;
     char *reqp;
     int reql;
     int want_reply;
     char *user_repbuf;
     int user_repbuf_len;
{
  int actual_replen = 0;
  int c;
  char file2[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];
  char func[16];
  char *getconfent();
  char *getenv();
  struct hostent *hp;
  int link_rc;
  int magic;
  int n;
  char *p;
  char prtbuf[PRTBUFSZ];
  int rep_type;
  char repbuf[REPBUFSZ];
  struct sockaddr_in sin; /* internet socket */
  struct servent *sp;
  int stg_s;
  char stghost[CA_MAXHOSTNAMELEN+1];
#if !defined(_WIN32)
  void wait4child();
#endif

  strcpy (func, "send2stgd");
#if (defined(sgi) && !defined(IRIX5)) || defined(hpux)
  signal (SIGCLD, wait4child);
#else
#if (defined(_AIX) && defined(_IBMR2)) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
  sa.sa_handler = wait4child;
  sa.sa_flags = SA_RESTART;
  sigaction (SIGCHLD, &sa, NULL);
#endif
#endif
  link_rc = 0;
  nb_ovl = 0;
  if ((sp = getservbyname (STG, "tcp")) == NULL) {
    stage_errmsg (func, STG09, STG, "not defined in /etc/services");
    return (CONFERR);
  }
  if (host == NULL) {
    if ((p = getenv ("STAGE_HOST")) == NULL &&
        (p = getconfent("STG", "HOST",0)) == NULL) {
      stage_errmsg (func, STG31);
      return (CONFERR);
    }
    strcpy (stghost, p);
  } else {
    strcpy (stghost, host);
  }
  if ((hp = gethostbyname (stghost)) == NULL) {
    stage_errmsg (func, STG09, "Host unknown:", stghost);
    return (CONFERR);
  }
  sin.sin_family = AF_INET;
  sin.sin_port = sp->s_port;
  sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

  if ((stg_s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
    stage_errmsg (func, STG02, "", "socket", neterror());
    return (SYERR);
  }

  c = RFIO_NONET;
  rfiosetopt (RFIO_NETOPT, &c, 4);

  if (connect (stg_s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    if (
#if defined(_WIN32)
        WSAGetLastError() == WSAECONNREFUSED
#else
        errno == ECONNREFUSED
#endif
        ) {
      stage_errmsg (func, STG00, stghost);
      (void) netclose (stg_s);
      return (ESTNACT);
    } else {
      stage_errmsg (func, STG02, "", "connect", neterror());
      (void) netclose (stg_s);
      return (SYERR);
    }
  }
  if ((n = netwrite_timeout (stg_s, reqp, reql, STGTIMEOUT)) != reql) {
    if (n == 0)
      stage_errmsg (func, STG02, "", "send", sys_serrlist[SERRNO]);
    else
      stage_errmsg (func, STG02, "", "send", neterror());
    (void) netclose (stg_s);
    return (SYERR);
  }
  if (! want_reply) {
    (void) netclose (stg_s);
    return (0);
  }
  
  while (1) {
    if ((n = netread(stg_s, repbuf, 3 * LONGSIZE)) != (3 * LONGSIZE)) {
      if (n == 0)
        stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
      else
        stage_errmsg (func, STG02, "", "recv", neterror());
      (void) netclose (stg_s);
      return (SYERR);
    }
    p = repbuf;
    unmarshall_LONG (p, magic) ;
    unmarshall_LONG (p, rep_type) ;
    unmarshall_LONG (p, c) ;
    if (rep_type == STAGERC) {
      (void) netclose (stg_s);
      break;
    }
    if ((n = netread(stg_s, repbuf, c)) != c) {
      if (n == 0)
        stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
      else
        stage_errmsg (func, STG02, "", "recv", neterror());
      (void) netclose (stg_s);
      c = SYERR;
      break;
    }
    p = repbuf;
    unmarshall_STRING (p, prtbuf);
    switch (rep_type) {
    case MSG_OUT:
      if (user_repbuf) {
        if (actual_replen + c <= user_repbuf_len)
          n = c;
        else
          n = user_repbuf_len - actual_replen;
        if (n) {
          memcpy (user_repbuf + actual_replen, repbuf, n);
          actual_replen += n;
        }
      } else
        stage_outmsg ("%s", prtbuf);
      break;
    case MSG_ERR:
    case RTCOPY_OUT:
      stage_errmsg (func, "%s", prtbuf);
      break;
    case SYMLINK:
      unmarshall_STRING (p, file2);
      if (c = dosymlink (prtbuf, file2))
        link_rc = c;
      break;
    case RMSYMLINK:
      dounlink (prtbuf);
    }
  }
#if !defined(_WIN32)
  while (nb_ovl > 0) sleep (1);
#endif
  return (c ? c : link_rc);
}

int dosymlink (file1, file2)
     char *file1;
     char *file2;
{
  char *filename;
  char func[16];
  char *host;
  int remote;
  
  strcpy (func, "send2stgd");
  remote = rfio_parseln (file2, &host, &filename, NORDLINKS);
  serrno = 0;
  if (rfio_symlink (file1, file2) &&
      ((!remote && errno != EEXIST) || (remote && rfio_errno != EEXIST))) {
    stage_errmsg (func, STG02, file1, "symlink", rfio_serror());
    if (serrno == SEOPNOTSUP) return (LNKNSUP);
    if ((remote &&
         (rfio_errno == EACCES || rfio_errno == ENOENT)) ||
        (remote == 0 && (errno == EACCES || errno == ENOENT)))
      return (USERR);
    else
      return (SYERR);
  }
  return (0);
}

void
dounlink (path)
     char *path;
{
  char *filename;
  char func[16];
  char *host;
#if !defined(_WIN32)
  int pid;
  struct stat st;
#endif
  int remote;
  
  strcpy (func, "send2stgd");
  remote = rfio_parseln (path, &host, &filename, NORDLINKS);
  if (rfio_unlink (path)) {
    if ((remote && rfio_errno == ENOENT) ||
        (remote == 0 && errno == ENOENT)) return;
#if !defined(_WIN32)
    if (getuid() || (remote && rfio_errno != EACCES) ||
        (remote == 0 && errno != EACCES) ||
        strncmp (filename, "/afs/", 5) == 0) {
#endif
      stage_errmsg (func, STG02, path, "unlink", rfio_serror());
      return;
    }
#if !defined(_WIN32)
    if (rfio_lstat (path, &st) != 0) {
      stage_errmsg (func, STG02, path, "unlink(lstat)", rfio_serror());
      return;
    }
    pid = fork ();
    if (pid < 0) {
      stage_errmsg (func, STG02, path, "unlink(fork)", rfio_serror());
      return;
    } else if (pid == 0) {
      setgid (st.st_gid);
      setuid (st.st_uid);
      if (rfio_unlink (path)) {
        stage_errmsg (func, STG02, path, "unlink", rfio_serror());
        exit (SYERR);
      }
      exit (0);
    }
    nb_ovl++;
  }
}
#endif

#if !defined(_WIN32)
void wait4child()
{
  int pid;
  int status;
  
#if defined(_IBMR2) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
  while ((pid = waitpid (-1, &status, WNOHANG)) > 0)
    nb_ovl--;
#else
  pid = wait (&status);
  nb_ovl--;
  signal (SIGCLD, wait4child);
#endif
}
#endif
