/*
 * $Id: socket_timeout.c,v 1.8 1999/12/09 13:39:48 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: socket_timeout.c,v $ $Revision: 1.8 $ $Date: 1999/12/09 13:39:48 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#if _WIN32
#include <time.h>
#else
#include <sys/time.h>
#endif
#ifdef _AIX
/* Otherwise cc will not know about fd_set on */
/* old aix versions.                          */
#include <sys/select.h>
#endif
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "net.h"
#include "serrno.h"
#include "socket_timeout.h"

#ifndef _WIN32
/* Signal handler - Simplify the POSIX sigaction calls */
#if defined(__STDC__)
typedef void    Sigfunc(int);
#else
typedef void    Sigfunc();
#endif
#endif

#if defined(__STDC__)
int      _net_readable(int, int);
int      _net_writable(int, int);
#ifndef _WIN32
Sigfunc *_netsignal(int, Sigfunc *);
#endif
#else
int      _net_readable();
int      _net_writable();
#ifndef _WIN32
Sigfunc *_netsignal();
#endif
#endif

ssize_t netread_timeout(fd, vptr, n, timeout)
     int fd;
     void *vptr;
     size_t n;
     int timeout;
{
  size_t nleft;                /* Bytes to read */
  ssize_t nread = 0;           /* Bytes yet read */
  ssize_t flag = 0;            /* != 0 means error */
  char  *ptr   = NULL;         /* Temp. pointer */
  int     select_status = 0;
#ifndef _WIN32
  Sigfunc *sigpipe;            /* Old SIGPIPE handler */
#endif

  /* If not timeout, we go to normal function */
  if (timeout <= 0) {
    return(netread(fd,vptr,n));
  }

#ifndef _WIN32
  /* In any case we catch and trap SIGPIPE */
  if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
    return(-1);
  }
#endif

  ptr   = vptr;
  nleft = n;
  nread = 0;

  while (nleft > 0) {
    if ((select_status = _net_readable(fd, timeout)) <= 0) {
      if (select_status == 0) {
        /* Timeout */
        serrno = SETIMEDOUT;
      }
      flag = -1;
      break;
    }
    if ((nread = recv(fd, ptr, nleft, 0)) < 0) {
      if (errno != EINTR) {
        /* Error */
        flag = -1;
        break;
      }
      /* We continue the loop (recv was just interrupted) */
      nread = 0;
    } else if (nread == 0) {
      /* Should not happen */
      /* In such a case netread() returns 0, so do we */
      serrno = SECONNDROP;
      /* Makes sure we will return zero */
      nleft = n;
      /* EOF */
      break;
    } else {
      nleft -= nread;
      ptr   += nread;
    }
  }

#ifndef _WIN32
  /* Restore previous handlers */
  _netsignal(SIGPIPE, sigpipe);
#endif

  if (flag != 0) {
    /* Return -1 if error */
    return(-1);
  }
  /* Return the number of bytes read ( >= 0) */
  return(n - nleft);
}

ssize_t netwrite_timeout(fd, vptr, n, timeout)
     int fd;
     void *vptr;
     size_t n;
     int timeout;
{
  size_t nleft;                /* Bytes to read */
  ssize_t nwritten = 0;        /* Bytes yet read */
  ssize_t flag = 0;            /* != 0 means error */
  char  *ptr   = NULL;         /* Temp. pointer */
#ifndef _WIN32
  Sigfunc *sigpipe;            /* Old SIGPIPE handler */
#endif
  int     select_status = 0;

  /* If not timeout, we go to normal function */
  if (timeout <= 0) {
    return(netwrite(fd,vptr,n));
  }

#ifndef _WIN32
  /* In any case we catch and trap SIGPIPE */
  if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
    return(-1);
  }
#endif

  ptr      = vptr;
  nleft    = n;

  while (nleft > 0) {
    if ((select_status = _net_writable(fd, timeout)) <= 0) {
      if (select_status == 0) {
        /* Timeout */
        serrno = SETIMEDOUT;
      }
      flag = -1;
      break;
    }
    if ((nwritten = send(fd, ptr, nleft, 0)) <= 0) {
      if (errno != EINTR) {
        /* Error */
        flag = -1;
        break;
      }
      /* We continue the loop (send was just interrupted) */
      nwritten = 0;
    } else {
      nleft -= nwritten;
      ptr   += nwritten;
    }
  }

#ifndef _WIN32
  /* Restore previous handlers */
  _netsignal(SIGPIPE, sigpipe);
#endif

  if (flag != 0) {
    /* Return -1 if error */
    return(-1);
  }
  /* Return the number of bytes writen ( >= 0) */
  return(n - nleft);
}

#ifndef _WIN32
Sigfunc *_netsignal(signo, func)
     int signo;
     Sigfunc *func;
{
  struct sigaction	act, oact;
  int n = 0;

  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
  } else {
#ifdef	SA_RESTART
    act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
  }
#ifdef __INSURE__
  /* Insure don't like the value I give to sigaction... */
  _Insure_set_option("runtime","off");
#endif
  n = sigaction(signo, &act, &oact);
#ifdef __INSURE__
  /* Restore runtime checking */
  _Insure_set_option("runtime","on");
#endif
  if (n < 0) {
    return(SIG_ERR);
  }
  return(oact.sa_handler);
}
#endif

int _net_writable(fd, timeout)
     int fd;
     int timeout;
{
  fd_set         rset;
  struct timeval tv;
  
  FD_ZERO(&rset);
  FD_SET(fd,&rset);
  
  tv.tv_sec = timeout;
  tv.tv_usec = 0;
  
  /* Will return > 0 if the descriptor is writable */
  return(select(fd + 1, NULL, &rset, NULL, &tv));
}

int _net_readable(fd, timeout)
     int fd;
     int timeout;
{
  fd_set         rset;
  struct timeval tv;
  
  FD_ZERO(&rset);
  FD_SET(fd,&rset);
  
  tv.tv_sec = timeout;
  tv.tv_usec = 0;
  
  /* Will return > 0 if the descriptor is readable */
  return(select(fd + 1, &rset, NULL, NULL, &tv));
}
