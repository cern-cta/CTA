/*
 * $Id: socket_timeout.c,v 1.3 1999/07/21 13:59:23 jdurand Exp $
 *
 * $Log: socket_timeout.c,v $
 * Revision 1.3  1999/07/21 13:59:23  jdurand
 * For old AIX systems, fd_set is now known by including <sys/time.h>. So I put
 * it as an additional flag in Imakefile, used in Cpool.c and socket_timeout.c
 *
 * Revision 1.2  1999/07/20 08:49:22  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Added missing Id and Log CVS's directives
 *
 */

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
#include <unistd.h>
#include "net.h"
#include "serrno.h"
#include "socket_timeout.h"

/* Signal handler - Simplify the POSIX sigaction calls */
#if defined(__STDC__)
typedef void    Sigfunc(int);
#else
typedef void    Sigfunc();
#endif

#if defined(__STDC__)
int      _net_readable(int, int);
int      _net_writable(int, int);
Sigfunc *_netsignal(int, Sigfunc *);
#else
int      _net_readable();
int      _net_writable();
Sigfunc *_netsignal();
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
  Sigfunc *sigpipe;            /* Old SIGPIPE handler */

  /* If not timeout, we go to normal function */
  if (timeout <= 0) {
    return(netread(fd,vptr,n));
  }

  /* In any case we catch and trap SIGPIPE */
  if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
    return(-1);
  }

  ptr   = vptr;
  nleft = n;
  nread = 0;

  while (nleft > 0) {
    if ((select_status = _net_readable(fd, timeout)) <= 0) {
      if (select_status == 0) {
        /* Timeout */
        errno = ETIMEDOUT;
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
    }
    nleft -= nread;
    ptr   += nread;
  }
  /* Restore previous handlers */
  _netsignal(SIGPIPE, sigpipe);

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
  Sigfunc *sigpipe;            /* Old SIGPIPE handler */
  int     select_status = 0;

  /* If not timeout, we go to normal function */
  if (timeout <= 0) {
    return(netwrite(fd,vptr,n));
  }

  /* In any case we catch and trap SIGPIPE */
  if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
    return(-1);
  }

  ptr      = vptr;
  nleft    = n;

  while (nleft > 0) {
    if ((select_status = _net_writable(fd, timeout)) <= 0) {
      if (select_status == 0) {
        /* Timeout */
        errno = ETIMEDOUT;
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
    }
    nleft -= nwritten;
    ptr   += nwritten;
  }
  /* Restore previous handlers */
  _netsignal(SIGPIPE, sigpipe);

  if (flag != 0) {
    /* Return -1 if error */
    return(-1);
  }
  /* Return the number of bytes writen ( >= 0) */
  return(n - nleft);
}

Sigfunc *_netsignal(signo, func)
     int signo;
     Sigfunc *func;
{
  struct sigaction	act, oact;
  
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
  if (sigaction(signo, &act, &oact) < 0)
    return(SIG_ERR);
  return(oact.sa_handler);
}

void _netalarm(signo)
     int signo;
{
  /* We just trap the signal and return */
  return;
}

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
