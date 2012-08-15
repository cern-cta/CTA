/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#if defined(linux)
#define USE_POLL_INSTEAD_OF_SELECT
/* #define __FD_SETSIZE 65536 */
#endif

#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>

#ifdef USE_POLL_INSTEAD_OF_SELECT
#include <poll.h>
#endif

#include "net.h"
#include "serrno.h"
#include "string.h"
#include "socket_timeout.h"

/* Signal handler - Simplify the POSIX sigaction calls */
typedef void    Sigfunc(int);

int      _net_readable(int, int);
int      _net_writable(int, int);
int      _net_connectable(SOCKET, int);
int      _net_isclosed(int);
Sigfunc *_netsignal(int, Sigfunc *);

int netconnect_timeout(SOCKET fd,
                       struct sockaddr *addr,
                       size_t addr_size,
                       int timeout)
{
	int rc, nonblocking;
	Sigfunc *sigpipe;            /* Old SIGPIPE handler */

	/* In any case we catch and trap SIGPIPE */
	if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
		return(-1);
	}

	rc = 0;
	if ( timeout >= 0 )  {
		nonblocking = 1;
		rc = ioctl(fd,FIONBIO,&nonblocking);
		if ( rc == SOCKET_ERROR ) {
			serrno = 0;
		} 
	} else {
		nonblocking = 0;
	}

	if ( rc != -1 ) {
		rc = connect(fd,addr,addr_size);
		if ( timeout >= 0 ) {
			if ( rc == -1 && errno != EINPROGRESS ) {
				serrno = 0;
			} else rc = 0;
		} /* timeout >= 0 */
	}

	if ( timeout >= 0 && rc != -1 ) {
		time_t time_start = time(NULL);
		int time_elapsed = 0;
		for (;;) {
			rc = _net_connectable(fd, timeout - time_elapsed);
			if ( (rc == -1) && (errno == EINTR) ) {
				time_elapsed = (int) (time(NULL) - time_start);
				if (time_elapsed >= timeout) {
					rc = -1;
					serrno = SETIMEDOUT;
					break;
				}
				continue;
			} else {
				break;
			}
		}
	}

	/* Restore previous handlers */
	{
		int save_errno = errno;
		_netsignal(SIGPIPE, sigpipe);
		errno = save_errno;
	}
	/* Restore blocking socket if connect was successful */
	if ( timeout >= 0 && rc == 0 ) {
		nonblocking = 0;
		rc = ioctl(fd,FIONBIO,&nonblocking);
		if ( rc == SOCKET_ERROR ) serrno = 0;
	}

	return(rc);
}

ssize_t netread_timeout(SOCKET fd,
                        void *vptr,
                        ssize_t n,
                        int timeout)
{
	size_t nleft;                /* Bytes to read */
	ssize_t nread = 0;           /* Bytes yet read */
	ssize_t flag = 0;            /* != 0 means error */
	char  *ptr = NULL;           /* Temp. pointer */
	int select_status = 0;
	Sigfunc *sigpipe;            /* Old SIGPIPE handler */
	time_t time_start;
	int time_elapsed;

	/* If n < 0 it is an app. error */
	if (n < 0) {
		serrno = EINVAL;
		return(-1);
	}

	/* If not timeout, we go to normal function */
	if (timeout <= 0) {
		return(netread(fd,vptr,(int) n));
	}

	/* In any case we catch and trap SIGPIPE */
	if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
		return(-1);
	}

	ptr   = vptr;
	nleft = n;
	nread = 0;

	time_start = time(NULL);
	time_elapsed = 0;

	while (nleft > 0) {
		if ((select_status = _net_readable(fd, timeout - time_elapsed)) <= 0) {
			if (select_status == 0) {
				/* Timeout */
				serrno = SETIMEDOUT;
				flag = -1;
				break;
			} else if (errno == EINTR) {
				/* Interupted */
				continue;
			} else {
				flag = -1;
				break;
			}
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
		if (nleft > 0) {
			time_elapsed = (int) (time(NULL) - time_start);
			if (time_elapsed >= timeout) {
				/* Timeout */
				serrno = SETIMEDOUT;
				flag = -1;
				break;
			}
		}
	}

	/* Restore previous handlers */
	{
		int save_errno = errno;
		_netsignal(SIGPIPE, sigpipe);
		errno = save_errno;
	}

	if (flag != 0) {
		/* Return -1 if error */
		return(-1);
	}
	/* Return the number of bytes read ( >= 0) */
	return(n - nleft);
}

ssize_t netwrite_timeout(SOCKET fd,
                         void *vptr,
                         ssize_t n,
                         int timeout)
{
	size_t nleft;                /* Bytes to read */
	ssize_t nwritten = 0;        /* Bytes yet read */
	ssize_t flag = 0;            /* != 0 means error */
	char  *ptr   = NULL;         /* Temp. pointer */
	Sigfunc *sigpipe;            /* Old SIGPIPE handler */
	int select_status = 0;
	time_t time_start;
	int time_elapsed;

	/* If n < 0 it is an app. error */
	if (n < 0) {
		serrno = EINVAL;
		return(-1);
	}

	/* If not timeout, we go to normal function */
	if (timeout <= 0) {
		return(netwrite(fd,vptr,(int) n));
	}

	/* In any case we catch and trap SIGPIPE */
	if ((sigpipe = _netsignal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
		return(-1);
	}

	ptr      = vptr;
	nleft    = n;

	time_start = time(NULL);
	time_elapsed = 0;

	while (nleft > 0) {
	  	/* Check if the client has closed its connection */
		if (_net_isclosed(fd)) {
			flag = -1;
			break;
		}
		if ((select_status = _net_writable(fd, timeout - time_elapsed)) <= 0) {
			if (select_status == 0) {
				/* Timeout */
				serrno = SETIMEDOUT;
				flag = -1;
				break;
			} else if (errno == EINTR) {
				/* Interrupted */
				continue;
			} else {
				flag = -1;
				break;
			}
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
		if (nleft > 0) {
			time_elapsed = (int) (time(NULL) - time_start);
			if (time_elapsed >= timeout) {
				/* Timeout */
				serrno = SETIMEDOUT;
				flag = -1;
				break;
			}
		}
	}

	/* Restore previous handlers */
	{
		int save_errno = errno;
		_netsignal(SIGPIPE, sigpipe);
		errno = save_errno;
	}

	if (flag != 0) {
		/* Return -1 if error */
		return(-1);
	}
	/* Return the number of bytes writen ( >= 0) */
	return(n - nleft);
}

Sigfunc *_netsignal(int signo,
                    Sigfunc *func)
{
	struct sigaction	act, oact;
	int n = 0;

        memset(&act, 0, sizeof(act));
	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
		act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
	} else {
#ifdef	SA_RESTART
		act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
	}
	n = sigaction(signo, &act, &oact);
	if (n < 0) {
		return(SIG_ERR);
	}
	return(oact.sa_handler);
}

#ifdef USE_POLL_INSTEAD_OF_SELECT
int _net_isclosed(int fd)
{
	struct pollfd pollit;
	char buf[1];
	
	pollit.fd = fd;
	pollit.events = POLLIN;
	pollit.revents = 0;
	
	/* Will return > 0 if the descriptor is closed */
	if (poll(&pollit, 1, 0) > 0) {
		if (recv(fd, buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT) == 0) {
			serrno = SECONNDROP;
			return 1;
		}
	}
	return 0;
}
#else
int _net_isclosed(int fd)
{
	fd_set         rset;
	struct timeval tv;
	char           buf[1];
	
	FD_ZERO(&rset);
	FD_SET(fd,&rset);
	
	tv.tv_sec = 0;
	tv.tv_usec = 0;
	
	/* Will return > 0 if the descriptor is closed */
	if (select(fd + 1, NULL, &rset, NULL, &tv) > 0) {
		if (recv(fd, buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT) == 0) {
			serrno = errno;
			return 1;
		}
	}
	return 0;
}

#endif

#ifdef USE_POLL_INSTEAD_OF_SELECT
int _net_writable(int fd,
                  int timeout)
{
	struct pollfd pollit;
	
	pollit.fd = fd;
	pollit.events = POLLOUT;
	pollit.revents = 0;

	/* Will return > 0 if the descriptor is writable */
	return(poll(&pollit, 1, timeout * 1000));
}
#else
int _net_writable(int fd,
                  int timeout)
{
	fd_set         rset;
	struct timeval tv;
	
	FD_ZERO(&rset);
	FD_SET(fd,&rset);
	
	tv.tv_sec  = timeout;
	tv.tv_usec = 0;
	
	/* Will return > 0 if the descriptor is writable */
	return(select(fd + 1, NULL, &rset, NULL, &tv));
}
#endif

#ifdef USE_POLL_INSTEAD_OF_SELECT
int _net_readable(int fd,
                  int timeout)
{
	struct pollfd pollit;
	
	pollit.fd = fd;
	pollit.events = POLLIN;
	pollit.revents = 0;

	/* Will return > 0 if the descriptor is readable */
	return(poll(&pollit, 1, timeout * 1000));
}
#else
int _net_readable(int fd,
                  int timeout)
{
	fd_set         rset;
	struct timeval tv;
  
	FD_ZERO(&rset);
	FD_SET(fd,&rset);
  
	tv.tv_sec  = timeout;
	tv.tv_usec = 0;
  
	/* Will return > 0 if the descriptor is readable */
	return(select(fd + 1, &rset, NULL, NULL, &tv));
}
#endif

int _net_connectable(SOCKET fd,
                     int timeout)
{
#ifdef USE_POLL_INSTEAD_OF_SELECT
	struct pollfd pollit;
#else
	fd_set wset, eset;
	struct timeval tv;
#endif
	int rc, errval;
	socklen_t errval_len;

#ifdef USE_POLL_INSTEAD_OF_SELECT
	pollit.fd = fd;
	pollit.events = POLLOUT;
	pollit.revents = 0;
#else
	FD_ZERO(&wset);
	FD_SET(fd,&wset);
	FD_ZERO(&eset);
	FD_SET(fd,&eset);

	tv.tv_sec = timeout;
	tv.tv_usec = 0;
#endif

	/* Will return > 0 if the descriptor is connected */
#ifdef USE_POLL_INSTEAD_OF_SELECT
	rc = poll(&pollit, 1, timeout * 1000);
#else
	rc = select(fd + 1, NULL, &wset, &eset, &tv);
#endif
	/*
	 * Timeout ?
	 */
	if ( rc == 0 ) {
		serrno = SETIMEDOUT;
		return(-1);
	}

	/*
	 * Other error ?
	 */
	if ( rc < 0 ) {
		serrno = 0;
		return(-1);
	}

	/*
	 * Check if a socket error was reported. Connection errors
	 * are reported through socket layer errors since the connect is
	 * non-blocking. In this case, select() has returned a positive
	 * value indicating that a socket was ready for writing. Note that
	 * most systems seem to require that one always checks the socket 
	 * error for the connect() completion. Some systems (Windows) sets 
	 * the exception set to indicate that there was an error.
	 */
	errval_len = (socklen_t) sizeof(errval);
	if ( getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *)&errval, &errval_len) == -1 ) {
		serrno = 0;
		return(-1);
	}

#ifdef USE_POLL_INSTEAD_OF_SELECT
	if (errval == 0) return(0);
#else
	if ( (FD_ISSET(fd,&wset)) && (errval == 0) ) return(0);
	/*
	 * For Windows it may happen that the getsockopt returns zero
	 * error value despite that the write set has not been set.
	 * Flag that an error occurred with SECOMERR. On windows we must
	 * set a valid socket error since WSAGetLastError() might be called.
	 * A good guess value is connection refused.
	 */
	if ( errval == 0 ) {
		errval = SECOMERR;
	}
#endif
	serrno = errval;
	return(-1);
}
