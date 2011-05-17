/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* socket.c     Generalized network interface                           */

#define READ(x,y,z)     recv(x,y,z,0)   /* Actual read system call      */
#define WRITE(x,y,z)    send(x,y,z,0)   /* Actual write system  call    */
#define CLOSE(x)        close(x)        /* Actual close system call     */
#define IOCTL(x,y,z)    ioctl(x,y,z)    /* Actual ioctl system call     */

#ifndef READTIMEOUTVALUE
#define READTIMEOUTVALUE     60         /* Default read time out        */
#endif /* READTIMEOUTVALUE */

static int      rtimeout=READTIMEOUTVALUE;
static int timeout_set=0;

/*
 * Define BLOCKSIZE if read/write calls have a length upper limit
 * E.g. VMS QIO calls are limited to transfer 65536 bytes.
 */

#ifdef BLOCKSIZE
#undef BLOCKSIZE        /* be safe      */
#endif /* BLOCKSIZE */
 
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <errno.h>
#include <setjmp.h>
#include <signal.h>
#include <net.h>                        /* networking specifics         */
#include <serrno.h>                     /* special errors               */

#ifdef READTIMEOUT
static jmp_buf alarmbuf;
static void     (* defsigalrm) ();
#endif

#ifndef min
#define min(x, y)       (((x-y) > 0) ? y : x)
#endif /* min */

#ifdef READTIMEOUT
void    catch()
{
    longjmp(alarmbuf, 1);
}
#endif /* READTIMEOUT */
 
static int                      /* non atomic receive with time out     */
t_recv (s, buf, nbytes)
SOCKET s;
char    *buf;
int    nbytes;
{
    fd_set  fds;
    struct  timeval timeout;

    FD_ZERO (&fds);
    FD_SET  (s, &fds);
    timeout.tv_sec = rtimeout;
    timeout.tv_usec = 0;

    switch(select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout)) {
    case -1:
        return (-1);        /* an error has occured */
    case 0:
        serrno = SETIMEDOUT; return(-1);
    default: break;
    }

#ifdef BLOCKSIZE
    return( READ(s, buf, min(BLOCKSIZE, nbytes)));
#else
    return( READ(s, buf, nbytes));
#endif
}

int
s_recv (SOCKET s,
            char    *buf,
            int     nbytes)
{
    register int    n, nb;
 
    if (nbytes < 0) {
      serrno = EINVAL;
      return(-1);
    }

#ifdef READTIMEOUT
    if (setjmp(alarmbuf) == 1)      {
        signal(SIGALRM, defsigalrm);    /* restore alarm handler*/
        errno = ETIMEDOUT;
        return(-1);
    }

    defsigalrm = signal (SIGALRM, (void (*)()) catch);
#endif /* READTIMEOUT */
    nb = nbytes;

    for (; nb >0;)       {
#ifdef READTIMEOUT
        alarm(rtimeout);/* successive calls reset the alarm     */
#endif /* READTIMEOUT */
#ifdef BLOCKSIZE
        if (timeout_set)
            n = t_recv(s, buf, min(BLOCKSIZE, nb));
        else
            n = READ(s, buf, min(BLOCKSIZE, nb));
#else
        if (timeout_set)
            n = t_recv(s, buf, nb);
        else
            n = READ(s, buf, nb);
#endif
        serrno = errno;
        nb -= n;

#ifdef READTIMEOUT
        alarm(0);
        signal(SIGALRM, defsigalrm);
#endif /* READTIMEOUT */
        if (n <= 0) {
            if (n == 0) {
                serrno=SECONNDROP;
                return(0);
            }
            return (n);
        }
        buf += n;
    }
    return (nbytes);
}
 
int
s_send (SOCKET  s,
            char    *buf,
            int     nbytes)
{
    register int    n, nb;
 
    if (nbytes < 0) {
      serrno = EINVAL;
      return(-1);
    }

    nb = nbytes;

    for (; nb >0;)       {
#ifdef BLOCKSIZE
        n = WRITE(s, buf, min(BLOCKSIZE, nb));
#else
        n = WRITE(s, buf, nb);
#endif
        nb -= n;
        if (n <= 0) {
            if (n == 0) {
                serrno=SECONNDROP;
                return(0);
            }
            return (n);
        }
        buf += n;
    }
    return (nbytes);
}

int
s_close(SOCKET     s)
{
    return(CLOSE(s));
}

char *
s_errmsg()                              /* return last error message    */
{
    if ( serrno != 0 ) return((char *)sstrerror(serrno));
    else return((char *)sstrerror(errno));
}

/*
 * Solaris 2.x defines but does not document a s_ioctl routine
 * in libsocket.a, therefore conflicting with ours. Hence this
 * workaround.
 */

int s_ioctl(SOCKET s,
            int     request,
            int     arg)                 /* issue an ioctl(2) call       */
{
    return(IOCTL(s, request, arg));
}

int s_nrecv(SOCKET s,
            char    *buf,
            int    nbytes)     /* Non blocking read           */
{
#if defined(BLOCKSIZE)
    return(READ(s, buf, min(BLOCKSIZE,nbytes)));
#else
    return(READ(s, buf, nbytes));
#endif /* BLOCKSIZE */
}

int setrtimo(int     val)
{

    register int    otimeout;

    otimeout = rtimeout;
    rtimeout=val;
    timeout_set = 1;
    return(otimeout);
}

int (*recvfunc)()=s_recv;               /* recv function to use         */
int (*sendfunc)()=s_send;               /* send function to use         */
int (*closefunc)()=s_close;             /* close function to use        */
int (*ioctlfunc)()=s_ioctl;             /* ioctl function to use        */
char *(*errfunc)()=s_errmsg;           /* strerror function to use     */
