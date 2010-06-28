/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* socket.c     Generalized network interface                           */


#undef DEBUG
/* Define DUMP to print buffers contents - heavy debug mode             */

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
#if defined(DEBUG) || defined(DUMP)
#include <log.h>                        /* logging functions            */
#endif /* DEBUG || DUMP */
#include <serrno.h>                     /* special errors               */

#ifdef READTIMEOUT
static jmp_buf alarmbuf;
static void     (* defsigalrm) ();
#endif

#ifndef min
#define min(x, y)       (((x-y) > 0) ? y : x)
#endif /* min */

 
#ifdef DUMP

#include <ctype.h>

static  int
Dump(buf, nbytes)
char    *buf;
int     nbytes;
{
    register int    i, j;
    register char   c;

    log(LOG_DEBUG ," *** Hexadecimal dump *** \n");

    for (i=0;i<nbytes/20;i++)       {
        for (j=0;j<20;j++)      {
            log(LOG_DEBUG ,"<%2.2X>", (char) buf[i*20+j]);
        }
        log(LOG_DEBUG ,"\n");
    }
    for (i=0;i<nbytes%20;i++)       {
        log(LOG_DEBUG ,"<%2.2X>",buf[(nbytes/20)*20+i]);
    }
    log(LOG_DEBUG ,"\n *** Interpreted dump *** \n");
    for (i=0;i<nbytes/80;i++)       {
        for (j=0;j<80;j++)      {
            c = (char) buf[i*80+j];
            if (isprint(c)) log(LOG_DEBUG ,"%c", c);
            else log(LOG_DEBUG ,".");
        }
        log(LOG_DEBUG ,"\n");
    }
    for (i=0;i<nbytes%80;i++)       {
        c = (char) buf[(nbytes/80)*80+i];
        if (isprint(c)) log(LOG_DEBUG ,"%c", c);
        else log(LOG_DEBUG ,".");
    }
    log(LOG_DEBUG ,"\n");
}
#endif /* DUMP */

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

#if defined(DEBUG)
    fprintf(stdout,"select(%d, %x, %x, %x, %d.%d)\n",
        FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,timeout.tv_sec, timeout.tv_usec);
#endif /* DEBUG */
    switch(select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout)) {
    case -1:
#if defined(DEBUG)
        fprintf(stdout,"select returned -1\n");
#endif /* DEBUG */
        return (-1);        /* an error has occured */
    case 0:
#if defined(DEBUG)
        fprintf(stdout,"select timed out\n");
        syslog(LOG_ALERT, "[%d]: socket: network recv timed out", getpid());
#endif /* DEBUG */
        serrno = SETIMEDOUT; return(-1);
    default: break;
    }

#if defined(DEBUG)
    fprintf(stdout,"select returned data\n");
#endif /* DEBUG */
#ifdef BLOCKSIZE
    return( READ(s, buf, min(BLOCKSIZE, nbytes)));
#else
    return( READ(s, buf, nbytes));
#endif
}

int DLL_DECL
s_recv (s, buf, nbytes)
SOCKET s;
char    *buf;
int     nbytes;
{
    register int    n, nb;
 
    if (nbytes < 0) {
      serrno = EINVAL;
      return(-1);
    }

#if defined(DEBUG)
    log(LOG_DEBUG ,"dorecv(%x, %x, %d)\n", s, buf, nbytes);
#endif /* DEBUG */

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
#if defined(DEBUG)
            log(LOG_DEBUG ,"ERROR: %d while n=%d,nb-n=%d,buf=%x\n",
                errno, n, nb, buf);
#endif /* DEBUG */
            return (n);
        }
#if defined(DEBUG)
        log(LOG_DEBUG ,"dorecv: %d bytes received\n",n);
#if defined(DUMP)
        log(LOG_DEBUG ,"dorecv: dump follows\n");
        Dump(buf,n);
#endif /* DUMP */
#endif /* DEBUG */
        buf += n;
    }
    return (nbytes);
}
 
int DLL_DECL
s_send (s, buf, nbytes)
SOCKET  s;
char    *buf;
int     nbytes;
{
    register int    n, nb;
 
    if (nbytes < 0) {
      serrno = EINVAL;
      return(-1);
    }

#if defined(DEBUG)
    log(LOG_DEBUG, "dosend(%x, %x, %d)\n", s, buf, nbytes);
#endif
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
#if defined(DEBUG)
            log(LOG_DEBUG ,"ERROR: %d while n=%d,nb-n=%d,buf=%x\n",
                errno, n, nb, buf);
#endif /* DEBUG */
            return (n);
        }
#if defined(DEBUG)
#if defined(DUMP)
        log(LOG_DEBUG ,"dosend: dump follows\n");
        Dump(buf,n);
#endif /* DUMP */
        log(LOG_DEBUG ,"dosend: %d bytes sent\n",n);
#endif /* DEBUG */
        buf += n;
    }
#if defined(DEBUG)
    log(LOG_DEBUG, "dosend(%x) returns %d\n", s, nbytes);
#endif /* DEBUG */
    return (nbytes);
}

int DLL_DECL
s_close(s)
SOCKET     s;
{
    return(CLOSE(s));
}

char DLL_DECL *
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

int DLL_DECL 
s_ioctl(s, request, arg)                /* issue an ioctl(2) call       */
SOCKET s;
int     request;
int     arg;
{
    return(IOCTL(s, request, arg));
}

int DLL_DECL s_nrecv(s, buf, nbytes)     /* Non blocking read           */
SOCKET s;
char    *buf;
int    nbytes;
{
#if defined(BLOCKSIZE)
    return(READ(s, buf, min(BLOCKSIZE,nbytes)));
#else
    return(READ(s, buf, nbytes));
#endif /* BLOCKSIZE */
}

int DLL_DECL setrtimo(val)
int     val;
{

    register int    otimeout;

    otimeout = rtimeout;
    rtimeout=val;
#if defined(DEBUG)
    fprintf(stdout,"setrtimo: switching to time'dout recv\n");
#endif /* DEBUG */
    timeout_set = 1;
    return(otimeout);
}

int (*recvfunc)()=s_recv;               /* recv function to use         */
int (*sendfunc)()=s_send;               /* send function to use         */
int (*closefunc)()=s_close;             /* close function to use        */
int (*ioctlfunc)()=s_ioctl;             /* ioctl function to use        */
char *(*errfunc)()=s_errmsg;           /* strerror function to use     */
