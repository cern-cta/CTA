/*
 * $Id: net.h,v 1.6 2007/03/14 09:30:08 sponcec3 Exp $ 
 */

#ifndef _NET_H
#define _NET_H
/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */
#include <osdep.h>
/*
 * Define NT types and constants for UNIX system. All
 * CASTOR sources dealing with networking access should 
 * use these definitions to maximize portability.
 */
#ifndef SOCKET
#define SOCKET int
#endif
/*
 * The following definitions should be used with shutdown()
 */
#if defined(SHUT_RD)
#define SD_RECEIVE SHUT_RD
#else /* SHUT_RD */
#define SD_RECEIVE 0x00
#endif /* SHUT_RD */

#if defined(SHUT_WR)
#define SD_SEND SHUT_WR
#else /* SHUT_WR */
#define SD_SEND 0x01
#endif /* SHUT_WR */

#if defined(SHUT_RDWR)
#define SD_BOTH SHUT_RDWR
#else /* SHUT_RDWR */
#define SD_BOTH 0x02
#endif /* SHUT_RDWR */

/*
 * Invalid socket and socket error. INVALID_SOCKET should be use
 * for system calls returning a socket, e.g. socket(), accept().
 * SOCKET_ERROR should be used for all other network routines
 * returning an int, e.g. connect(), bind()
 */
#ifndef INVALID_SOCKET
#define INVALID_SOCKET -1
#endif
#ifndef SOCKET_ERROR
#define SOCKET_ERROR -1
#endif

#ifndef closesocket
#define closesocket close
#endif
#ifndef ioctlsocket
#define ioctlsocket ioctl
#endif

EXTERN_C int (*recvfunc) _PROTO((SOCKET, char *, int));
                                        /* Network receive function     */
#define netread    (*recvfunc)
EXTERN_C int (*sendfunc) _PROTO((SOCKET, char *, int));
                                        /* Network send function        */
#define netwrite   (*sendfunc)
EXTERN_C int (*closefunc) _PROTO((SOCKET));
                                        /* Network close function       */
#define netclose   (*closefunc)
EXTERN_C int (*ioctlfunc) _PROTO((SOCKET, int, int));
                                        /* Network ioctl function       */
#define netioctl  (*ioctlfunc)
EXTERN_C char *(*errfunc) _PROTO((void));
                                        /* Network error function       */
#define neterror (*errfunc)

#include <socket_timeout.h>


#endif /* _NET_H */
