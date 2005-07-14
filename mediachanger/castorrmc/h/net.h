/*
 * $Id: net.h,v 1.5 2005/07/14 14:02:52 jdurand Exp $ 
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
#if !defined(_WIN32)
typedef int SOCKET;
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

#else /* _WIN32 */
#include <winsock2.h>
/*
 * Some UNIX definitions for NT
 */
#if defined(SD_RECEIVE)
#define SHUT_RD SD_RECEIVE
#else /* SD_RECEIVE */
#define SHUT_RD 0x00
#endif /* SD_RECEIVE */

#if defined(SD_SEND)
#define SHUT_WR SD_SEND
#else /* SD_SEND */
#define SHUT_WR 0x01
#endif /* SD_SEND */

#if defined(SD_BOTH)
#define SHUT_RDWR SD_BOTH
#else /* SD_BOTH */
#define SHUT_RDWR 0x02
#endif /* SD_BOTH */

#endif /* _WIN32 */

EXTERN_C int DLL_DECL (*recvfunc) _PROTO((SOCKET, char *, int));
                                        /* Network receive function     */
#define netread    (*recvfunc)
EXTERN_C int DLL_DECL (*sendfunc) _PROTO((SOCKET, char *, int));
                                        /* Network send function        */
#define netwrite   (*sendfunc)
EXTERN_C int DLL_DECL (*closefunc) _PROTO((SOCKET));
                                        /* Network close function       */
#define netclose   (*closefunc)
EXTERN_C int DLL_DECL (*ioctlfunc) _PROTO((SOCKET, int, int));
                                        /* Network ioctl function       */
#define netioctl  (*ioctlfunc)
EXTERN_C char DLL_DECL *(*errfunc) _PROTO((void));
                                        /* Network error function       */
#define neterror (*errfunc)

#include <socket_timeout.h>


#endif /* _NET_H */
