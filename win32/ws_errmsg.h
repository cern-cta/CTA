/*
 * @(#)ws_errmsg.h	1.1 12/11/98 CERN IT-PDP/IP Aneta Baran
 */

/*
 * Copyright (C) 1990-1998 by CERN/IT/PDP/IP
 * All rights reserved
 */


#ifndef WS_ERRMSG_H
#define WS_ERRMSG_H
#if defined(_WIN32)
#include <winsock2.h>
#include <stdio.h>

char *ws_errmsg[] = {
   "No error - operation completed successfully.", 	/* 10000 */
   "", "", "",
   "WSAEINTR: Interrupted system call.", 		/* 10004 */ 
   "", "", "", "",
   "WSAEBADF: Bad file number",				/* 10009 */ 
   "", "", "",
   "WSAEACCES: Permission denied", 			/* 10013 */
   "WSAEFAULT: Bad address",				/* 10014 */
   "", "", "", "", "", "", "",
   "WSAEINVAL: Invalid argument",			/* 10022 */
   "",
   "WSAEMFILE: Too many open files",			/* 10024 */
   "", "", "", "", "", "", "", "", "", "", "", 
   "WSAEWOULDBLOCK: Operation would block", 		/* 10035 */
   "WSAEINPROGRESS: Operation now in progress", 	/* 10036 */
   "WSAEALREADY: Operation already in progress",	/* 10037 */
   "WSAENOTSOCK: Socket operation on non-socket",	/* 10038 */
   "WSAEDESTADDRREQ: Destination address required",	/* 10039 */
   "WSAEMSGSIZE: Message too long",			/* 10040 */
   "WSAEPROTOTYPE: Protocol wrong type for sockets",	/* 10041 */
   "WSAENOPROTOOPT: Protocol not available", 		/* 10042 */
   "WSAEPROTONOSUPPORT: Protocol not supported",	/* 10043 */
   "WSAESOCKTNOSUPPORT: Socket type not supported",	/* 10044 */
   "WSAEOPNOTSUPP: Operation not supported on socket",	/* 10045 */
   "WSAEPFNOSUPPORT: Protocol family not supported",    /* 10046 */
   "WSAEAFNOSUPPORT: Address family not supported by protocol family"/* 10047 */
   "WSAEADDRINUSE: Address already in use",		/* 10048 */
   "WSAEADDRNOTAVAIL: Cannot assign requested address",	/* 10049 */
   "WSAENETDOWN: Network is down",			/* 10050 */
   "WSAENETUNREACH: Network is unreachable", 		/* 10051 */
   "WSAENETRESET: Network dropped connection on reset",	/* 10052 */
   "WSAECONNABORTED: Software caused connection abort",	/* 10053 */
   "WSAECONNRESET: Connection reset by peer",		/* 10054 */
   "WSAENOBUFS: No buffer space available",		/* 10055 */
   "WSAEISCONN: Socket is already connected",		/* 10056 */
   "WSAENOTCONN: Socket is not connected",		/* 10057 */
   "WSAESHUTDOWN: Can't send after socket shutdown",	/* 10058 */
   "WSAETOOMANYREFS: Too many references: cannot splice",/* 10059 */
   "WSAETIMEDOUT: Connection timed out",		/* 10060 */
   "WSAECONNREFUSED: Connection refused",		/* 10061 */
   "WSAELOOP: Too many levels of symbolic link",	/* 10062 */
   "WSAENAMETOOLONG: File name too long",		/* 10063 */
   "WSAEHOSTDOWN: Host is down",			/* 10064 */
   "WSAEHOSTUNREACH: No route to host",			/* 10065 */
   "WSAENOTEMPTY: Directory not empty",			/* 10066 */
   "WSAEPROCLIM: Too many processes",			/* 10067 */
   "WSAEUSERS: Too many users",				/* 10068 */
   "WSAEDQUOT: Disc quota exceed",			/* 10069 */
   "WSAESTALE: No filesystem",				/* 10070 */
   "WSAEREMOTE: Item is not local to host",		/* 10071 */
   "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
   "WSASYSNOTREADY",				/* 10091 */ 
   "WSAVERNOTSUPPORTED",			/* 10092 */ 
   "WSANOTINITIALISED",				/* 10093 */ 
   "", "", "", "", "", "", "",				 
   "WSAEDISCON",				/* 10101 */ 
   "WSAENOMORE",				/* 10102 */ 
   "WSAECANCELLED",				/* 10103 */ 
   "WSAEINVALIDPROCTABLE",			/* 10104 */ 
   "WSAEINVALIDPROVIDER",   			/* 10105 */ 
   "WSAEPROVIDERFAILEDINIT",			/* 10106 */ 
   "WSASYSCALLFAILURE",				/* 10107 */ 
   "WSASERVICE_NOT_FOUND",			/* 10108 */ 
   "WSATYPE_NOT_FOUND",				/* 10109 */ 
   "WSA_E_NO_MORE",				/* 10110 */ 
   "WSA_E_CANCELLED",				/* 10111 */ 
   "WSAEREFUSED",				/* 10112 */ 
};

char *geterr(void);
int ws_perror (char *s);
char *ws_strerr (int err_no);

#endif	/* WIN32 */
#endif	/* WS_ERRMSG_H */
