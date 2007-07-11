/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cnetdb.h,v $ $Revision: 1.11 $ $Date: 2007/07/11 14:27:12 $ CERN IT-PDP/DM Olof Barring
 */


#ifndef _CNETDB_H
#define _CNETDB_H

#include <osdep.h>
#if !defined(_WIN32)
#include <netdb.h>
#else
#include <winsock2.h>
#endif

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#if defined(hpux) || defined(HPUX10) || defined(sgi) || defined(SOLARIS)
EXTERN_C int DLL_DECL *C__h_errno _PROTO((void));
#define h_errno (*C__h_errno())
#endif /* hpux || HPUX10 || sgi || SOLARIS */
#endif /* _REENTRANT || _THREAD_SAFE */


EXTERN_C struct hostent DLL_DECL *Cgethostbyname _PROTO((CONST char *));
EXTERN_C struct hostent DLL_DECL *Cgethostbyaddr _PROTO((CONST void *, size_t, int));
EXTERN_C struct servent DLL_DECL *Cgetservbyname _PROTO((CONST char *, CONST char *));

#if defined(_WIN32)
#define CLOSE(x)        closesocket(x)  /* Actual close system call     */
#define IOCTL(x,y,z)    ioctlsocket(x,y,&(z)) /* Actual ioctl system call*/
#else /* _WIN32 */
#define CLOSE(x)        ::close(x)        /* Actual close system call     */
#define IOCTL(x,y,z)    ::ioctl(x,y,z)    /* Actual ioctl system call     */
#endif /* _WIN32 */

#endif /* _CNETDB_H */
