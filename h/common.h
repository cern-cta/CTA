/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: common.h,v $ $Revision: 1.1 $ $Date: 2004/10/29 07:52:55 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * common.h   - common prototypes
 */

#ifndef _COMMON_H_INCLUDED_
#define _COMMON_H_INCLUDED_

#if !defined(_WIN32)
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#else
#include <winsock2.h>
#endif

#include <Castor_limits.h>
#include <osdep.h>
#include <Cgetopt.h>
#include <Cglobals.h>
#include <Cnetdb.h>
#include <Cpool_api.h>
#include <Cpwd.h>
#include <Cthread_api.h>
#include <getacct.h>
#include <getacctent.h>
#include <log.h>
#include <net.h>
#include <serrno.h>
#include <socket_timeout.h>
#include <trace.h>
#include <u64subr.h>
#include <ypgetacctent.h>

EXTERN_C char DLL_DECL *getconfent_r _PROTO((char *, char *, int, char *, int));
EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));
EXTERN_C int DLL_DECL setnetio _PROTO((int));
EXTERN_C int DLL_DECL solveln _PROTO((char *, char *, int));
EXTERN_C int DLL_DECL seelink _PROTO((char *, char *, int));
EXTERN_C int DLL_DECL isremote _PROTO((struct in_addr, char *));
EXTERN_C int DLL_DECL CDoubleDnsLookup _PROTO((SOCKET s, char *));
EXTERN_C int DLL_DECL isadminhost _PROTO((SOCKET s, char *));
EXTERN_C char DLL_DECL *getifnam_r _PROTO((SOCKET, char *, size_t));
EXTERN_C char DLL_DECL *getifnam _PROTO((SOCKET));
EXTERN_C int DLL_DECL get_user _PROTO((char *, char *, int, int, char *, int *, int *));

/**
 * Checks whether the given string is a size.
 * The accepted syntax is :
 *    [blanks]<digits>[b|k|M|G|T|P]
 * @return -1 if it is not a size,
 * 0 if it is a size with no unit,
 * 1 if it is a size with a unit
 */
EXTERN_C int  DLL_DECL check_for_strutou64 _PROTO((char*));

#endif /* _COMMON_H_INCLUDED_ */
