/*
 * $Id*
 */

#ifndef __stager_errmsg_h
#define __stager_errmsg_h

#include "osdep.h"

#ifdef __cplusplus
 extern "C" {
#endif 

int DLL_DECL stager_seterrbuf _PROTO((char *, int));
int DLL_DECL stager_setoutbuf _PROTO((char *, int));
int DLL_DECL stager_geterrbuf _PROTO((char **, int *));
int DLL_DECL stager_getoutbuf _PROTO((char **, int *));
int DLL_DECL stager_setlog _PROTO((void (*) _PROTO((int, char *))));
int DLL_DECL stager_getlog _PROTO((void (**) _PROTO((int, char *))));
int DLL_DECL stager_errmsg _PROTO((const char *, const char *, ...));
int DLL_DECL stager_outmsg _PROTO((const char *, const char *, ...));

#ifdef __cplusplus
 }
#endif

#endif /* __stager_errmsg_h */
