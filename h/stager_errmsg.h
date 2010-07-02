/*
 * $Id*
 */

#ifndef __stager_errmsg_h
#define __stager_errmsg_h

#include "osdep.h"

#ifdef __cplusplus
 extern "C" {
#endif 

int stager_seterrbuf _PROTO((char *, int));
int stager_setoutbuf _PROTO((char *, int));
int stager_geterrbuf _PROTO((char **, int *));
int stager_getoutbuf _PROTO((char **, int *));
int stager_setlog _PROTO((void (*) _PROTO((int, char *))));
int stager_getlog _PROTO((void (**) _PROTO((int, char *))));
int stager_errmsg _PROTO((const char *, const char *, ...));
int stager_outmsg _PROTO((const char *, const char *, ...));

#ifdef __cplusplus
 }
#endif

#endif /* __stager_errmsg_h */
