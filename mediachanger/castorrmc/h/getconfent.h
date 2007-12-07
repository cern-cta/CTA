/*
 * $Id: getconfent.h,v 1.5 2007/12/07 11:40:53 sponcec3 Exp $
 */

#ifndef __getconfent_h
#define __getconfent_h

#include "osdep.h"

EXTERN_C char DLL_DECL *getconfent _PROTO((const char *, const char *, int));
EXTERN_C char DLL_DECL *getconfent_fromfile _PROTO((const char *, const char *, const char *, int));
EXTERN_C int DLL_DECL getconfent_multi _PROTO((const char *, const char *, int, char ***, int *));
EXTERN_C int DLL_DECL getconfent_multi_fromfile _PROTO((const char *, const char *, const char *, int, char ***, int *));
EXTERN_C int DLL_DECL getconfent_parser _PROTO((char **, char ***, int *));

#endif /* __getconfent_h */
