/*
 * $Id: getconfent.h,v 1.2 2005/05/23 08:49:46 mmarques Exp $
 */

#ifndef __getconfent_h
#define __getconfent_h

#include "osdep.h"

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));
EXTERN_C char DLL_DECL *getconfent_fromfile _PROTO((char *, char *, char *, int));
EXTERN_C int DLL_DECL getconfent_multi _PROTO((char *, char *, int, char ***, int **));
EXTERN_C int DLL_DECL getconfent_multi_fromfile _PROTO((char *, char *, char *, int, char ***, int **));
EXTERN_C int DLL_DECL getcontent_parser _PROTO((char **, char ***, int **));

#endif /* __getconfent_h */
