/*
 * $Id: stager_api.h,v 1.1 2004/10/19 18:16:42 jdurand Exp $
 */

#ifndef stager_api_h
#define stager_api_h

#include <sys/types.h>
#include "osdep.h"

EXTERN_C int DLL_DECL stagerApiConfigTimeout     _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigPort        _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigDebug       _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigTrace       _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigDbNthread   _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigUserNthread _PROTO((int *));
EXTERN_C int DLL_DECL stagerApiConfigFacility     _PROTO((size_t, char *));

#endif /* stager_api_h */
