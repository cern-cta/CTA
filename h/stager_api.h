/*
 * $Id: stager_api.h,v 1.2 2004/10/20 09:25:38 jdurand Exp $
 */

#ifndef stager_api_h
#define stager_api_h

#include <sys/types.h>
#include "osdep.h"

EXTERN_C int DLL_DECL stager_configTimeout     _PROTO((int *));
EXTERN_C int DLL_DECL stager_configPort        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDebug       _PROTO((int *));
EXTERN_C int DLL_DECL stager_configTrace       _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDbNthread   _PROTO((int *));
EXTERN_C int DLL_DECL stager_configUserNthread _PROTO((int *));
EXTERN_C int DLL_DECL stager_configFacility     _PROTO((size_t, char *));

#endif /* stager_api_h */
