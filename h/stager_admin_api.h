/*
 * $Id: stager_admin_api.h,v 1.7 2004/10/28 17:43:00 jdurand Exp $
 */

#ifndef stager_admin_api_h
#define stager_admin_api_h

#include <sys/types.h>
#include "osdep.h"

EXTERN_C int DLL_DECL stager_configTimeout           _PROTO((int *));
EXTERN_C int DLL_DECL stager_configPort              _PROTO((int *));
EXTERN_C int DLL_DECL stager_configSecurePort        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configNotifyPort        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configSecure            _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDebug             _PROTO((int *));
EXTERN_C int DLL_DECL stager_configTrace             _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDbNthread         _PROTO((int *));
EXTERN_C int DLL_DECL stager_configQueryNthread      _PROTO((int *));
EXTERN_C int DLL_DECL stager_configUpdateNthread     _PROTO((int *));
EXTERN_C int DLL_DECL stager_configGetnextNthread    _PROTO((int *));
EXTERN_C int DLL_DECL stager_configFacility          _PROTO((size_t, char *));
EXTERN_C int DLL_DECL stager_configIgnoreCommandLine _PROTO((int *));
EXTERN_C int DLL_DECL stager_sendToStager            _PROTO((u_signed64, char *));
EXTERN_C int DLL_DECL stager_configHost              _PROTO((size_t, char *));
EXTERN_C int DLL_DECL stager_seterrbuf               _PROTO((char *, int));
EXTERN_C int DLL_DECL stager_setoutbuf               _PROTO((char *, int));
EXTERN_C int DLL_DECL stager_geterrbuf               _PROTO((char **, int *));
EXTERN_C int DLL_DECL stager_getoutbuf               _PROTO((char **, int *));
EXTERN_C int DLL_DECL stager_setlog                  _PROTO((void (*)(int, char*)));
EXTERN_C int DLL_DECL stage_getlog                   _PROTO((void (**)(int, char*)));
EXTERN_C int DLL_DECL stage_errmsg                   _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL stage_outmsg                   _PROTO((char *, char *, ...));

#endif /* stager_admin_api_h */
