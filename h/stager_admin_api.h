/*
 * $Id: stager_admin_api.h,v 1.16 2005/07/06 08:57:43 jdurand Exp $
 */

#ifndef stager_admin_api_h
#define stager_admin_api_h

#include <sys/types.h>
#include "osdep.h"

EXTERN_C int DLL_DECL stager_configTimeout           _PROTO((int *));
EXTERN_C int DLL_DECL stager_configNoDlf             _PROTO((int *));
EXTERN_C int DLL_DECL stager_configPort              _PROTO((int *));
EXTERN_C int DLL_DECL stager_configSecurePort        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configNotifyPort        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configSecure            _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDebug             _PROTO((int *));
EXTERN_C int DLL_DECL stager_configTrace             _PROTO((int *));
EXTERN_C int DLL_DECL stager_configDbNbthread        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configGCNbthread        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configErrorNbthread        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configQueryNbthread     _PROTO((int *));
EXTERN_C int DLL_DECL stager_configGetnextNbthread   _PROTO((int *));
EXTERN_C int DLL_DECL stager_configJobNbthread       _PROTO((int *));
EXTERN_C int DLL_DECL stager_configAdminNbthread     _PROTO((int *));
EXTERN_C int DLL_DECL stager_configFsNbthread        _PROTO((int *));
EXTERN_C int DLL_DECL stager_configFsUpdate          _PROTO((int *));
EXTERN_C int DLL_DECL stager_configFacility          _PROTO((size_t, char *));
EXTERN_C int DLL_DECL stager_configLog               _PROTO((size_t, char *));
EXTERN_C int DLL_DECL stager_configIgnoreCommandLine _PROTO((int *));
EXTERN_C int DLL_DECL stager_notifyStager            _PROTO((u_signed64, void *, size_t));
EXTERN_C int DLL_DECL stager_notifyHost              _PROTO((size_t, char *));
EXTERN_C int DLL_DECL stager_seterrbuf               _PROTO((char *, int));
EXTERN_C int DLL_DECL stager_setoutbuf               _PROTO((char *, int));
EXTERN_C int DLL_DECL stager_geterrbuf               _PROTO((char **, int *));
EXTERN_C int DLL_DECL stager_getoutbuf               _PROTO((char **, int *));
EXTERN_C int DLL_DECL stager_setlog                  _PROTO((void (*)(int, char*)));
EXTERN_C int DLL_DECL stage_getlog                   _PROTO((void (**)(int, char*)));
EXTERN_C int DLL_DECL stage_errmsg                   _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL stage_outmsg                   _PROTO((char *, char *, ...));

#endif /* stager_admin_api_h */
