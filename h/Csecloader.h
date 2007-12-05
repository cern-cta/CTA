/*
 * Csecloader.h,v 1.00 2007/12/01 
 */

#ifndef _CSECLOADER_H
#define _CSECLOADER_H

#include <dlfcn.h>
#include <Csec_api.h>

  
int DLL_DECL loader _PROTO(());
int DLL_DECL getServer_initContext _PROTO((Csec_context_t *, int, Csec_protocol *));
int DLL_DECL getClient_initContext _PROTO((Csec_context_t *, int,Csec_protocol *));
int DLL_DECL getClient_establishContext _PROTO((Csec_context_t *,int));
int DLL_DECL getServer_establishContext _PROTO((Csec_context_t *,int));
int DLL_DECL getClientId _PROTO((Csec_context_t *, char **, char **));
int DLL_DECL getMapUser _PROTO((const char *, const char *, char *, size_t, uid_t *, gid_t *));
int DLL_DECL getClearContext _PROTO((Csec_context_t *));

#endif
