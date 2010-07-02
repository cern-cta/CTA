/*
 * Csecloader.h,v 1.00 2007/12/01 
 */

#ifndef _CSECLOADER_H
#define _CSECLOADER_H

#include <dlfcn.h>
#include <Csec_api.h>

  
int loader _PROTO(());
int getServer_initContext _PROTO((Csec_context_t *, int, Csec_protocol *));
int getClient_initContext _PROTO((Csec_context_t *, int,Csec_protocol *));
int getClient_establishContext _PROTO((Csec_context_t *,int));
int getServer_establishContext _PROTO((Csec_context_t *,int));
int getClientId _PROTO((Csec_context_t *, char **, char **));
int getMapUser _PROTO((const char *, const char *, char *, size_t, uid_t *, gid_t *));
int getClearContext _PROTO((Csec_context_t *));

#endif
