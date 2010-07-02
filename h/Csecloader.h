/*
 * Csecloader.h,v 1.00 2007/12/01 
 */

#ifndef _CSECLOADER_H
#define _CSECLOADER_H

#include <dlfcn.h>
#include <Csec_api.h>

  
int loader ();
int getServer_initContext (Csec_context_t *, int, Csec_protocol *);
int getClient_initContext (Csec_context_t *, int,Csec_protocol *);
int getClient_establishContext (Csec_context_t *,int);
int getServer_establishContext (Csec_context_t *,int);
int getClientId (Csec_context_t *, char **, char **);
int getMapUser (const char *, const char *, char *, size_t, uid_t *, gid_t *);
int getClearContext (Csec_context_t *);

#endif
