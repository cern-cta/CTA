/*
 * $Id: Csec_api_loader.c,v 1.9 2004/08/27 14:36:39 motiakov Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_api_loader.c - API function used for authentication in CASTOR
 */

#include <osdep.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <stdarg.h>

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

#include "marshall.h"
#include "serrno.h"
#include "Cnetdb.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#ifndef lint
static char sccsid[] = "@(#)Csec_api_loader.c,v 1.1 2004/01/12 10:31:39 CERN IT/ADC/CA Benjamin Couturier";
#endif

#include <Cmutex.h>
#include <Cglobals.h>
#include <Cthread_api.h>
#include <net.h>
#include <serrno.h>
#include <dlfcn.h>

#include <Csec.h>

/* Macro to initialize one symbol in the context structure */
#define DLSETFUNC(CTX, HDL, SYM) strcpy(symname, #SYM "_"); \
    strcat(symname, CTX->protocols[CTX->current_protocol].id); \
    if ((CTX->SYM = dlsym(HDL, symname)) == NULL) { \
    serrno =  ESEC_NO_SECMECH;                                  \
    Csec_errmsg(func, "Error finding symbol %s: %s\n",		\
		#SYM, dlerror());					\
    CTX->shhandle = NULL;						\
    return NULL; }


void Csec_unload_shlib(Csec_context_t *ctx) {
  char *func = "Csec_unload_shlib";

  Csec_trace(func, "Unloading plugin\n");

  if (ctx->shhandle != NULL) {
    dlclose(ctx->shhandle);
    ctx->shhandle = NULL;
  }

  /* Just keep the 3 initial flags */
  ctx->flags &= (CSEC_CTX_INITIALIZED
		 |CSEC_CTX_SERVICE_TYPE_SET
		 |CSEC_CTX_PROTOCOL_LOADED);
  ctx->Csec_init_context = NULL;
  ctx->Csec_reinit_context = NULL;
  ctx->Csec_delete_connection_context = NULL;
  ctx->Csec_delete_creds = NULL;
  ctx->Csec_acquire_creds = NULL;
  ctx->Csec_server_establish_context_ext = NULL;
  ctx->Csec_client_establish_context = NULL;
  ctx->Csec_map2name = NULL;
  ctx->Csec_get_service_name = NULL;
 
}

/**
 * Gets the shared library corresponding to the context !
 */
void *Csec_get_shlib(Csec_context_t *ctx) {
  char filename[CA_MAXNAMELEN];
  char symname[256];
  void *handle;
  char *func = "Csec_get_shlib";
 
  Csec_trace(func, "Loading plugin\n");

  /* Checking input */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL !");
    return NULL;
  }

  /* Checking that a shared library isn't already loaded,
     and closing in case ! */
  if (ctx->shhandle != NULL) {
    Csec_trace(func, "Forcing unload of shlib\n");
    Csec_unload_shlib(ctx);
  }

  /* Creating the library name */
  snprintf(filename, CA_MAXNAMELEN, "libCsec_plugin_%s.so", ctx->protocols[ctx->current_protocol].id);
  Csec_trace(func, "Using shared library <%s> for mechanism <%s>\n",
	     filename,
	     ctx->protocols[ctx->current_protocol].id);
    
  handle = dlopen(filename, RTLD_NOW);
  
  if (handle == NULL) {
    char dlerrmsg[ERRBUFSIZE+1];
    serrno =  ESEC_NO_SECMECH;
    strncpy(dlerrmsg, dlerror(), ERRBUFSIZE);
    
    ctx->shhandle = NULL;
    Csec_trace(func, "Error opening shared library %s: %s\n", filename,
	       dlerrmsg);
    Csec_errmsg(func, "Error opening shared library %s: %s\n", filename,
		dlerrmsg);
    
    return NULL;
  }

  ctx->shhandle = handle;

  DLSETFUNC(ctx, handle, Csec_init_context);
  DLSETFUNC(ctx, handle, Csec_reinit_context);
  DLSETFUNC(ctx, handle, Csec_delete_connection_context);
  DLSETFUNC(ctx, handle, Csec_delete_creds);
  DLSETFUNC(ctx, handle, Csec_acquire_creds);
  DLSETFUNC(ctx, handle, Csec_server_establish_context_ext);
  DLSETFUNC(ctx, handle, Csec_client_establish_context);
  DLSETFUNC(ctx, handle, Csec_map2name);
  DLSETFUNC(ctx, handle, Csec_get_service_name);

  ctx->flags |= CSEC_CTX_SHLIB_LOADED;

  return handle;
    
}






