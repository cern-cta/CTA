/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA Vitaly Motyakov
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_static_loader.c,v $ $Revision: 1.3 $ $Date: 2005/12/12 15:24:36 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

/*
 * Csec_static_loader.c - API function used for authentication in CASTOR
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

#include "Cglobals.h"
#include <net.h>
#include <dlfcn.h>

#include "Csec.h"

#define _CSEC_CALLS_PLUGIN
#include "Csec_plugin.h"

#define SET_FP(CTX,MECH) \
PLUGINFP(CTX,Csec_activate) = &__CONCAT(Csec_activate,MECH); \
PLUGINFP(CTX,Csec_deactivate) = &__CONCAT(Csec_deactivate,MECH); \
PLUGINFP(CTX,Csec_init_context) = &__CONCAT(Csec_init_context,MECH); \
PLUGINFP(CTX,Csec_reinit_context) = &__CONCAT(Csec_reinit_context,MECH); \
PLUGINFP(CTX,Csec_delete_connection_context) = &__CONCAT(Csec_delete_connection_context,MECH); \
PLUGINFP(CTX,Csec_delete_creds) = &__CONCAT(Csec_delete_creds,MECH); \
PLUGINFP(CTX,Csec_acquire_creds) = &__CONCAT(Csec_acquire_creds,MECH); \
PLUGINFP(CTX,Csec_server_establish_context_ext) = &__CONCAT(Csec_server_establish_context_ext,MECH); \
PLUGINFP(CTX,Csec_client_establish_context) = &__CONCAT(Csec_client_establish_context,MECH); \
PLUGINFP(CTX,Csec_map2name) = &__CONCAT(Csec_map2name,MECH); \
PLUGINFP(CTX,Csec_get_service_name) = &__CONCAT(Csec_get_service_name,MECH);

void Csec_unload_shlib(Csec_context_t *ctx) {
  char *func = "Csec_unload_shlib";
  Csec_trace(func, "Unloading library\n");

  /* Just keep the 3 initial flags */
  ctx->flags &= (CSEC_CTX_INITIALIZED
		 |CSEC_CTX_SERVICE_TYPE_SET
		 |CSEC_CTX_PROTOCOL_LOADED);

  (void) (*(ctx->Csec_deactivate))(ctx);

  ctx->Csec_activate = NULL;
  ctx->Csec_deactivate = NULL;
  ctx->Csec_init_context = NULL;
  ctx->Csec_reinit_context = NULL;
  ctx->Csec_delete_connection_context = NULL;
  ctx->Csec_delete_creds = NULL;
  ctx->Csec_acquire_creds = NULL;
  ctx->Csec_server_establish_context_ext = NULL;
  ctx->Csec_client_establish_context = NULL;
  ctx->Csec_map2name = NULL;
  ctx->Csec_get_service_name = NULL;

  free(ctx->shhandle);
  ctx->shhandle = NULL;
}

/**
 * Overloads static library functions corresponding to the context !
 */
void *Csec_get_shlib(Csec_context_t *ctx) {

  char *func = "Csec_get_shlib";

  Csec_trace(func, "Overloading functions\n");

  /* Checking input */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL !");
    return NULL;
  }

  Csec_trace(func, "Loading library functions for mechanism <%s>\n",
	     ctx->protocols[ctx->current_protocol].id);

  if (ctx->shhandle == NULL)
    ctx->shhandle = malloc(sizeof(Csec_plugin_pluginptrs_t));

  if (ctx->shhandle == NULL) {
    Csec_errmsg(func, "Could not allocate memory for context plugin handle");
    serrno = ENOMEM;
    return NULL;
  }

  if (strcmp(ctx->protocols[ctx->current_protocol].id, "ID") == 0) {
    SET_FP(ctx,_ID);
  }
#ifdef KRB5
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "KRB5") == 0) {
    SET_FP(ctx,_KRB5);
  }
#endif
#ifdef GSI
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "GSI") == 0) {
    if (ctx->thread_safe) {
      SET_FP(ctx,_GSI_pthr);
    } else {
      SET_FP(ctx,_GSI);
    }
  }
#endif
#ifdef KRB4
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "KRB4") == 0) {
    SET_FP(ctx,_KRB4);
  }
#endif
  else {
    Csec_trace(func, "Unknonwn mechanism <%s>\n",
	       ctx->protocols[ctx->current_protocol].id);
    free(ctx->shhandle);
    ctx->shhandle = NULL;
    return(NULL);
  }    

  ctx->Csec_activate = &Csec_activate_caller;
  ctx->Csec_deactivate = &Csec_deactivate_caller;
  ctx->Csec_init_context = &Csec_init_context_caller;
  ctx->Csec_reinit_context = &Csec_reinit_context_caller;
  ctx->Csec_delete_connection_context = &Csec_delete_connection_context_caller;
  ctx->Csec_delete_creds = &Csec_delete_creds_caller;
  ctx->Csec_acquire_creds = &Csec_acquire_creds_caller;
  ctx->Csec_server_establish_context_ext = &Csec_server_establish_context_ext_caller;
  ctx->Csec_client_establish_context = &Csec_client_establish_context_caller;
  ctx->Csec_map2name = &Csec_map2name_caller;
  ctx->Csec_get_service_name = &Csec_get_service_name_caller;

  if ( (*(ctx->Csec_activate))(ctx) <0) {
    /* Csec_activate should have set error message and number */
    free(ctx->shhandle);
    return NULL;
  }

  ctx->flags |= CSEC_CTX_SHLIB_LOADED;

  return ((void*)1);
    
}
