/*
 * $Id: Csec_static_loader.c,v 1.1 2004/08/27 14:45:13 motiakov Exp $
 * Copyright (C) 2004 by CERN/IT/ADC/CA Vitaly Motyakov
 * All rights reserved
 */

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

#ifndef lint
static char sccsid[] = "@(#)Csec_static_loader.c,v 1.1 2004/01/12 10:31:39 CERN IT/ADC/CA Benjamin Couturier";
#endif

#include <Cmutex.h>
#include <Cglobals.h>
#include <Cthread_api.h>
#include <net.h>
#include <serrno.h>
#include <dlfcn.h>

#include <Csec.h>
#include <Csec_plugin.h>

struct Csec_func_table {
  /* Pointers to the functions */
  int (*Csec_init_context)(struct Csec_context *);
  int (*Csec_reinit_context)(struct Csec_context *);
  int (*Csec_delete_connection_context)(struct Csec_context *);
  int (*Csec_delete_creds)(struct Csec_context *);
  int (*Csec_acquire_creds)(struct Csec_context *, char *, int);
  int (*Csec_server_establish_context)(struct Csec_context *, int);
  int (*Csec_server_establish_context_ext)(struct Csec_context *, int,char *, int);
  int (*Csec_client_establish_context)(struct Csec_context *, int);
  int (*Csec_map2name)(struct Csec_context *, char *, char *, int);
  int (*Csec_get_service_name)(struct Csec_context *, int, char *, char *, char *, int);
};

#define MAKE_FUNC_TABLE(MECH) struct Csec_func_table MECH ## _func_table = { \
  Csec_init_context_ ## MECH, \
  Csec_reinit_context_ ## MECH, \
  Csec_delete_connection_context_ ## MECH, \
  Csec_delete_creds_ ## MECH, \
  Csec_acquire_creds_ ## MECH, \
  NULL, \
  Csec_server_establish_context_ext_ ## MECH, \
  Csec_client_establish_context_ ## MECH, \
  Csec_map2name_ ## MECH, \
  Csec_get_service_name_ ## MECH \
};

#ifdef KRB4
MAKE_FUNC_TABLE(KRB4)
#endif
#ifdef KRB5
MAKE_FUNC_TABLE(KRB5)
#endif
#ifdef GSI
MAKE_FUNC_TABLE(GSI)
#endif
MAKE_FUNC_TABLE(ID)

void Csec_unload_shlib(Csec_context_t *ctx) {
  char *func = "Csec_unload_shlib";
  Csec_trace(func, "Unloading library\n");

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
 * Overloads static library functions corresponding to the context !
 */
void *Csec_get_shlib(Csec_context_t *ctx) {

  char *func = "Csec_get_shlib";

  struct Csec_func_table func_table;

 
  Csec_trace(func, "Overloading functions\n");

  /* Checking input */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL !");
    return NULL;
  }

  Csec_trace(func, "Loading library functions for mechanism <%s>\n",
	     ctx->protocols[ctx->current_protocol].id);

  if (strcmp(ctx->protocols[ctx->current_protocol].id, "ID") == 0) func_table = ID_func_table;
#ifdef KRB5
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "KRB5") == 0) func_table = KRB5_func_table;
#endif
#ifdef GSI
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "GSI") == 0) func_table = GSI_func_table;
#endif
#ifdef KRB4
  else if (strcmp(ctx->protocols[ctx->current_protocol].id, "KRB4") == 0) func_table = KRB_func_table;
#endif
  else {
    Csec_trace(func, "Unknonwn mechanism <%s>\n",
	       ctx->protocols[ctx->current_protocol].id);
    return(NULL);

  }    
  ctx->Csec_init_context = func_table.Csec_init_context;
  ctx->Csec_reinit_context = func_table.Csec_reinit_context;
  ctx->Csec_delete_connection_context = func_table.Csec_delete_connection_context;
  ctx->Csec_delete_creds = func_table.Csec_delete_creds;
  ctx->Csec_acquire_creds = func_table.Csec_acquire_creds;
  ctx->Csec_server_establish_context_ext = func_table.Csec_server_establish_context_ext;
  ctx->Csec_client_establish_context = func_table.Csec_client_establish_context;
  ctx->Csec_map2name = func_table.Csec_map2name;
  ctx->Csec_get_service_name = func_table.Csec_get_service_name;

  ctx->flags |= CSEC_CTX_SHLIB_LOADED;

  return ((void*)1);
    
}






