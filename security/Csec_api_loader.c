/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_api_loader.c,v $ $Revision: 1.11 $ $Date: 2005/12/12 15:24:36 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

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

#include "Cglobals.h"
#include "Cmutex.h"
#include <net.h>
#include <dlfcn.h>

#include "Csec.h"

#define _CSEC_CALLS_PLUGIN
#include "Csec_plugin.h"

/* Macro to initialize one symbol in the context structure */
#define DLSETFUNC(CTX, HDL, SYM, SFX) strcpy(symname, #SYM "_");\
    strcat(symname, CTX->protocols[CTX->current_protocol].id);  \
    strcat(symname, SFX);                                       \
    if ((PLUGINFP(CTX,SYM) = dlsym(HDL, symname)) == NULL) {    \
    serrno =  ESEC_NO_SECMECH;                                  \
    Csec_errmsg(func, "Error finding symbol %s: %s",		\
		symname, dlerror());				\
    free(CTX->shhandle);                                        \
    CTX->shhandle = NULL;					\
    return NULL; }                                              \
    CTX->SYM = &__CONCAT(SYM,_caller);

static void *list_base=NULL;
typedef struct {
  char id[CA_MAXCSECPROTOLEN+1];
  void *dlhandle;
  void *next;
} id_list_t;

/* Local protos */
static id_list_t * DLL_DECL _check_for_id _PROTO((char *));
static int DLL_DECL _add_id _PROTO((id_list_t *));
static int DLL_DECL _try_activate_func _PROTO((Csec_context_t *,void *, char *));

/* List handling */
static int list_lock;

static id_list_t *_check_for_id(char *id) {
  char *func = "_check_for_id";
  id_list_t *element;

  Csec_trace(func, "Trying to lookup %s\n",id);

  element = list_base;
  while(element != NULL) {
    if (strcmp(element->id, id) == 0) {
      Csec_trace(func, "Found %s\n",id);
      break;
    }
    element = element->next;
  }

  if (element == NULL)
    Csec_trace(func, "Did not find %s\n",id);

  return element;
}

static int _add_id(id_list_t *new) {
  char *func = "_add_id";
  id_list_t *element;

  Csec_trace(func, "Trying to add %s to list\n",new->id);

  if (_check_for_id(new->id) != NULL) {
    Csec_errmsg(func, "Element with same id already in list");
    serrno = EINVAL;
    return -1;
  }

  element = malloc(sizeof(id_list_t));
  if (element == NULL) {
    Csec_errmsg(func, "Could not allocate memory for buffer");
    serrno = ENOMEM;
    return -1;
  }

  memcpy(element,new,sizeof(id_list_t));
  element->next = NULL;

  if (list_base == NULL) {
    list_base = element;
    Csec_trace(func, "Added as first element\n");
  } else {
    id_list_t *n;
    n = list_base;
    while(n->next != NULL) {
      n=n->next;
    }
    n->next = element;
    Csec_trace(func, "Added to end of list\n");
  }

  Csec_trace(func, "Exiting\n");

  return 0;
}
/* End of list operations */

static int _try_activate_func(Csec_context_t *ctx,void *hdl, char *sfx) {
  char symname[256];
  char *mech = ctx->protocols[ctx->current_protocol].id;
  char *func = "_try_activate_func";
  int ret;

  Csec_trace(func, "Entering\n");

  strcpy(symname, "Csec_activate");
  strcat(symname, "_");
  strcat(symname, mech);
  strcat(symname, sfx);
  Csec_trace(func, "Meth: %s\n", symname);

  ctx->shhandle = malloc(sizeof(Csec_plugin_pluginptrs_t));
  if (ctx->shhandle == NULL) {
    Csec_errmsg(func, "Could not allocate memory for context plugin handle");
    serrno = ENOMEM;
    return -1;
  }
  PLUGINFP(ctx,handle) = hdl;

  PLUGINFP(ctx,Csec_activate) = dlsym(hdl, symname);
  if (PLUGINFP(ctx,Csec_activate) == NULL) {
    free(ctx->shhandle);
    return -1;
  }

  ret = Csec_activate_caller(ctx);

  free(ctx->shhandle);
  ctx->shhandle = NULL;

  return ret;
}
    

/* Don't actually unload library, avoid possibility of
   memory leaks in the sec libraries themseleves through
   multiple initialisations
*/
void Csec_unload_shlib(Csec_context_t *ctx) {
  char *func = "Csec_unload_shlib";

  Csec_trace(func, "Entering\n");

  if (ctx->shhandle != NULL) {
    /* Csec_trace(func, "Calling deactivate method\n");
    (void) (*(ctx->Csec_deactivate))(ctx);
    Csec_trace(func, "Called deactivate method\n");
    dlclose(PLUGINFP(ctx,handle)); */
    free(ctx->shhandle);
    ctx->shhandle = NULL;
  }

  /* Just keep the 3 initial flags.
     FIXME Should probably clear contexts and
           credentials, if they were set */

  ctx->flags &= (CSEC_CTX_INITIALIZED
		 |CSEC_CTX_SERVICE_TYPE_SET
		 |CSEC_CTX_PROTOCOL_LOADED);

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
}

/**
 * Gets the shared library corresponding to the context !
 */
void *Csec_get_shlib(Csec_context_t *ctx) {
  char filename[CA_MAXNAMELEN];
  char filename_thread[CA_MAXNAMELEN];
  char suffix[CA_MAXNAMELEN];
  char symname[256];
  void *handle;
  char *func = "Csec_get_shlib";
  id_list_t *lp;

  static int once = 0;
  static int csec_nothread = 0;
  char *CSEC_NOTHREAD;

  Csec_trace(func, "Loading plugin\n");

  if ( ! once ) {
    if ((CSEC_NOTHREAD = getenv("CSEC_NOTHREAD")) != NULL)
      csec_nothread = atoi(CSEC_NOTHREAD);
    once++;
  }

  /* Checking input */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL !");
    return NULL;
  }

  if (ctx->current_protocol < 0) {
    serrno = EINVAL;
    Csec_errmsg(func, "No valid protocol currently selected");
    return NULL;
  }

  /* Checking that a shared library isn't already loaded,
     and closing in case ! */
  if (ctx->shhandle != NULL) {
    Csec_trace(func, "Forcing unload of shlib\n");
    Csec_unload_shlib(ctx);
  }

  /* Setup the symbol suffix */
  suffix[0] = '\0';
  if (strcmp(ctx->protocols[ctx->current_protocol].id,"GSI")==0 && ctx->thread_safe) {
    if (csec_nothread) 
      Csec_trace(func, "NOT TRYING TO LOAD _pthr !!\n");
    else {
      Csec_trace(func, "    TRYING TO LOAD _pthr !!\n");
      strcpy(suffix,"_pthr");
    }
  }

  Csec_trace(func, "Trying to acquire mutex\n");

  if (Cmutex_lock(&list_lock, -1)<0) {
    Csec_errmsg(func, "Could not lock list_lock");
    serrno = ESEC_SYSTEM;
    return NULL;
  }

  Csec_trace(func, "Locked mutex\n");

  if ((lp = _check_for_id(ctx->protocols[ctx->current_protocol].id)) == NULL) {
    id_list_t list;

    Csec_trace(func, "Could not find library in linked list. Will try to load it\n");

    /* Creating the library name */
    snprintf(filename, CA_MAXNAMELEN, "libCsec_plugin_%s", ctx->protocols[ctx->current_protocol].id);
    strcpy(filename_thread,filename);
    strcat(filename,".so");
    strcat(filename_thread,"_thread.so");

    handle = NULL;
    
    if (ctx->thread_safe && ! csec_nothread ) {
      Csec_trace(func, "Using shared library <%s> for mechanism <%s>\n",
                        filename_thread,
                        ctx->protocols[ctx->current_protocol].id);
      handle = dlopen(filename_thread, RTLD_NOW);
    }
    
    if (handle == NULL) {
      Csec_trace(func, "Using shared library <%s> for mechanism <%s>\n",
                        filename,
                        ctx->protocols[ctx->current_protocol].id);
      handle = dlopen(filename, RTLD_NOW);
    }
  
    if (handle == NULL) {
      char dlerrmsg[ERRBUFSIZE+1];

      Cmutex_unlock(&list_lock);

      serrno =  ESEC_NO_SECMECH;
      strncpy(dlerrmsg, dlerror(), ERRBUFSIZE);
    
      ctx->shhandle = NULL;
      Csec_trace(func, "Error opening shared library %s: %s\n", filename,
	         dlerrmsg);
      Csec_errmsg(func, "Error opening shared library %s: %s", filename,
		  dlerrmsg);
      return NULL;
    }

    if (_try_activate_func(ctx,handle, suffix)<0) {
      dlclose(handle);
      Cmutex_unlock(&list_lock);
      serrno = EINVAL;
      Csec_errmsg(func, "Error calling activate method");
      return NULL;
    }

    Csec_trace(func, "Called activate method OK\n");

    strncpy(list.id, ctx->protocols[ctx->current_protocol].id, CA_MAXCSECPROTOLEN);
    list.id[CA_MAXCSECPROTOLEN] = '\0';
    list.dlhandle = handle;
    if (_add_id(&list)<0) {
      Cmutex_unlock(&list_lock);
      Csec_errmsg(func, "Could not add new id to list");
      serrno = ESEC_SYSTEM;
      return NULL;
    }
    Csec_trace(func, "Library loaded and entry added to list\n");

  } else {
    Csec_trace(func, "Using previously loaded library for %s\n",lp->id);
    handle = lp->dlhandle;
  }

  Csec_trace(func, "Unlocking mutex\n");

  if (Cmutex_unlock(&list_lock)<0) {
    Csec_errmsg(func, "Could not unlock list_lock");
    serrno = ESEC_SYSTEM;
    return NULL;
  }

  ctx->shhandle = malloc(sizeof(Csec_plugin_pluginptrs_t));
  if (ctx->shhandle == NULL) {
    Csec_errmsg(func, "Could not allocate memory for context plugin handle");
    serrno = ENOMEM;
    return NULL;
  }
  PLUGINFP(ctx,handle) = handle;

  DLSETFUNC(ctx, handle, Csec_activate, suffix);
  DLSETFUNC(ctx, handle, Csec_deactivate, suffix);
  DLSETFUNC(ctx, handle, Csec_init_context, suffix);
  DLSETFUNC(ctx, handle, Csec_reinit_context, suffix);
  DLSETFUNC(ctx, handle, Csec_delete_connection_context, suffix);
  DLSETFUNC(ctx, handle, Csec_delete_creds,suffix);
  DLSETFUNC(ctx, handle, Csec_acquire_creds, suffix);
  DLSETFUNC(ctx, handle, Csec_server_establish_context_ext, suffix);
  DLSETFUNC(ctx, handle, Csec_client_establish_context, suffix);
  DLSETFUNC(ctx, handle, Csec_map2name, suffix);
  DLSETFUNC(ctx, handle, Csec_get_service_name, suffix);

  ctx->flags |= CSEC_CTX_SHLIB_LOADED;

  return handle;
    
}
