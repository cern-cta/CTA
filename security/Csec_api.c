/*
 * $Id: Csec_api.c,v 1.16 2005/03/15 22:52:37 bcouturi Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_api.c - API functions used for authentication in CASTOR
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

#include "Cglobals.h"
#include "Cthread_api.h"
#include <net.h>
#include <dlfcn.h>

#include <Csec_protocol.h>
#include <Csec.h>

int Cdomainname(char *name, int namele);
EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

struct _serv_table {
    char* name;
};

/**
 * Table of service types
 */
static struct _serv_table service_table[] = {
  {"host"},
  {"castor-central"},
  {"castor-disk"},
  {"castor-tape"},
  {"castor-stager"},
  {""}
};



/*****************************************************************
 *                                                               *
 *               CONTEXT INITIALIZATION FUNCTIONS                *
 *                                                               *
 *****************************************************************/

/**
 * Initializes the Csec context for a client
 */
int 
Csec_client_initContext(Csec_context_t *ctx, 
			int service_type,
			Csec_protocol_t*protocol) {

  char *func="Csec_client_initContext";
  int rc = 0;

  _Csec_trace(func, "Initializing client plugin for service type %d\n",
	      service_type);

  _Csec_init_context(ctx, CSEC_CONTEXT_CLIENT, service_type);


  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    rc =  _Csec_initialize_protocols_from_list(protocol,
					       &(ctx->supported_protocols),
					       &(ctx->nb_supported_protocols));
    if (rc == 0)  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  }

  /* otherwise leave the protocols unloaded for now, 
     will be loaded at context establish */
  return rc;
}



/**
 * Initializes the Csec context for the server
 */
int 
Csec_server_initContext(Csec_context_t *ctx, 
			int service_type,
			Csec_protocol_t*protocol) {
  char *func="Csec_server_initContext";
  int rc = 0;

  _Csec_trace(func, "Initializing server plugin for service type %d\n",
	     service_type);

  _Csec_init_context(ctx, 
		     CSEC_CONTEXT_SERVER, 
		     service_type);

  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    rc =  _Csec_initialize_protocols_from_list(protocol,
					       &(ctx->supported_protocols),
					       &(ctx->nb_supported_protocols));
    if (rc == 0)  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  }

  /* otherwise leave the protocols unloaded for now, 
     will be loaded at context establish */
  return rc;

}

/**
 * Cleans/reinitializes a server context
 */
int   
Csec_server_reinitContext(Csec_context_t *ctx,
			  int service_type,
			  Csec_protocol_t *protocols) {
  char *func="Csec_server_reinitContext";
  
  _Csec_trace(func, "Re-initializing server plugin for service type %d\n",
	      service_type);
  
  Csec_clearContext (ctx);
  return Csec_server_initContext(ctx, service_type, protocols);
}


/**
 * Clears the Csec_context_t structure
 */
int   
Csec_clearContext (Csec_context_t *ctx) {
  _Csec_clear_context(ctx);
  return 0;
}


/*****************************************************************
 *                                                               *
 *               CONTEXT ESTABLISHMENT FUNCTIONS                 *
 *                                                               *
 *****************************************************************/

/**
 * Allows a server to accept a client context
 */
int Csec_server_establishContext (Csec_context_t *ctx,
				  int socket) {
  int rc;
  char *func = "Csec_server_establishContext";
  
  /* Logging and sanity checks */
  _Csec_trace(func, "Entering ctx:%p, socket:%d\n", ctx, socket);
  CHECKCTX(ctx, func,-1);
  
  /* Performing protocol negociation */
  if(_Csec_server_negociateProtocol(ctx, socket)!=0) {
    return -1;
  }
  ctx->flags |= CSEC_CTX_PROTOCOL_CHOSEN;


  /* Loading the plugin */
  _Csec_trace(func, "Looking up plugin for protocol: %s\n", 
	      _Csec_protocol_name(ctx->protocol));

  rc = _Csec_load_plugin(ctx->protocol,
			 &(ctx->plugin));
  if (rc != 0) {
    return -1;
  }
  ctx->flags |= CSEC_CTX_PLUGIN_LOADED;
  

  rc = ctx->plugin->Csec_plugin_initContext(&(ctx->plugin_context),
					    ctx->service_type,
					    ctx->context_type,
					    ctx->options);
  if (rc != 0) {
    _Csec_sync_error(ctx->plugin);
    return -1;
  }
 
  ctx->flags |= CSEC_CTX_PLUGIN_INITIALIZED;

  rc = ctx->plugin->Csec_plugin_serverEstablishContextExt(ctx->plugin_context,
							  socket,
							  NULL,
							  0,
							  &(ctx->peer_id));
  if (rc != 0) {
    _Csec_sync_error(ctx->plugin);
    return -1;
  }
  
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;

  _Csec_trace(func, "The client is %s %s\n", 
	      _Csec_id_mech(ctx->peer_id),
	      _Csec_id_name(ctx->peer_id));

  
  return 0;
}

/**
 * Allows a client to establish a context with a server
 */
int   Csec_client_establishContext (Csec_context_t *ctx,
				    int socket) {
  
  int rc;
  char *func = "Csec_client_establishContext";
  
  /* Logging and sanity checks */
  _Csec_trace(func, "Entering ctx:%p, socket:%d\n", ctx, socket);
  CHECKCTX(ctx, func,-1);

  /* Performing protocol negociation */
  if(_Csec_client_negociateProtocol(ctx, socket)) {
    return -1;
  }
  ctx->flags |= CSEC_CTX_PROTOCOL_CHOSEN;

  /* Loading the plugin */
  _Csec_trace(func, "Looking up plugin for protocol: %s\n", 
	      _Csec_protocol_name(ctx->protocol));

  rc = _Csec_load_plugin(ctx->protocol,
			 &(ctx->plugin));
  if (rc != 0) {
    return -1;
  }

  ctx->flags |= CSEC_CTX_PLUGIN_LOADED;

  rc = ctx->plugin->Csec_plugin_initContext(&(ctx->plugin_context),
					    ctx->service_type,
					    ctx->context_type,
					    ctx->options);
  if (rc != 0) {
    _Csec_sync_error(ctx->plugin);
    return -1;
  }
 
  ctx->flags |= CSEC_CTX_PLUGIN_INITIALIZED;

  rc = ctx->plugin->Csec_plugin_clientEstablishContext(ctx->plugin_context,
						       socket);
  
  if (rc != 0) {
    _Csec_sync_error(ctx->plugin);
    return -1;
  }
  
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;

  return 0;
}


/*****************************************************************
 *                                                               *
 *               ERROR MESSAGE LOOKUP                            *
 *                                                               *
 *****************************************************************/


/**
 * Returns the error message, valid when a Csec call fails.
 */
/* char *Csec_getErrorMessage() { */
/* XXX TO BE IMPLEMENTED */
/* } */


/*****************************************************************
 *                                                               *
 *               SECURITY OPTIONS SETTING                        *
 *                                                               *
 *****************************************************************/


int _Csec_setSecurityOpts(Csec_context_t *ctx, 
			  int option) {
  char *func = "_Csec_setSecurityOpts";
  _Csec_trace(func, "Entering\n");
  /* Sanity checks - complain if we already have an established security context */
  CHECKCTX(ctx,"_Csec_setSecurityOpts",-1);
  if (ctx->flags& CSEC_CTX_CONTEXT_ESTABLISHED) {
    serrno = EINVAL;
    _Csec_errmsg(func, "A security context has already been established");
    return -1;
  }

  ctx->options |= option;
  return 0;
}


int  Csec_client_setSecurityOpts (Csec_context_t *ctx, 
				  int option) {
  CHECKCTX(ctx,"Csec_client_setSecurityOpts",-1)
  return _Csec_setSecurityOpts(ctx, option);
}


int  Csec_server_setSecurityOpts (Csec_context_t *ctx, 
				  int option) {
  CHECKCTX(ctx,"Csec_server_setSecurityOpts",-1)
  return _Csec_setSecurityOpts(ctx, option);
}


/*****************************************************************
 *                                                               *
 *               DELEGATED CREDENTIALS RETRIEVAL                 *
 *                                                               *
 *****************************************************************/


int  Csec_server_getDelegatedCredential (Csec_context_t *ctx, 
					 char **mech, 
					 void **buffer, 
					 size_t *buffer_size) {
  
  char *func = "Csec_server_getDelegatedCredential";
  csec_buffer_t tmpbuff;
  int rc;

  CHECKCTX(ctx, func, -1);
  
  tmpbuff = &(ctx->delegated_credentials);

  /* Export the credentials to the buffer if necessary */
  if (!(ctx->flags & CSEC_CTX_DELEGCREDS_EXPORTED)) {

    rc = ctx->plugin->Csec_plugin_exportDelegatedCredentials(ctx->plugin_context,
							     tmpbuff);
    if (rc != 0) {
      _Csec_sync_error(ctx->plugin);
      return -1;
    }
    
    ctx->flags |= CSEC_CTX_DELEGCREDS_EXPORTED;
  }


  if (mech != NULL) {
    *mech = _Csec_protocol_name(ctx->plugin->protocol);
  }

  if (buffer != NULL) {
    *buffer = tmpbuff->value;
  }

  if (buffer_size != NULL) {
    *buffer_size = tmpbuff->length;
  }

  return 0;
}


/*****************************************************************
 *                                                               *
 *               ID/AUTHORIZATION ID HANDLING                    *
 *                                                               *
 *****************************************************************/


int  Csec_client_setAuthorizationId (Csec_context_t *ctx, 
				     const char *mech, 
				     const char *name) {
  char *func = " Csec_client_setAuthorizationId";
  CHECKCTX(ctx, func, -1);
    
  if(mech== NULL || name == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL mech or name passed\n");
    return -1;
  }
       
  ctx->authorization_id = _Csec_create_id(mech, name);
  return 0;
}

int  Csec_server_getAuthorizationId (Csec_context_t *ctx, 
				     char **mech, 
				     char **name) {
  char *func = " Csec_server_getAuthorizationId";
  CHECKCTX(ctx, func, -1);

  _Csec_trace(func, "Entering\n");

  if(mech== NULL || name == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL mech or name passed");
    return -1;
  }

  *mech = _Csec_id_mech(ctx->authorization_id);
  *name = _Csec_id_name(ctx->authorization_id);
  return 0;
}

int  Csec_server_getClientId (Csec_context_t *ctx, 
			      char **mech, 
			      char **name) {
  char *func = "Csec_server_getClientId";
  CHECKCTX(ctx, func, -1);

  if(mech== NULL || name == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL mech or name passed \n");
    return -1;
  }
  
  *mech = _Csec_id_mech(ctx->peer_id);
  *name = _Csec_id_name(ctx->peer_id);
  return 0;
}


int  Csec_isIdAService (const char *mech, const char *name) {
  /* XXX TO BE IMPLEMENTED */
  return 0;
}


int Csec_server_isClientAService(Csec_context_t *ctx) {
  char *func = "Csec_server_isClientAService";
  char *mech, *clientid;
  
  CHECKCTX(ctx, func, -1);
  if (!(ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED)) {
    serrno = EINVAL;
    _Csec_errmsg(func, "requires an established context");
    return -1;
  }
  
  if (Csec_server_getClientId(ctx, &mech, &clientid) < 0) {
    return -1;
  }

  return  Csec_isIdAService(mech, clientid);
}


int  Csec_mapToLocalUser (const char *mech, 
			  const char *name, 
			  char *username_buf, 
			  size_t buffsize, 
			  uid_t *uid, 
			  gid_t *gid) {
  
  Csec_plugin_t *plugin=NULL;
  Csec_id_t *id=NULL, *mapped_id=NULL;
  Csec_protocol_t *prot = NULL;
  int rc;

  prot = _Csec_create_protocol(mech);

  /* First load the plugin */
  rc = _Csec_load_plugin(prot,
			 &plugin);
  if (rc == 0) {
    id = _Csec_create_id(mech, name);
    rc = plugin->Csec_plugin_map2name(id, &mapped_id);
    if ( rc == 0) {
      strncpy(username_buf, _Csec_id_name(mapped_id), buffsize);
    } else {
      /* Import the error from the plugin if needed */
      _Csec_sync_error(plugin);
    }

    /* Now proceed to the mapping of username to uid/gid */
    _Csec_name2id(username_buf, uid, gid);

  }
  /* After use, unload the plugin and free the memory */
  _Csec_delete_protocol(prot);
  _Csec_unload_plugin(plugin);
  _Csec_delete_id(id);
  _Csec_delete_id(mapped_id);  
  return rc;
}


int Csec_server_mapClientToLocalUser(Csec_context_t *ctx, 
				     char **username, 
				     uid_t *uid, 
				     gid_t *gid) {

  char *func = "Csec_server_mapClientToLocalUser";
  char *mech, *clientid;
  char tmpusername[CA_MAXUSRNAMELEN+1];
  uid_t tuid;
  gid_t tgid;

  CHECKCTX(ctx, func, -1);
  if (!(ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED)) {
    serrno = EINVAL;
    _Csec_errmsg(func, "requires an established context");
    return -1;
  }
  
  if (Csec_server_getClientId(ctx, &mech, &clientid) < 0) {
    return -1;
  }

  if (!(ctx->flags & CSEC_CTX_USER_MAPPING_DONE)) {
    if (Csec_mapToLocalUser(mech, clientid,
			    tmpusername, CA_MAXUSRNAMELEN,
			    &tuid, &tgid) < 0) {
      return -1;
    }
    ctx->peer_username = strdup(tmpusername);
    ctx->peer_uid = tuid;
    ctx->peer_gid = tgid;
  }


  if (username) {
    *username = ctx->peer_username;
  }


  if (uid) {
    *uid = ctx->peer_uid;
  }

  if (gid) {
    *gid = ctx->peer_gid;
  }

  return 0;
}

