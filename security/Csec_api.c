/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_api.c,v $ $Revision: 1.17 $ $Date: 2005/12/07 10:19:21 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

/*
 * Csec_api.c - API function used for authentication in CASTOR
 */

#include <osdep.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <stdarg.h>
#include <ctype.h>

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
#include "Csec_protocol_policy.h"

#define CHECKCTX_EXT(CTX,FUNC, RET) if(check_ctx(CTX, FUNC)<0) return RET;
#define CHECKCTX(CTX,FUNC) if(check_ctx(CTX, FUNC)<0) return -1;
int Cdomainname(char *name, int namele);

/**
 * Structure containing the service name/ID
 */
struct _serv_table {
    char* name;
    int   type;
};

/**
 * Table of service types
 */
static struct _serv_table service_table[] = {
    {"host/", CSEC_SERVICE_(HOST)},
    {"castor-central/", CSEC_SERVICE_(CENTRAL)},
    {"castor-disk/", CSEC_SERVICE_(DISK)},
    {"castor-tape/", CSEC_SERVICE_(TAPE)},
    {"castor-stager/", CSEC_SERVICE_(STAGER)},
    {"", 0}
};

static int DLL_DECL _setSecurityOpts _PROTO((Csec_context_t *, int));

/*****************************************************************
 *                                                               *
 *               CONTEXT INITIALIZATION FUNCTIONS                *
 *                                                               *
 *****************************************************************/

/**
 * Initializes the Csec context for a client
 */
int Csec_client_initContext(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {

  char *func="Csec_client_initContext";

  Csec_trace(func, "Initializing client plugin for service type %d\n",
	     service_type);

  memset(ctx, 0, sizeof(Csec_context_t));
  ctx->magic = CSEC_CONTEXT_MAGIC_CLIENT_1;
  ctx->server_service_type = (service_type & CSEC_SERVICE_TYPE_MASK);
  ctx->flags = CSEC_CTX_INITIALIZED|CSEC_CTX_SERVICE_TYPE_SET;

  if (service_type & CSEC_SERVICE_THREAD_MASK) {
    ctx->thread_safe = 1;
  }

  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    return Csec_initialize_protocols_from_list(ctx, protocol);
  }

  /* otherwise leave the protocols unloaded for now, will be loaded at context establish */
  return 0;
}


/**
 * Initializes the Csec context for the server
 */
int Csec_server_initContext(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {
  char *func="Csec_server_initContext";

  Csec_trace(func, "Initializing server plugin for service type %d\n",
	     service_type);

  memset(ctx, 0, sizeof(Csec_context_t));
  ctx->magic = CSEC_CONTEXT_MAGIC_SERVER_1;
  ctx->server_service_type = (service_type & CSEC_SERVICE_TYPE_MASK);
  ctx->flags = CSEC_CTX_INITIALIZED|CSEC_CTX_SERVICE_TYPE_SET;

  if (service_type & CSEC_SERVICE_THREAD_MASK) {
    ctx->thread_safe = 1;
  }

  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    return Csec_initialize_protocols_from_list(ctx, protocol);
  }

  /* otherwise leave the protocols unloaded for now, will be loaded at context establish */
  return 0;
}

/**
 * Re-initializes the Csec context, clearing the
 * variables first if needed.
 */
int Csec_server_reinitContext(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {
  char *func="Csec_server_reinitContext";

  Csec_clearContext(ctx);
  return Csec_server_initContext(ctx, service_type, protocol);
}

/**
 * Clears the Csec_context_t, deallocating memory.
 */
int Csec_clearContext(Csec_context_t *ctx) {
  char *func = "Csec_clearContext";

  Csec_trace(func, "Clearing context\n");
  if (ctx->magic !=  CSEC_CONTEXT_MAGIC_CLIENT_1
      && ctx->magic !=  CSEC_CONTEXT_MAGIC_SERVER_1) {
    Csec_trace(func, "Bad magic:%x - Probably uninitialized context !\n",ctx->magic);
    Csec_errmsg(func, "Not a valid context");
    serrno = EINVAL;
    return -1;
  }

  if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
    if (ctx->Csec_delete_connection_context != NULL)
      (*(ctx->Csec_delete_connection_context))(ctx);
  }
  if (ctx->flags & (CSEC_CTX_CREDENTIALS_LOADED | CSEC_CTX_DELEG_CRED_LOADED)) {
    if (ctx->Csec_delete_creds != NULL)
      (*(ctx->Csec_delete_creds))(ctx);
  }

  if (ctx->shhandle != NULL)
    Csec_unload_shlib(ctx);

  if (ctx->total_protocols != NULL) {
    free(ctx->total_protocols);
  }

  if (ctx->protocols != NULL) {
    free(ctx->protocols);
  }

  if (ctx->peer_protocols != NULL) {
    free(ctx->peer_protocols);
  }

  /* Clearing the VOMS attributes */
  if (ctx->voname != NULL) {
    free(ctx->voname);
  }

  /* fqan is a NULL terminated array of char * */
  if (ctx->fqan != NULL) {
    int i;
    for (i = 0; i < ctx->nbfqan; i++)
      free(ctx->fqan[i]);
    free(ctx->fqan);
  }
  

  memset(ctx, 0, sizeof(Csec_context_t));

  return 0;
    
}

/*****************************************************************
 *                                                               *
 *               CONTEXT ESTABLISHMENT FUNCTIONS                 *
 *                                                               *
 *****************************************************************/

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establishContext(ctx, s)
     Csec_context_t *ctx;
     int s;
{  
  return Csec_server_establish_context_ext(ctx, s, NULL, 0);
}

/**
 * API function for the server to establish the context
 * Allows to specify a buffer for a token already received.
 */
int Csec_server_establish_context_ext(ctx, s, buf, len)
     Csec_context_t *ctx;
     int s;
     char *buf;
     int len;
{
  char *func = "Csec_server_establish_context_ext";

  Csec_trace(func, "Server establishing context\n");

  /* Checking the status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }
  
  if (!(ctx->flags & CSEC_CTX_SERVICE_TYPE_SET)) {
    Csec_errmsg(func, "Service type not set");
    serrno  = ESEC_NO_SVC_TYPE;
    return -1;
  }

  /* This will load the list of server supported protocols,
     (possibly depending on client IP address) if they are not already set.
   */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    if (Csec_server_set_protocols(ctx, s) < 0) {
      return -1;
    }
  }

  /*
    Receive information from client, and send back chosen protocol and settings
  */
  if (Csec_server_negociate_protocol(s, CSEC_NET_TIMEOUT, ctx, buf, len) < 0) {
    return -1;
  }

  if ( Csec_get_shlib(ctx) == NULL) {
    /* get_shlib() should have set an error message and number */
    return -1;
  }

  if (Csec_server_set_service_name(ctx, s)) {
    return -1;
  }

  return (*(ctx->Csec_server_establish_context_ext))(ctx, s, NULL, 0);
}


/**
 * API function for client to establish function with the server
 */
int Csec_client_establishContext(ctx, s)
     Csec_context_t *ctx;
     int s;
{
  char *func="Csec_client_establishContext";

  Csec_trace(func, "Client establishing context\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }

  if (!(ctx->flags & CSEC_CTX_SERVICE_TYPE_SET)) {
    serrno =  ESEC_NO_SVC_TYPE;
    Csec_errmsg(func, "Service type not set");
    return -1;
  }
 
  /* If protos not yet loaded (at ctx initiallisation) then get them now  */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    int rc;
    rc = Csec_client_lookup_protocols(&(ctx->total_protocols), &(ctx->nb_total_protocols));
    if (rc != 0) {
      /* Csec_client_lookup_protocols already sets the serrno etc... */
      return rc;
    }
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;    
  }

  if (Csec_client_negociate_protocol(s, CSEC_NET_TIMEOUT, ctx) < 0) {
    /* Error already reported */
    return -1;
  }

  if ( Csec_get_shlib(ctx) == NULL) {
    /* get_shlib() should have set an error message and number */
    return -1;   
  }

  /* Loading up the server service name */
  if (!(ctx->flags & CSEC_CTX_SERVICE_NAME_SET)) {
    if (Csec_client_set_service_name(ctx, s) != 0) {
      /* Error already set in Csec_client_set_service_name */
      return -1;
    }
  }
    
  return (*(ctx->Csec_client_establish_context))(ctx, s);
}


/*****************************************************************
 *                                                               *
 *               SERVICE NAME/MAPPING  FUNCTIONS                *
 *                                                               *
 *****************************************************************/



int Csec_map2id(Csec_context_t *ctx, char *principal, uid_t *uid, gid_t *gid) {
  char *func = "Csec_map2id";
  char username[CA_MAXNAMELEN];

  *uid = *gid = -1;
    
  if (Csec_map2name(ctx, principal, username, CA_MAXNAMELEN)<0) {
    Csec_trace(func, "Could not find mapping for <%s>\n", principal)
      /* Serrno and error buffer already set by Csec_map2name */;
    return -1;
  }

  Csec_trace(func, "Principal <%s> mapped to user <%s>\n", principal, username);

  return Csec_name2id(username, uid, gid);

}

/**
 * Maps the credential to the corresponding name
 */
int Csec_map2name(Csec_context_t *ctx, const char *principal,
                  char *name, int maxnamelen) {
  return (*(ctx->Csec_map2name))(ctx, principal, name, maxnamelen);
}

/**
 * Returns the name of the service for the specified mechanism
 */
int Csec_get_service_name(Csec_context_t *ctx, int service_type, char *host, char *domain,
                          char *service_name, int service_namelen) {

  char *func = "Csec_get_service_name";

  CHECKCTX(ctx,func);
  Csec_trace(func, "Was initialized, calling method\n");
  return (*(ctx->Csec_get_service_name))(ctx, service_type, host, domain,
					 service_name, service_namelen);
}


/**
 * Returns the principal that the peer name should have
 */
int Csec_get_peer_service_name(Csec_context_t *ctx, int s, int service_type, char *service_name, int service_namelen){

  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  int rc;
  struct hostent *hp;
  char *clienthost;
  char *pos;
  char hostname[CA_MAXHOSTNAMELEN+1];
  char domain[CA_MAXHOSTNAMELEN+1];
  char *func = "Csec_get_peer_service_name";

  /* Getting the peer IP address */
  rc = getpeername(s, (struct sockaddr *)&from, &fromlen);
  if (rc < 0) {
    Csec_errmsg(func, "Could not get peer name: %s", strerror(errno));
    return -1;
  }
    
  /* Looking up name */
  hp = Cgethostbyaddr ((char *)(&from.sin_addr),
		       sizeof(struct in_addr), from.sin_family);
  if (hp == NULL)
    clienthost = inet_ntoa (from.sin_addr);
  else
    clienthost = hp->h_name ;

  strncpy(hostname, clienthost, CA_MAXHOSTNAMELEN); 
  hostname[CA_MAXHOSTNAMELEN] = '\0';
    
  pos = strchr(clienthost, '.');
  if (pos==NULL) {
    /*  We don't have the domain */
    if (Cdomainname(domain, sizeof(domain))<0) {
      Csec_errmsg(func, "Could not get domain name: <%s>", sstrerror(serrno));
      serrno = ESEC_SYSTEM;
      return -1;
    }

    rc = Csec_get_service_name(ctx, service_type, clienthost, domain,
			       service_name, service_namelen);
        
  } else {
        
    /* client host contains host and domain */
    if ((pos-clienthost) + 1 < sizeof(hostname)){
      memcpy(hostname, clienthost, (pos-clienthost)); 
      hostname[pos -clienthost] = '\0';
    } else {
      Csec_errmsg(func, "Host buffer too short");
      serrno = ESEC_SYSTEM;
      return -1;
            
    }

    if (strlen(pos)+1 > sizeof(hostname)) {
      Csec_errmsg(func, "Domain buffer too short");
      serrno = ESEC_SYSTEM;
      return -1;
    } else {
      strncpy(domain, pos, sizeof(domain));
    }

    rc =  Csec_get_service_name(ctx, service_type, hostname, domain,
				service_name, service_namelen);
  }

  Csec_trace(func, "Peer service name is %s\n", service_name);
    
  return rc;

}

/**
 * Returns the principal that the service on local machine should have
 */
int Csec_get_local_service_name(Csec_context_t *ctx, 
				int service_type, 
				char *service_name, 
				int service_namelen) {


  int rc;
  char *pos;
  char hostname[CA_MAXHOSTNAMELEN+1];
  char domain[CA_MAXHOSTNAMELEN+1];
  char *func = "Csec_get_local_service_name";

  gethostname(hostname, CA_MAXHOSTNAMELEN);
    
  pos = strchr(hostname, '.');
  if (pos==NULL) {
    /*  We don't have the domain */
    Csec_trace(func, "Have to call Cdomainname\n");
    if ( Cdomainname(domain, sizeof(domain)) <  0) {
      Csec_errmsg(func, "Could not get domain name: <%s>", sstrerror(serrno));
      serrno = ESEC_SYSTEM;
      return -1;
    }

    rc = Csec_get_service_name(ctx, service_type, hostname, domain,
			       service_name, service_namelen);
        
  } else {
        
    /* client host contains host and domain */
    *pos++ = '\0';

    rc =  Csec_get_service_name(ctx, service_type, hostname, pos,
				service_name, service_namelen);
  }

  Csec_trace(func, "%d Local service name is <%s>\n", rc,  service_name);
    
  return rc;

}

/**
 * On a client, sets up the service name, according to the protocol
 * and the server name, looked up using the socket
 */
int Csec_client_set_service_name(Csec_context_t *ctx, 
				 int s) {
  int rc;
  char *func = "Csec_client_set_service_name";

  CHECKCTX(ctx,func);
  rc = Csec_get_peer_service_name(ctx, 
				  s, 
				  (ctx->server_service_type), 
				  (ctx->peer_name), 
				  CA_MAXCSECNAMELEN);
  if (rc == 0) {
    ctx->flags |= CSEC_CTX_SERVICE_NAME_SET;
  } else {
    serrno = ESEC_NO_SVC_NAME;
    Csec_errmsg(func, "Could not set service name !");
  }
  return rc;
}

/**
 * Sets the service type in the context
 */
int Csec_server_set_service_type(Csec_context_t *ctx, int service_type) {
  char *func = "Csec_server_set_service_type";
  CHECKCTX(ctx,func);
  ctx->server_service_type = (service_type & CSEC_SERVICE_TYPE_MASK);
  ctx->flags |= CSEC_CTX_SERVICE_TYPE_SET;
  return 0;
}


int Csec_server_set_service_name(Csec_context_t *ctx, int s) {
  int rc;
  char *func = "Csec_server_set_service_name";

  CHECKCTX(ctx,func);
  rc = Csec_get_local_service_name(ctx, 
				  ctx->server_service_type, 
				  (ctx->local_name), 
				  CA_MAXCSECNAMELEN);

  if (rc == 0) {
    rc = Csec_get_peer_service_name(ctx, 
  				    s, 
				    (ctx->server_service_type), 
				    (ctx->peer_name), 
				    CA_MAXCSECNAMELEN);
  }

  if (rc == 0) {
    ctx->flags |= CSEC_CTX_SERVICE_NAME_SET;
  } else {
    Csec_errmsg(func, "Could not set service name !");
  }

  return rc;

}



/**
 * Returns the Service name on the client side
 */
char *Csec_client_get_service_name(Csec_context_t *ctx) {
  char *func = "Csec_client_get_service_name";

  CHECKCTX_EXT(ctx,func,NULL);
  if (ctx->flags & CSEC_CTX_SERVICE_NAME_SET) {
    return ctx->peer_name;
  } else {
    return NULL;
  }
}


/**
 * Returns the Service name on the server side
 */
char *Csec_server_get_service_name(Csec_context_t *ctx) {
  char *func = "Csec_server_get_service_name";

  CHECKCTX_EXT(ctx,func,NULL);
  if (ctx->flags & CSEC_CTX_SERVICE_NAME_SET) {
    return ctx->local_name;
  } else {
    return NULL;
  }
}

int Csec_client_get_service_type (Csec_context_t *ctx) {
  char *func = "Csec_client_get_service_type";

  return ctx->server_service_type;
}

/**
 * Optionally set flags in the Csec_context_t (client/server)
 * Supported flags:
 *
 *     CSEC_OPT_DELEG_FLAG
 *     CSEC_OPT_NODELEG_FLAG
 */
static int _setSecurityOpts(Csec_context_t *ctx,int opts) {
  char *func = "_setSecurityOpts";

  Csec_trace(func, "Entering\n");

  /* Sanity checks - complain if we already have an established security context */
  if (ctx->flags& CSEC_CTX_CONTEXT_ESTABLISHED) {
    serrno = EINVAL;
    Csec_errmsg(func, "A security context has already been established");
    return -1;
  }

  if ((opts & CSEC_OPT_DELEG_FLAG) && (opts & CSEC_OPT_NODELEG_FLAG)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Cannot set both delegate and no delegate flags");
    return -1;
  }

  if (opts & CSEC_OPT_DELEG_FLAG) {
    Csec_trace(func, "Setting CSEC_OPT_DELEG_FLAG\n");
    ctx->sec_flags |= CSEC_OPT_DELEG_FLAG;
  }

  if (opts & CSEC_OPT_NODELEG_FLAG) {
    Csec_trace(func, "Setting CSEC_OPT_NODELEG_FLAG\n");
    ctx->sec_flags |= CSEC_OPT_NODELEG_FLAG;
  }

  return 0;
}


int Csec_client_setSecurityOpts(Csec_context_t *ctx,int opts) {
  char *func = "Csec_client_setSecurityOpts";

  Csec_trace(func, "Entering\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }

  if (!Csec_context_is_client(ctx)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Not a client context");
    return -1;
  }

  return _setSecurityOpts(ctx, opts);
}

int Csec_server_setSecurityOpts(Csec_context_t *ctx,int opts) {
  char *func = "Csec_server_setSecurityOpts";

  Csec_trace(func, "Entering\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }

  if (Csec_context_is_client(ctx)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Not a server context");
    return -1;
  }

  return _setSecurityOpts(ctx, opts);
}

/**
 * Returns the delegated credential from context to user
 * and removes it from the context
 */
int Csec_server_getDelegatedCredentials(Csec_context_t *ctx, char **mech_name, void **buffer, size_t *length) {
  char *func = "Csec_server_getDelegatedCredentials";

  Csec_trace(func, "Entering\n");

  if (Csec_context_is_client(ctx)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Not a server context");
    return -1;
  }

  if (!(ctx->flags & CSEC_CTX_DELEG_CRED_LOADED)) {
    serrno = EINVAL;
    Csec_errmsg(func, "No delegated credential available");
    return -1;
  }

  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED) || ctx->current_protocol < 0) {
    serrno = EINVAL;
    Csec_errmsg(func, "Unexpected error: Invalid protocol selection found");
    return -1;
  }

  Csec_trace(func, "Returning delegated credential\n");

  if (buffer != NULL) {
    *buffer = ctx->deleg_credentials;
  }

  if (length != NULL) {
    *length = ctx->deleg_credentials_len;
  }

  if (mech_name != NULL) {
    *mech_name = ctx->protocols[ctx->current_protocol].id;
  }

  return 0;
}
  

/**
 * Sets the authorization name in the client's context
 */
int Csec_client_setAuthorizationId(Csec_context_t *ctx, const char *mech, const char *principal) {
  char *func = "Csec_client_setAuthorizationId";

  Csec_trace(func, "Entering\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }

  if (!Csec_context_is_client(ctx)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Not a client context");
    return -1;
  }

  /* Sanity check - complain if we already have an established security context */
  if (ctx->flags& CSEC_CTX_CONTEXT_ESTABLISHED) {
    serrno = EINVAL;
    Csec_errmsg(func, "A security context has already been established");
    return -1;
  }

  if (mech==NULL || strlen(mech) > CA_MAXCSECPROTOLEN) {
    serrno = EINVAL;
    Csec_errmsg(func, "Supplied mech name is invalid");
    return -1;
  }

  if (principal==NULL || strlen(principal) > CA_MAXCSECNAMELEN) {
    serrno = EINVAL;
    Csec_errmsg(func, "Supplied principal is invalid");
    return -1;
  }

  strncpy(ctx->client_authorization_mech, mech, CA_MAXCSECPROTOLEN);
  ctx->client_authorization_mech[CA_MAXCSECPROTOLEN] = '\0';

  strncpy(ctx->client_authorization_id, principal, CA_MAXCSECNAMELEN);
  ctx->client_authorization_id[CA_MAXCSECNAMELEN] = '\0';

  ctx->flags |= CSEC_CTX_AUTHID_AVAIL;

  Csec_trace(func, "Set to: %s %s\n",ctx->client_authorization_mech, ctx->client_authorization_id);

  return 0;
}

/**
 * Gets the authorization name in the server's context
 */
int Csec_server_getAuthorizationId(Csec_context_t *ctx, char **mech, char **principal) {
  char *func = "Csec_server_getAuthorizationId";

  Csec_trace(func, "Entering\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }

  if (Csec_context_is_client(ctx)) {
    serrno = EINVAL;
    Csec_errmsg(func, "Not a server context");
    return -1;
  }

  if (!(ctx->flags & CSEC_CTX_AUTHID_AVAIL)) {
    Csec_errmsg(func,"No authorizationId available");
    serrno = EINVAL;
    return -1;
  }

  if (mech != NULL)
    *mech = ctx->client_authorization_mech;

  if (principal != NULL)
    *principal = ctx->client_authorization_id;

  return 0;
}

/**
 * Returns the principal or DN of the client connecting to the server.
 */
int Csec_server_getClientId(Csec_context_t *ctx, char **mech, char **principal) {
  char *func = "Csec_server_getClientId";

  if (mech != NULL)
    *mech = ctx->protocols[ctx->current_protocol].id;

  if (principal != NULL)
    *principal = ctx->effective_peer_name;

  return 0;
}

int Csec_isIdAService(const char *mech, const char *principal) {
  char *func = "Csec_isIdAService";
  int i;
  int found;
  int use_simple_check;

  if (mech == NULL || principal == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Invalid argument");
    return (-1);
  }

  if (strcmp(mech,"GSI") && strcmp(mech,"KRB5")) {
    serrno = EINVAL;
    Csec_errmsg(func, "Unable to determine if identity is a service for mechanism %s",mech);
    return -1;
  }

  Csec_trace(func, "Client Mech: %s DN: %s\n", mech, principal);

  /* the simple check for methods other than GSI */
  if (strcmp(mech,"GSI")) {
    use_simple_check = 1;
  } else {
    use_simple_check = 0;
  }

  found = 0;
  for (i = 0; *service_table[i].name != '\0'; i++) {
    const char *p1,*p2,*p3,*p4;
    size_t index, name_len, n_dots, n_bad;

    if (use_simple_check) {
      if (strstr(principal, service_table[i].name) != NULL) {
        found++;
        break;
      }
      continue;
    }

    /* the more complicated check, currently for GSI only */

    name_len = strlen(principal);
    if (name_len < 5) continue;
    p1 = NULL;

    for(index=0;index<name_len-4;index++) {
      /* take first common name found */
      if (!strncasecmp(&principal[index], "/CN=", 4)) {
        p1 = &principal[index];
        break;
      }
    }
    if (p1 == NULL) continue;

    p1 += 4;

    /* p1 is the start of the 'name string', ie. the string after /CN= */

    p2 = strstr(p1,"/");
    if (p2 == NULL) p2 = &principal[name_len];

    /* p2 is the first / in the name string, or the end of the string if no more / */

    p3 = NULL;
    p4 = NULL;
    if (p2[0] != '\0' && p2[1] != '\0' ) {
      p3 = strstr(&p2[1],"/");
      p4 = strstr(&p2[1], "=");
    }

    /* p3 is the second /, ie. the / after p2 or NULL if no more / */
    /* p4 is the first = after p2, NULL if none */

    if ((*p2 == '/' && p3 == NULL && p4 == NULL) || (p3 != NULL && p4 != NULL && p4 > p3)) {

      /* **********************************************************
         a first / but no second / and no = after the first /    OR
         = is after second / in the name string:

         eg a dn like:
         /CN=host/a1.abc.com/Email=service@abc.com
         ********************************************************** */

      name_len = strlen(service_table[i].name) - 1; /* ignore the trailing '/' */
      /* check only service name */
      if ((p2 - p1) == name_len && !strncasecmp(p1, service_table[i].name, name_len)) {
        found++;
        break;
      }

      /* not this service */
      continue;

    }

    /* name without service should be understood to be the host service */
    if ( service_table[i].type != CSEC_SERVICE_(HOST) ) continue;

    /* try to see if the name 'looks like' a fqdn */
    name_len = p2 - p1;
    n_dots = 0;
    n_bad = 0;

    for(index=0;index<name_len && !n_bad;index++) {

      if (p1[index] == '.') n_dots++;
      else if (!isalnum(p1[index]) && p1[index] != '-') n_bad++; 
    }

    if (n_bad == 0 && n_dots > 0) {
      found++;
      break;
    }
  }

  if (found) {
    Csec_trace(func, "Client is castor service type: %d\n",service_table[i].type); 
    return (service_table[i].type);
  }
  else return (-1);
}

/* Csec_get_default_context - Returns a pointer to a default per thread context */
Csec_context_t *Csec_get_default_context() {
  struct Csec_api_thread_info *thip;
  
  if (Csec_apiinit (&thip))
    return NULL;

  return &(thip->default_context);
}

/* Csec_mapToLocalUser() maps an ID (mech, principal pair) to username/uid/gid.
 * The routine maintains a short lived cache for the last username looked up,
 * to avoid looking up the same username several times in a row with Csec_name2id()
 */
int Csec_mapToLocalUser(const char *mech, const char *principal, char *username, size_t username_size, uid_t *uid, gid_t *gid) {
  char *func = "Csec_mapToLocalUser";
  Csec_context_t ctx;
  Csec_protocol proto[2];
  char *local_name;
  size_t local_size;
  uid_t local_uid;
  gid_t local_gid;
  typedef struct {
    char username[CA_MAXCSECNAMELEN+1];
    uid_t uid;
    gid_t gid;
    time_t time;
  } simple_cache_t;
  static simple_cache_t *cache=NULL;
  static int cache_lock;

  Csec_trace(func, "Entering. Mech name %s, principal name %s\n",mech,principal);

  if (mech==NULL || strlen(mech) > CA_MAXCSECPROTOLEN) {
    serrno = EINVAL;
    Csec_errmsg(func, "Supplied mech name is invalid");
    return -1;
  }

  if (principal==NULL || strlen(principal) > CA_MAXCSECNAMELEN) {
    serrno = EINVAL;
    Csec_errmsg(func, "Supplied principal is invalid");
    return -1;
  }

  strcpy(proto[0].id, mech);
  proto[1].id[0] = '\0';

  if (Csec_server_initContext(&ctx, CSEC_SERVICE_TYPE_HOST, proto) < 0) {
    return -1;
  }

  if (Csec_setup_protocols_to_offer(&ctx)<0) {
    Csec_clearContext(&ctx);
    return -1;
  }

  ctx.current_protocol = 0;

  if ( Csec_get_shlib(&ctx) == NULL) {
    Csec_clearContext(&ctx);
    return -1;
  }

  if (username != NULL) {
    local_name = username;
    local_size = username_size;
  } else {
    local_name = malloc(CA_MAXCSECNAMELEN+1);
    if (local_name == NULL) {
      Csec_clearContext(&ctx);
      serrno = ENOMEM;
      Csec_errmsg(func, "Unable to make temporary buffer for username");
      return -1;
    }
    local_size = CA_MAXCSECNAMELEN+1;
  }

  if (Csec_map2name(&ctx, principal, local_name, local_size)<0) {
    Csec_errmsg(func, "Could not map principal %s to a local user name!",principal);
    serrno =  ESEC_NO_PRINC;
    goto error;
  }
  Csec_trace(func, "Found user name %s\n",local_name);

  if (uid != NULL || gid != NULL) {
    
    if (Cmutex_lock(&cache_lock, -1)<0) {
      Csec_errmsg(func, "Could not lock cache_lock");
      serrno = ESEC_SYSTEM;
      goto error;
    }

    if (cache == NULL) {
      cache = malloc(sizeof(simple_cache_t));
      if (cache == NULL) {
        Cmutex_unlock(&cache_lock);
        Csec_errmsg(func, "Could not allocate memory for cache");
        serrno = ENOMEM;
        goto error;
      }
      memset(cache,0,sizeof(simple_cache_t));
    }
    
    if (strcmp(cache->username,local_name) || abs(time(NULL) - cache->time)>10) {
      if (Csec_name2id(local_name, &local_uid, &local_gid)<0) {
        Cmutex_unlock(&cache_lock);
        Csec_errmsg(func, "Could not map username %s to uid/gid!",local_name);
        serrno =  ESEC_NO_USER;
        goto error;
      }
      if (strlen(local_name)<=CA_MAXCSECNAMELEN) {
        strcpy(cache->username,local_name);
        (void)time(&cache->time);
        cache->uid = local_uid;
        cache->gid = local_gid;
      }
    } else {
      local_uid = cache->uid;
      local_gid = cache->gid;
    }

    if (Cmutex_unlock(&cache_lock)<0) {
      Csec_errmsg(func, "Could not unlock cache_lock");
      serrno = ESEC_SYSTEM;
      goto error;
    }

    Csec_trace(func, "Found uid %d, gid %d\n",local_uid,local_gid);

    if (uid != NULL)
      *uid = local_uid;

    if (gid != NULL)
      *gid = local_gid;
  }

  Csec_clearContext(&ctx);
  if (username == NULL) free(local_name);
  Csec_trace(func, "Leaving\n");
  return 0;

error:
  if (username == NULL) free(local_name);
  Csec_clearContext(&ctx);
  return -1;
}


/*****************************************************************
 *                                                               *
 *               VOMS FUNCTIONS                                  *
 *                                                               *
 *****************************************************************/



/* Returns the VO name, if it could be retrieved via VOMS */
char *Csec_server_get_client_vo(Csec_context_t *ctx) {
  if (ctx == NULL) return NULL;
  return ctx->voname;
}

char **Csec_server_get_client_fqans(Csec_context_t *ctx, int *nbfqan) {
  if (ctx == NULL) return NULL;
  if (nbfqan != NULL) {
    *nbfqan = ctx->nbfqan;
  }
  return ctx->fqan;
}

