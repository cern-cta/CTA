/*
 * $Id: Csec_api.c,v 1.15 2004/10/22 20:16:09 jdurand Exp $
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
#include <Csec_protocol_policy.h>

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
    {"host", CSEC_SERVICE_TYPE_HOST},
    {"castor-central", CSEC_SERVICE_TYPE_CENTRAL},
    {"castor-disk", CSEC_SERVICE_TYPE_DISK},
    {"castor-tape", CSEC_SERVICE_TYPE_TAPE},
    {"castor-stager", CSEC_SERVICE_TYPE_STAGER},
    {"", 0}
};

/*****************************************************************
 *                                                               *
 *               CONTEXT INITIALIZATION FUNCTIONS                *
 *                                                               *
 *****************************************************************/

/**
 * Initializes the Csec context for a client
 */
int Csec_client_init_context(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {

  char *func="Csec_client_init_context_ext";

  Csec_trace(func, "Initializing client plugin for service type %d\n",
	     service_type);

  memset(ctx, 0, sizeof(Csec_context_t));
  ctx->magic = CSEC_CONTEXT_MAGIC_CLIENT_1;
  ctx->server_service_type = service_type;
  ctx->flags = CSEC_CTX_INITIALIZED|CSEC_CTX_SERVICE_TYPE_SET;
  

  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    return Csec_initialize_protocols_from_list(ctx, protocol);
  } else {
    int rc;
    rc = Csec_client_lookup_protocols(&(ctx->protocols), &(ctx->nb_protocols));
    if (rc == 0) {
      ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
    }
    return rc;
  }
  return 0;
}


/**
 * Check the various credentials
 */
int *Csec_check_creds(Csec_context_t *ctx) {
  int i, current_protocol_save;
  char *func = "Csec_check_creds";

  Csec_trace(func, "Checking credentials\n");

  current_protocol_save = ctx->current_protocol;
  int *result;

  result = calloc(ctx->nb_protocols, sizeof(int));
  if (result == NULL) 
    return NULL;
	
  for (i=0; i<ctx->nb_protocols; i++) {
    ctx->current_protocol = i;
    Csec_trace(func, "Checking credentials for <%s>\n",
	       ctx->protocols[ctx->current_protocol].id);
    if (Csec_get_shlib(ctx) == NULL) {  
      result[i] |= CSEC_PROT_NOSHLIB;
      continue;
    } 
    
    if (!Csec_context_is_client(ctx)) {
      Csec_server_set_service_name(ctx);
    }

    if (Csec_acquire_creds(ctx) != 0) {
      if (serrno == ENOTSUP) {
	result[i] |= CSEC_PROT_NOCHECK;
      } else {
	result[i] |= CSEC_PROT_NOCREDS;
      }
    } else {                                                                                                                    /* Clear the credentials that have been acquired ! */
      if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
	if (ctx->Csec_delete_creds != NULL)
	  (*(ctx->Csec_delete_creds))(ctx);
      }
    } /* else */
    /* Now clear the shared lib */
    Csec_unload_shlib(ctx);
  } /* for */
  ctx->current_protocol =  current_protocol_save;
  return result;
}

/**
 * Initializes the Csec context for the server
 */
int Csec_server_init_context(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {
  char *func="Csec_server_init_context_ext";

  Csec_trace(func, "Initializing server plugin for service type %d\n",
	     service_type);

  memset(ctx, 0, sizeof(Csec_context_t));
  ctx->magic = CSEC_CONTEXT_MAGIC_SERVER_1;
  ctx->server_service_type = service_type;
  ctx->flags = CSEC_CTX_INITIALIZED|CSEC_CTX_SERVICE_TYPE_SET;

  /* Setting the list of protocols from what was passed */
  if (protocol != NULL) {
    return Csec_initialize_protocols_from_list(ctx, protocol);
  }  else {
    int rc;
    rc = Csec_server_lookup_protocols(0, &(ctx->protocols), &(ctx->nb_protocols));
    if (rc == 0) {
      ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
    }
    return rc;
  } 

  return 0;
}

/**
 * Re-initializes the Csec context, clearing the
 * variables first if needed.
 */
int Csec_server_reinit_context(Csec_context_t *ctx, 
			     int service_type,
			     Csec_protocol *protocol) {
  char *func="Csec_server_reinit_context";

  Csec_clear_context(ctx);
  return Csec_server_init_context(ctx, service_type, protocol);;
}

/**
 * Clears the Csec_context_t, deallocating memory.
 */
int Csec_clear_context(Csec_context_t *ctx) {
  char *func = "Csec_clear_context";

  Csec_trace(func, "Clearing context\n");
  if (ctx->magic !=  CSEC_CONTEXT_MAGIC_CLIENT_1
      && ctx->magic !=  CSEC_CONTEXT_MAGIC_SERVER_1) {
    Csec_trace(func, "Bad magic:%xd - Probably uninitialized context !\n");
    return -1;
  }

  if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
    if (ctx->Csec_delete_connection_context != NULL)
      (*(ctx->Csec_delete_connection_context))(ctx);
  }
  if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
    if (ctx->Csec_delete_creds != NULL)
      (*(ctx->Csec_delete_creds))(ctx);
  }
  if (ctx->shhandle != NULL)
    Csec_unload_shlib(ctx);
  if (ctx->protocols != NULL) {
    free(ctx->protocols);
  }

  if (ctx->peer_protocols != NULL) {
    free(ctx->peer_protocols);
  }

  memset(ctx, 0, sizeof(Csec_context_t));

  return 0;
    
}

/**
 * Deletes the security context inside the Csec_context_t
 * 
 * @returns 0 in case of success, -1 otherwise
 */
int Csec_delete_connection_context(ctx)
     Csec_context_t *ctx;
{
  if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
    if (ctx->protocols != NULL) {
      free(ctx->protocols);
      ctx->nb_protocols = 0;
    }
    if (ctx->Csec_delete_connection_context != NULL)
      return (*(ctx->Csec_delete_connection_context))(ctx);
  }
      
  return 0;
}


/**
 * Deletes the credentials inside the Csec_context_t
 */
int Csec_delete_creds(ctx)
     Csec_context_t *ctx;
{
  return (*(ctx->Csec_delete_creds))(ctx);   
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
int Csec_server_establish_context(ctx, s)
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
    Csec_errmsg(func, "Context is NULL\n");
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
  
  if (Csec_server_receive_protocol(s, CSEC_NET_TIMEOUT, ctx, buf, len) < 0) {
    Csec_errmsg(func, "Could not initialize protocol: %s\n", sstrerror(serrno));
    return -1;
  }

  if (Csec_server_set_service_name(ctx)) {
    return -1;
  }

  return (*(ctx->Csec_server_establish_context_ext))(ctx, s, NULL, 0);
}


/**
 * API function for client to establish function with the server
 */
int Csec_client_establish_context(ctx, s)
     Csec_context_t *ctx;
     int s;
{
  char *func="Csec_client_establish_context";

  Csec_trace(func, "Client establishing context\n");

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL\n");
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
 
  /* Checking whether the list of protocols has been loaded */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    int rc;
    rc = Csec_client_lookup_protocols(&(ctx->protocols), &(ctx->nb_protocols));
    if (rc != 0) {
      /* Csec_client_lookup_protocols already sets the serrno etc... */
      return rc;
    }
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;    
  }
 
  /* Checking whther the shared lib is already loaded ! */
  if (!(ctx->flags & CSEC_CTX_SHLIB_LOADED)) {
    if (Csec_get_shlib(ctx) == NULL) {
      return -1;
    }
  }


  /* Loading up the server service name */
  if (!(ctx->flags & CSEC_CTX_SERVICE_NAME_SET)) {
    if (Csec_client_set_service_name(ctx, s) != 0) {
      /* Error already set in Csec_client_set_service_name */
      return -1;
    }
  }

  if (Csec_client_negociate_protocol(s, CSEC_NET_TIMEOUT, ctx) < 0) {
    /* Error already reported */
    return -1;
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
int Csec_map2name(Csec_context_t *ctx, char *principal,
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
    Csec_errmsg(func, "Could not get peer name: %s\n", strerror(errno));
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
 * API function to load the server credentials.
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int Csec_acquire_creds(Csec_context_t *ctx) {
  /* XXX the credentials are not cached, but looked up
     every time for the moment */
  char *func = "Csec_acquire_creds";
  int rc;

  if(Csec_check_shlib_loaded(ctx)<0) {
    return -1;
  }
  
  if (ctx->Csec_acquire_creds != NULL) {
    Csec_trace(func, "Acquiting creds for service %s\n", Csec_server_get_service_name(ctx));
    rc = (*(ctx->Csec_acquire_creds))(ctx,
				      Csec_server_get_service_name(ctx),
				      Csec_context_is_client(ctx));
    if (rc != 0) {
      ctx->flags &= ~CSEC_CTX_CREDENTIALS_LOADED;  
    } else {
      ctx->flags |= CSEC_CTX_CREDENTIALS_LOADED;  
    }    
    return rc;
  } else {
    /* XX What error should be returned here ? */
    return -1;
  }

  return -1;
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
  ctx->server_service_type = service_type;
  ctx->flags |= CSEC_CTX_SERVICE_TYPE_SET;
  return 0;
}



int Csec_server_set_service_name(Csec_context_t *ctx) {
  int rc;
  char *func = "Csec_server_set_service_name";

  CHECKCTX(ctx,func);
  rc = Csec_get_local_service_name(ctx, 
				  ctx->server_service_type, 
				  (ctx->local_name), 
				  CA_MAXCSECNAMELEN);
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
  if(Csec_check_shlib_loaded(ctx)) {
    return NULL;
  }

  if (ctx->flags & CSEC_CTX_SERVICE_NAME_SET) {
    return ctx->local_name;
  } else {
    if (Csec_server_set_service_name(ctx)<0) {
      return NULL;
    } else {
      return ctx->local_name;
    }
  }
}

int Csec_client_get_service_type (Csec_context_t *ctx) {
  char *func = "Csec_client_get_service_type";
  char *p;
  int i;
  int found;

  p = Csec_client_get_service_name(ctx);
  if (p == NULL) return (-1); /* Service name not set */
  found = 0;
  for (i = 0; *service_table[i].name != '\0'; i++) {
    if (strstr(p, service_table[i].name) != NULL) {
      found++;
      break;
    }
  }
  if (found) return (service_table[i].type);
  else return (-1);
}




/**
 * Returns the user name of the client connecting to the server.
 */
char *Csec_server_get_client_username(Csec_context_t *ctx, int *uid, int *gid) {
  char *func = "Csec_server_get_client_username";

  /* Checking whether the mapping has been done */
  if (ctx->flags & CSEC_CTX_USER_MAPPED) {
    if (uid != NULL) 
      *uid = ctx->peer_uid;
    if (gid != NULL)
      *gid = ctx->peer_gid;
    return ctx->peer_username;
  }

  if(Csec_map2name(ctx, 
		   ctx->peer_name, 
		   ctx->peer_username, 
		   CA_MAXCSECNAMELEN)<0) {
    Csec_errmsg(func, "Could not map principal %s to a local user name!",
		ctx->peer_name);
    serrno =  ESEC_NO_PRINC; 
    return NULL;
  }

  if(Csec_name2id(ctx->peer_username, 
		 &(ctx->peer_uid),
		 &(ctx->peer_gid))) {
    Csec_errmsg(func, "Could not map username  %s to uid/gid!",
		ctx->peer_username);
    serrno =  ESEC_NO_USER; 
    return NULL;
  }

     
  ctx->flags |=  CSEC_CTX_USER_MAPPED;
    if (uid != NULL) 
      *uid = ctx->peer_uid;
    if (gid != NULL)
      *gid = ctx->peer_gid;
  return ctx->peer_username;
}

/**
 * Returns the principal or DN of the client connecting to the server.
 */
char *Csec_server_get_client_name(Csec_context_t *ctx) {
  char *func = "Csec_server_get_client_name";
  return ctx->peer_name;
}

int Csec_server_is_castor_service (Csec_context_t *ctx) {
  char *func = "Csec_server_is_castor_service";
  char *p;
  int i;
  int found;

  p = Csec_server_get_client_name(ctx);
  Csec_trace(func, "Client DN: %s\n", p);
  if (p == NULL) return (-1); /* Client name not set */
  found = 0;
  for (i = 0; *service_table[i].name != '\0'; i++) {
    if (strstr(p, service_table[i].name) != NULL) {
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


/**
 * Checks that the plugin has been loaded properly
 */
int Csec_check_shlib_loaded(Csec_context_t *ctx) {
  char *func = "Csec_check_shlib_loaded";

  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }
  
  if (!(ctx->flags & CSEC_CTX_SERVICE_TYPE_SET)) {
    serrno =  ESEC_NO_SVC_TYPE;
    Csec_errmsg(func, "Service type not set");
    return -1;
  }
  
  /* Checking whether the list of protocols has been loaded */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    int rc;
    rc = Csec_client_lookup_protocols(&(ctx->protocols), &(ctx->nb_protocols));
    if (rc != 0) {
      /* Csec_client_lookup_protocols already sets the serrno etc... */
      return rc;
    }
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;    
  }
  
  /* Checking whther the shared lib is already loaded ! */
  if (!(ctx->flags & CSEC_CTX_SHLIB_LOADED)) {
    if (Csec_get_shlib(ctx) == NULL) {
      return -1;
    }
  }
  return 0;

}
