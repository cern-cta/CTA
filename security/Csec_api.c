/*
 * $id$
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

#define CHECKCTX(CTX,FUNC) if(check_ctx(CTX, FUNC)<0) return -1;

/**
 * Initializes the Csec the context, and the protocol as well
 */
int Csec_init_context_client(Csec_context *ctx) {
  return Csec_init_context_ext(ctx, 1, 0);
}

/**
 * Initializes the Csec the context, but not the protocol
 */
int Csec_init_context_server(Csec_context *ctx) {
  return Csec_init_context_ext(ctx, 0, 0);
}

/**
 * Initializes the context, and allows to choose whether
 * the protocol/shared library should be as well.

*/
int Csec_init_context_ext(Csec_context *ctx, int init_proto, int reinitialize) {
  char *func="Csec_init_context_ext";
  int rc;
  
  if (reinitialize) {
    if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
      if (ctx->Csec_delete_context != NULL)
	(*(ctx->Csec_delete_context))(ctx);
    }
    if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
      if (ctx->Csec_delete_creds != NULL)
	(*(ctx->Csec_delete_creds))(ctx);
    }
    if (ctx->shhandle != NULL)
      dlclose(ctx->shhandle);
  }
  
  memset(ctx, 0, sizeof(Csec_context));
  ctx->flags = CSEC_CTX_INITIALIZED;
  
  if (init_proto) {
    rc = Csec_client_lookup_protocols(&(ctx->protocols), &(ctx->nb_protocols));
    if (rc != 0) {
      Csec_errmsg(func, "Could not get security protocols");
      return rc;
    }
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
    
    if ( Csec_get_shlib(ctx) == NULL) {
      return -1; 
    } 
  }
  return 0;
}


/**
 * Reinitializes the security context
 */
int Csec_reinit_context_server(ctx)
     Csec_context *ctx;
{
  return  Csec_init_context_ext(ctx, 0, 1);
}

/**
 * Deletes the security context inside the Csec_context
 * 
 * @returns 0 in case of success, -1 otherwise
 */
int Csec_delete_context(ctx)
     Csec_context *ctx;
{
  if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
    if (ctx->protocols != NULL) {
      free(ctx->protocols);
      ctx->nb_protocols = 0;
    }
    if (ctx->Csec_delete_context != NULL)
      return (*(ctx->Csec_delete_context))(ctx);
  }
      
  return 0;
}


/**
 * Deletes the credentials inside the Csec_context
 */
int Csec_delete_creds(ctx)
     Csec_context *ctx;
{
  return (*(ctx->Csec_delete_creds))(ctx);   
}

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establish_context(ctx, s)
     Csec_context *ctx;
     int s;
{
    
  return Csec_server_establish_context_ext(ctx, s, NULL, 0);
}

/**
 * API function for the server to establish the context
 * Allows to specify a buffer for a token already received.
 */
int Csec_server_establish_context_ext(ctx, s, buf, len)
     Csec_context *ctx;
     int s;
     char *buf;
     int len;
{
  char *func = "Csec_server_establish_context_ext";
  char srv_name[CA_MAXSERVICENAMELEN];

  if (!(ctx->flags & CSEC_CTX_SERVICE_TYPE_SET)) {
    Csec_errmsg(func, "Service type not set");
    serrno  = ESEC_NO_SVC_TYPE;
    return -1;
  }
    
  if (Csec_server_receive_protocol(s, CSEC_NET_TIMEOUT, ctx, buf, len) < 0) {
    Csec_errmsg(func, "Could not initialize protocol: %s\n", sstrerror(serrno));
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
int Csec_client_establish_context(ctx, s)
     Csec_context *ctx;
     int s;
{
  char *func="Csec_client_establish_context";

  /* Checking ths status of the context */
  if (ctx == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "Context is NULL\n");
    return -1;
  }
    
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    if (Csec_init_context_client(ctx)<-1) {
      return -1;
    }
  }

 if (!(ctx->flags & CSEC_CTX_SERVICE_NAME_SET)) {
    Csec_errmsg(func, "Service name not set");
    return -1;
  }

  if (Csec_client_negociate_protocol(s, CSEC_NET_TIMEOUT, ctx) < 0) {
    return -1;
  }
    
    
  return (*(ctx->Csec_client_establish_context))(ctx, s);
}


int Csec_map2id(Csec_context *ctx, char *principal, uid_t *uid, gid_t *gid) {
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
int Csec_map2name(Csec_context *ctx, char *principal,
                  char *name, int maxnamelen) {
  return (*(ctx->Csec_map2name))(ctx, principal, name, maxnamelen);
}

/**
 * Returns the name of the service for the specified mechanism
 */
int Csec_get_service_name(Csec_context *ctx, int service_type, char *host, char *domain,
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
int Csec_get_peer_service_name(Csec_context *ctx, int s, int service_type, char *service_name, int service_namelen){

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
int Csec_get_local_service_name(Csec_context *ctx, int s, int service_type, char *service_name, int  service_namelen) {


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
 * This function caches the credentials in the Csec_context object.
 * This function must be called again to refresh the credentials.
 */
int Csec_server_acquire_creds(ctx, service_name)
     Csec_context *ctx;
     char *service_name;
{
  return (*(ctx->Csec_server_acquire_creds))(ctx, service_name);
}


int Csec_client_set_service_name(Csec_context *ctx, 
				 int s,
				 int service_type) {
  int rc;
  char *func = "Csec_client_set_service_name";

  CHECKCTX(ctx,func);
  rc = Csec_get_peer_service_name(ctx, 
				  s, 
				  service_type, 
				  (ctx->peer_name), 
				  CA_MAXCSECNAMELEN);
  if (rc == 0) {
    ctx->flags |= CSEC_CTX_SERVICE_NAME_SET;
  } else {
    Csec_errmsg(func, "Could not set service name !");
  }

  return rc;

}

int Csec_server_set_service_type(Csec_context *ctx, int service_type) {
  char *func = "Csec_server_set_service_type";
  CHECKCTX(ctx,func);
  ctx->server_service_type = service_type;
  ctx->flags |= CSEC_CTX_SERVICE_TYPE_SET;
  return 0;
}



int Csec_server_set_service_name(Csec_context *ctx, 
				 int s) {
  int rc;
  char *func = "Csec_server_set_service_name";

  CHECKCTX(ctx,func);
  rc = Csec_get_local_service_name(ctx, 
				  s, 
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
char *Csec_client_get_service_name(Csec_context *ctx) {
  char *func = "Csec_client_get_service_name";

  CHECKCTX(ctx,func);
  if (ctx->flags & CSEC_CTX_SERVICE_NAME_SET) {
    return ctx->peer_name;
  } else {
    return NULL;
  }
}


/**
 * Returns the Service name on the server side
 */
char *Csec_server_get_service_name(Csec_context *ctx) {
  char *func = "Csec_server_get_service_name";

  CHECKCTX(ctx,func);
  if (ctx->flags & CSEC_CTX_SERVICE_NAME_SET) {
    return ctx->local_name;
  } else {
    return NULL;
  }
}
