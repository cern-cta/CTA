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

char protocols[][PROTID_SIZE] = { "KRB5",
                                  "GSI" ,
                                  "ID"};

/* Macro to initialize one symbol in the context structure */
#define DLSETFUNC(CTX, HDL, SYM) if ((CTX->SYM = dlsym(HDL, #SYM "_impl")) == NULL) { \
                                       Csec_errmsg(func, "Error finding symbol %s: %s\n",    \
                                                   #SYM, dlerror());                           \
                                       CTX->shhandle = NULL;  \
                                       return NULL; }

int check_ctx(Csec_context *ctx, char *func) {
    if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
        Csec_errmsg(func, "Context not initialized");
        serrno = ESEC_CTX_NOT_INITIALIZED;
        return -1;
    }
    return 0;
}

#define CHECKCTX(CTX,FUNC) if(check_ctx(CTX, FUNC)<0) return -1;



/**
 * Gets the shared library corresponding to the context !
 */
void *Csec_get_shlib(Csec_context *ctx) {
    char filename[CA_MAXNAMELEN];
    void *handle;
    char *func = "Csec_get_shlib";
    /* Checking input */
    if (ctx == NULL) {
        serrno = EINVAL;
        return NULL;
    }

    /* Creating the library name */
    snprintf(filename, CA_MAXNAMELEN, "libCsec_plugin_%s.so", ctx->protid);
    Csec_trace(func, "Using shared library <%s> for mechanism <%s>\n",
               filename,
               ctx->protid);
    
    handle = dlopen(filename, RTLD_NOW);

    if (handle == NULL) {
        ctx->shhandle = NULL;
        Csec_trace(func, "Error opening shared library %s: %s\n", filename,
                   dlerror());
        Csec_errmsg(func, "Error opening shared library %s: %s\n", filename,
                    dlerror());
       
        return NULL;
    }

    ctx->shhandle = handle;

    DLSETFUNC(ctx, handle, Csec_init_context);
    DLSETFUNC(ctx, handle, Csec_reinit_context);
    DLSETFUNC(ctx, handle, Csec_delete_context);
    DLSETFUNC(ctx, handle, Csec_delete_credentials);
    DLSETFUNC(ctx, handle, Csec_server_acquire_creds);
       DLSETFUNC(ctx, handle, Csec_server_establish_context_ext);
    DLSETFUNC(ctx, handle, Csec_client_establish_context);
    DLSETFUNC(ctx, handle, Csec_map2name);
    DLSETFUNC(ctx, handle, Csec_get_service_name);

    return handle;
    
}


/**
 * Gets protocol
 */
char *Csec_get_protocol(char *previous_tried) {

    char *p;

    p = getenv (CSEC_MECH);

    if (p == NULL)
        return protocols[0];

    return p;
}

/**
 * Initializes the Csec the context, and the protocol as well
 */
int Csec_init_context(Csec_context *ctx)
{
    return Csec_init_context_ext(ctx, 1);
}

/**
 * Initializes the context, and allows to choose whether
 * the protocol/shared library should be as well.

 */
int Csec_init_context_ext(Csec_context *ctx, int init_proto) {

    memset(ctx, 0, sizeof(Csec_context));
    ctx->flags = CSEC_CTX_INITIALIZED;

    if (init_proto) {
        return Csec_init_protocol(ctx, Csec_get_protocol(NULL));
    }

    return 0;
}


/**
 * Reinitializes the security context
 */
int Csec_reinit_context(ctx)
    Csec_context *ctx;
{

    if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
        if (ctx->Csec_delete_context != NULL)
            (*(ctx->Csec_delete_context))(ctx);
    }

    if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
        if (ctx->Csec_delete_credentials != NULL)
            (*(ctx->Csec_delete_credentials))(ctx);
    }

    if (ctx->shhandle != NULL)
        dlclose(ctx->shhandle);
    ctx->protid[0] = '\0';
    
    memset(ctx, 0, sizeof(Csec_context));
    return 0;
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
        if (ctx->Csec_delete_context != NULL)
            return (*(ctx->Csec_delete_context))(ctx);
    }
      
    return 0;
}


/**
 * Deletes the credentials inside the Csec_context
 */
int Csec_delete_credentials(ctx)
    Csec_context *ctx;
{
    return (*(ctx->Csec_delete_credentials))(ctx);   
}

/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
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

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establish_context(ctx, s, client_name, client_name_size,
                                  ret_flags)
    Csec_context *ctx;
    int s;
    char *client_name;
    int client_name_size;
    U_LONG  *ret_flags;
{
    
    return Csec_server_establish_context_ext(ctx, s, NULL, client_name,
                                             client_name_size, ret_flags,
                                             NULL, 0);
}

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establish_context_ext(ctx, s, service_name, client_name, client_name_size,
                                      ret_flags, buf, len)
    Csec_context *ctx;
    int s;
    char *service_name;
    char *client_name;
    int client_name_size;
    U_LONG  *ret_flags;
    char *buf;
    int len;
{
    char *func = "Csec_server_establish_context_ext";
    char srv_name[CA_MAXSERVICENAMELEN];
    char *real_service_name = service_name;
    
    if (Csec_receive_protocol(s, CSEC_NET_TIMEOUT, ctx, buf, len) < 0) {
        Csec_errmsg(func, "Could not initialize protocol: %s\n", sstrerror(serrno));
        return -1;
    }

    if (service_name == NULL || strlen(service_name) == 0) {
        if (Csec_get_local_service_name(ctx,
                                        s,
                                        CSEC_SERVICE_TYPE_CENTRAL,
                                        srv_name,
                                        sizeof(srv_name))<0) {
            return -1;
        }
        real_service_name = srv_name;
    }

    return (*(ctx->Csec_server_establish_context_ext))(ctx, s,real_service_name , client_name, 
                                                       client_name_size,
                                                       ret_flags, NULL, 0);
}


/**
 * API function for client to establish function with the server
 */
int Csec_client_establish_context(ctx, s, service_name, ret_flags)
    Csec_context *ctx;
    int s;
    const char *service_name;
    U_LONG *ret_flags;
{
    char *func="Csec_client_establish_context";
    
    if (ctx == NULL) {
        serrno = EINVAL;
        Csec_errmsg(func, "Context is NULL\n");
        return -1;
    }
    
    if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
        if (Csec_init_context(ctx)<-1) {
            return -1;
        }
    }

    if (Csec_send_protocol(s, CSEC_NET_TIMEOUT, ctx) < 0) {
        Csec_errmsg(func, "Could not send protocol\n");
        return -1;
    }
    
    
    return (*(ctx->Csec_client_establish_context))(ctx, s, service_name, ret_flags);
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

    CHECKCTX(ctx,func );
    Csec_trace(func, "Was initialized, calling method\n");
    return (*(ctx->Csec_get_service_name))(ctx, service_type, host, domain,
                                           service_name, service_namelen);
}


/**
 * Returns the principal that the peer name should have
 */
int Csec_get_peer_service_name(Csec_context *ctx, int s, int service_type, char *service_name, int service_namelen) {

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
 * Sends the initial buffer to indicate the method
 */
 int Csec_send_protocol(int s, int timeout, Csec_context *ctx) {

     gss_buffer_desc buf;
     char *func = "Csec_send_protocol";
     
     if (ctx == NULL) {
         serrno = EINVAL;
         return -1;
     }
     
     if (!(ctx->flags&CSEC_CTX_INITIALIZED)) {
         serrno = ESEC_NO_CONTEXT;
         return -1;
     }
     
     buf.length = strlen(ctx->protid) + 1;
     buf.value = malloc(buf.length);
     strncpy(buf.value, ctx->protid, buf.length);
     *(((char *)buf.value) + buf.length) = '\0';

     Csec_trace(func, "Sending %d bytes, <%s>\n", buf.length, (char *)buf.value);
     
     return _Csec_send_token(s, &buf, timeout);
     
 }
 

/**
 * Sends the initial buffer to indicate the method
 */
 int Csec_receive_protocol(int s, int timeout, Csec_context *ctx, char *buf, int len) {

     gss_buffer_desc gssbuf;
     char *func = "Csec_receive_protocol";
     char protid[PROTID_SIZE];

     Csec_trace(func, "Entering\n");
     
     if (ctx == NULL) {
         serrno = EINVAL;
         return -1;
     }

     Csec_trace(func, "Context not null\n");
     
     if (!(ctx->flags&CSEC_CTX_INITIALIZED)) {
          Csec_trace(func, "Context not initialized !\n");
          serrno = ESEC_NO_CONTEXT;
         return -1;
     }

     Csec_trace(func, "Context was initialized %p %d\n", buf, len);

     if (len > 0) {
         gssbuf.length = len;
         gssbuf.value = malloc(len);
         if (gssbuf.value == NULL) {
             Csec_errmsg(func, "Could not allocate memory for buffer");
             serrno = ESEC_SYSTEM;
             return -1;
         }
         memcpy(gssbuf.value, buf, len);
     } else {
         gssbuf.length = 0;
         gssbuf.value = NULL;
     }
     
     if (_Csec_recv_token(s, &gssbuf, timeout)<0) {
         Csec_errmsg(func, "Could not read protocol token");
         return -1;
     }
     
     strncpy(protid, gssbuf.value, PROTID_SIZE);
     free(gssbuf.value);
     
     Csec_trace(func, "Received initial token, for protocol <%s>\n",
                protid);

     return Csec_init_protocol(ctx, protid);
     
 }


/**
 * Initializes the security mechanism dependent part of the context
 */
int Csec_init_protocol(ctx, protid)
    Csec_context *ctx;
    char *protid;
{
    strncpy(ctx->protid, protid, PROTID_SIZE);
    if (Csec_get_shlib(ctx)==NULL) {
        return -1;
    }
    return 0;
}




