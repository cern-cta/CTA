/*
 * $Id: Csec_plugin_KRB4.c,v 1.10 2004/10/22 20:15:41 jdurand Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Cauth_api.c - API function used for authentication in CASTOR
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_plugin_KRB4.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
#endif


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
#include <arpa/inet.h>
#endif

#include "marshall.h"
#include "serrno.h"
#include "Cnetdb.h"
#include <sys/stat.h>
#include <Cmutex.h>
#include <Cglobals.h>
#include <Cthread_api.h>
#include <net.h>
#include <pwd.h>
#include <sys/types.h>

#include <krb.h>
#include <Csec_plugin.h>

#define TMPBUFSIZE 100

#define SRVTAB "/etc/srvtab"
char *srvtab=SRVTAB;
char KRB4_service_prefix[][20] = { "rcmd",
                                   "rcmd",
                                   "rcmd",
                                   "" };

typedef struct {
    char realm[REALM_SZ];
    char local_host[CA_MAXHOSTNAMELEN + 1];
    char remote_host[CA_MAXHOSTNAMELEN + 1];
	KTEXT_ST ticket;
    CREDENTIALS cred;
    AUTH_DAT auth_data;
    Key_schedule schedule;
    MSG_DAT msg_data;
} krb4_context;


typedef struct {
    char local_host[CA_MAXHOSTNAMELEN + 1];
    char remote_host[CA_MAXHOSTNAMELEN + 1];
	KTEXT_ST ticket;
    AUTH_DAT auth_data;
    Key_schedule schedule;
    char version[9];
    char client_creds[ANAME_SZ + REALM_SZ + 2];
    char client_name[ANAME_SZ];
} krb4_srv_context;



int get_local_ticket(char *tkfile, char *myname, char *realm);

/******************************************************************************/
/* EXPORTED FUNCTIONS */
/******************************************************************************/

/**
 * Initializes the Csec the context.
 * Just sets the area to 0 for the moment.
 */
int Csec_init_context_KRB4(ctx)
    Csec_context_t *ctx;
{

    memset(ctx, 0, sizeof(Csec_context_t));
    ctx->flags = CSEC_CTX_INITIALIZED;
    return 0;
}


/**
 * Reinitializes the security context
 */
int Csec_reinit_context_KRB4(ctx)
    Csec_context_t *ctx;
{

    if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
        Csec_delete_connection_context_KRB4(ctx);
    }

    if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
        Csec_delete_creds_KRB4(ctx);
    }

    memset(ctx, 0, sizeof(Csec_context_t));
    return 0;
}

/**
 * Deletes the security context inside the Csec_context_t
 */
int Csec_delete_connection_context_KRB4(ctx)
    Csec_context_t *ctx;
{
    free(ctx->connection_context);
    return 0;
}


/**
 * Deletes the credentials inside the Csec_context_t
 */
int Csec_delete_creds_KRB4(ctx)
    Csec_context_t *ctx;
{
  return 0;
}



/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int Csec_acquire_creds_KRB4(ctx, service_name, is_client)
    Csec_context_t *ctx;
    char *service_name;
    int is_client;
{
 
  serrno = ENOTSUP;
  return -1;
}

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establish_context_ext_KRB4(ctx, s, buf, len)
    Csec_context_t *ctx;
    int s;
    char *buf;
    int len;
{
    char *func = "server_establish_context";
    struct sockaddr_in client, server;
    socklen_t slen = sizeof(struct sockaddr_in);
    struct hostent *hp;
    int rc;
    char *clienthost, *c;
    long authopts;
    char instance[INST_SZ];
    int status;
    krb4_srv_context *kctx;
    
    kctx = malloc(sizeof(krb4_srv_context));
    if (kctx == NULL) {
        Csec_errmsg(func, "Could not allocate memory for krb4_src_context");
        return -1;
    }
   
    /* Getting host name and IP */
    if (gethostname(kctx->local_host, CA_MAXHOSTNAMELEN)<0) {
        Csec_errmsg(func, "gethostname error: %s\n", strerror(errno));
        return -1;;
    }
    if ((c=strchr(kctx->local_host,'.')) != NULL) {
        *c=0;		/* 'name' part only */
    }

	if (getsockname(s, (struct sockaddr *)&server, &slen) < 0) {
		Csec_errmsg(func, "Couldn't do getsockname");
		return -1;
	}

    
    /* Getting the peer IP address */
    rc = getpeername(s, (struct sockaddr *)&client, &slen);
    if (rc < 0) {
        Csec_errmsg(func, "Could not get peer name: %s\n", strerror(errno));
        return -1;
    }
    
    /* Looking up PEER name */
    hp = gethostbyaddr ((char *)(&client.sin_addr),
                        sizeof(struct in_addr), client.sin_family);
    if (hp == NULL)
      clienthost = (char *)inet_ntoa (client.sin_addr);
    else
        clienthost = hp->h_name ;
    
    strncpy(kctx->remote_host, clienthost, CA_MAXHOSTNAMELEN); 
    if ((c=strchr(kctx->remote_host,'.'))!= NULL) {
        *c=0;		/* 'name' part only */
    }

   (void) strcpy(instance, "*");
   authopts = KOPT_DO_MUTUAL;
   status = krb_recvauth(authopts,
                         s,
                         &(kctx->ticket),
                         "rcmd",
                         instance,
                         &client,
                         &server,
                         &(kctx->auth_data),
                         "",
                         kctx->schedule,
                         kctx->version);

    if (status != KSUCCESS) {
        Csec_errmsg(func, "Kerberos error(peer %s):",
                inet_ntoa(client.sin_addr));
        
        Csec_errmsg(func, "%s\n", krb_err_txt[status]);
        return -1;
    } 

    if ((status = krb_kntoln(&(kctx->auth_data), kctx->client_name))
        != KSUCCESS) {
        Csec_errmsg(func, "Could not get client name: %s\n",  krb_err_txt[status]);
        return -1;
    }

    snprintf(kctx->client_creds,
             ANAME_SZ + REALM_SZ + 1,
             "%s@%s",
             kctx->auth_data.pname,
             kctx->auth_data.prealm);
    
    Csec_trace(func, "Client name is <%s>\n", kctx->client_name);
    Csec_trace(func, "Credentials are <%s>\n", kctx->client_creds);

    strncpy(ctx->peer_name, kctx->client_creds , CA_MAXCSECNAMELEN);
    ctx->connection_context = kctx;
    
    /* Setting the flag in the context object ! */
    ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
    return 0;
}


#define MSGBUFCSEC 200

/**
 * API function for client to establish function with the server
 */
int Csec_client_establish_context_KRB4(ctx, s)
    Csec_context_t *ctx;
    int s;
{

    char *func = "client_extablish_context";
    struct sockaddr_in client, server;
    socklen_t len = sizeof(struct sockaddr_in);
    struct hostent *hp;
    int rc;
    char *clienthost, *c;
    long authopts;    
    long cksum = (long) time(0);
    krb4_context *kctx;

    kctx = malloc(sizeof(krb4_context));
    if (kctx == NULL) {
        Csec_errmsg(func, "Could not allocate memory for krb4_context");
        return -1;
    }
    
    /* Getting the REALM */
    if ((rc=krb_get_lrealm(kctx->realm,1))!=KSUCCESS) {
        Csec_errmsg(func, "Cannot obtain local realm: %s\n",
                krb_err_txt[rc]);
        return -1;
    }
    Csec_trace(func, "Realm is: %s\n", kctx->realm);

    /* Getting the client hostname and IP */
    if (gethostname(kctx->local_host, CA_MAXHOSTNAMELEN)<0) {
        Csec_errmsg(func, "gethostname error: %s\n", strerror(errno));
        return -1;;
    }
    if ((c=strchr(kctx->local_host,'.')) != NULL) {
        *c=0;		/* 'name' part only */
    }
    if (getsockname(s, (struct sockaddr *)&client, &len) < 0) {
		Csec_errmsg(func, "Couldn't do getsocknam");
		return -1;
	}
    

    /* Getting the peer IP address and HOSTNAME*/
    rc = getpeername(s, (struct sockaddr *)&server, &len);
    if (rc < 0) {
        Csec_errmsg(func, "Could not get peer name: %s\n", strerror(errno));
        return -1;
    }
    
    hp = gethostbyaddr ((char *)(&server.sin_addr),
                         sizeof(struct in_addr), server.sin_family);
    if (hp == NULL)
      clienthost = (char *)inet_ntoa (server.sin_addr);
    else
        clienthost = hp->h_name ;
    
    strncpy(kctx->remote_host, clienthost, CA_MAXHOSTNAMELEN); 
    if ((c=strchr(kctx->remote_host,'.')) != NULL) {
        *c=0;		/* 'name' part only */
    }

    Csec_trace(func, "Client: <%s> Server: <%s>\n",
           kctx->local_host,
           kctx->remote_host);
    
    /* Building the ticket */
    rc=krb_mk_req(&(kctx->ticket),
                  "rcmd",
                  kctx->remote_host,
                  kctx->realm,
                  cksum);
    
    if (rc != KSUCCESS) {
        Csec_errmsg(func,
                "Cannot authenticate to server %s: %s\n",
                kctx->remote_host, krb_err_txt[rc]);
	}


    Csec_trace(func, "Doing sendauth - Ticket build successfully\n");
    
    authopts = KOPT_DO_MUTUAL+KOPT_DONT_MK_REQ;
    rc = krb_sendauth(authopts,
                      s,
                      &(kctx->ticket),
                      "rcmd",
                      kctx->remote_host,
                      NULL,
                      cksum,
                      &(kctx->msg_data),
                      &kctx->cred,
                      kctx->schedule,
                      &client,
                      &server,
                      "VERSION9");
    
    if (rc != KSUCCESS) {
        Csec_errmsg(func, "cannot authenticate to %s: %s\n",
                kctx->remote_host, krb_err_txt[rc]);
        return -1;
    }  

    ctx->connection_context = kctx;
    
    /* Setting the flag in the context object ! */
    ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
  
    return 0;
}


int Csec_get_service_name_KRB4(Csec_context_t *ctx, 
			       int service_type, char *host, char *domain,
			       char *service_name, int service_namelen) {

    char *func = "Csec_get_service_name_KRB4";
    int rc;
    
    
    Csec_trace(func, "Type: %d, host:<%s> domain:<%s> (%p,%d)\n",
               service_type,
               host,
               domain,
               service_name,
               service_namelen);
    
    if (service_type < 0 ||  service_type >= CSEC_SERVICE_TYPE_MAX
        || service_name == NULL || service_namelen <= 0) {
        serrno = EINVAL;
        return -1;
    }

    if (domain[0] == '.') {
      rc = snprintf(service_name, service_namelen, "%s.%s%s",
		    KRB4_service_prefix[service_type],
		    host,
		    domain);

    } else {
      rc = snprintf(service_name, service_namelen, "%s.%s.%s",
		    KRB4_service_prefix[service_type],
		    host,
		    domain);
    }

    Csec_trace(func,"derived service name:<%s>\n", service_name);
    
    if (rc < 0) {
        serrno = E2BIG;
        return -1;
    }

    return 0;
  

  return 0;

}


#define SEP '@'

int Csec_map2name_KRB4(Csec_context_t *ctx, char *principal, char *name, int maxnamelen) {
    char *p;
    
    p = strchr(principal, SEP);
    if (p== NULL) {
        strncpy(name, principal, maxnamelen);
    } else {
        size_t pos = (p - principal);
        memcpy(name, principal, pos);
        name[pos] = '\0';
    }
    
    return 0;
    
}

