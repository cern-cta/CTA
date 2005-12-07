/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_plugin_ID.c,v $ $Revision: 1.10 $ $Date: 2005/12/07 10:19:21 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

/*
 * Csec_plugin_ID.c - Plugin function used for authentication in CASTOR
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
#include "Cpwd.h"
#include <sys/stat.h>
#include "Cglobals.h"
#include <net.h>
#include <pwd.h>
#include <sys/types.h>

#include "Csec_plugin.h"

#define TMPBUFSIZE 100

typedef struct {
  uid_t uid;
  gid_t gid;
  char username[CA_MAXUSRNAMELEN+1];
} id_creds;


/******************************************************************************/
/* EXPORTED FUNCTIONS */
/******************************************************************************/

int Csec_activate_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{
  return 0;
}

int Csec_deactivate_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{
  return 0;
}

/**
 * Not used.
 */
int Csec_init_context_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{
    return 0;
}


/**
 * Not used.
 */
int Csec_reinit_context_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{
    return 0;
}

/**
 * Deletes the security context inside the Csec_context_t
 */
int Csec_delete_connection_context_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{
    return 0;
}


/**
 * Deletes the credentials inside the Csec_context_t
 */
int Csec_delete_creds_ID(FP, ctx)
    FPARG;
    Csec_context_t *ctx;
{

  if (ctx->credentials != NULL) {
    free(ctx->credentials);
  }
  return 0;
}



/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int Csec_acquire_creds_ID(FP, ctx, service_name, is_client)
    FPARG;
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
int Csec_server_establish_context_ext_ID(FP, ctx, s, buf, len)
    FPARG;
    Csec_context_t *ctx;
    int s;
    char *buf;
    int len;
{
    csec_buffer_desc recv_tok;
    char *func = "server_establish_context";
    int rc;
    uid_t uid;
    gid_t gid;
    id_creds  *creds;
    char username[CA_MAXUSRNAMELEN+1];
 
    recv_tok.length = 0;
    if (_Csec_recv_token(s, &recv_tok, CSEC_NET_TIMEOUT, NULL) < 0) {
      Csec_errmsg(func, "Could not receive token");
      return -1;
    }
  
    Csec_trace(func, "%s\n", recv_tok.value);
    rc = sscanf(recv_tok.value, "%d %d %14s", &uid, &gid, username);
    if (rc != 3) {
      free(recv_tok.value);
      Csec_errmsg(func, "Could not read uid and gid");
      return -1;
    }

    free(recv_tok.value);

    creds = malloc(sizeof(id_creds));
    if (creds == NULL) {
      Csec_errmsg(func, "Could not allocate memory for credentials");
      return -1;
    }

    creds->uid = uid;
    creds->gid = gid;
    strncpy(creds->username, username, CA_MAXUSRNAMELEN);
    strncpy(ctx->effective_peer_name, username, CA_MAXCSECNAMELEN);

    ctx->credentials = creds;
    ctx->flags |= CSEC_CTX_CREDENTIALS_LOADED;

    /* Setting the flag in the context object ! */
    ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
    return 0;
}


#define MSGBUFCSEC 200

/**
 * API function for client to establish function with the server
 */
int Csec_client_establish_context_ID(FP, ctx, s)
    FPARG;
    Csec_context_t *ctx;
    int s;
{

  uid_t uid;
  gid_t gid;
  csec_buffer_desc send_tok;
  char *func = "client_extablish_context";
  char buf[MSGBUFCSEC];
  struct passwd *p;

  Csec_trace(func, "Entering\n");

  uid = geteuid();
  gid = getegid();

  p = Cgetpwuid(uid);
  if (p == NULL) {
    Csec_errmsg(func, "Could not look up user");
    return -1;
  }

  snprintf(buf, MSGBUFCSEC, "%d %d %s", uid, gid, p->pw_name);
  Csec_trace(func, "%s\n", buf);
  
  send_tok.value = malloc(strlen(buf));
  if (send_tok.value == NULL) {
    Csec_errmsg(func, "malloc: Could not allocate memory");
    return -1;
  }

  strncpy((char *)(send_tok.value), buf, strlen(buf)); 
  send_tok.length = strlen(buf);

  if (_Csec_send_token(s, &send_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
    Csec_errmsg(func, "Could not send token");
    return -1;
  }
    
  free(send_tok.value);

  strncpy(ctx->effective_peer_name, ctx->peer_name, CA_MAXCSECNAMELEN);

  /* Setting the flag in the context object ! */
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
  
  return 0;
}


int Csec_get_service_name_ID(FPARG, Csec_context_t *ctx, 
			       int service_type, char *host, char *domain,
			       char *service_name, int service_namelen) {
  strncpy(service_name, "ID", service_namelen);
  return 0;

}


int Csec_map2name_ID(FPARG, Csec_context_t *ctx, const char *principal, char *name, int maxnamelen) {

  strncpy(name, principal, maxnamelen);
  return 0;

}
