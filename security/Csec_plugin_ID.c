/*
 * $Id: Csec_plugin_ID.c,v 1.9 2005/03/15 22:52:37 bcouturi Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Cauth_api.c - API function used for authentication in CASTOR
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_plugin_ID.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
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
#endif

#include "marshall.h"
#include "serrno.h"
#include "Cnetdb.h"
#include <sys/stat.h>
#include "Cglobals.h"
#include "Cthread_api.h"
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

typedef struct {
  id_creds local_id;
} id_context;


/******************************************************************************/
/* EXPORTED FUNCTIONS */ 
/******************************************************************************/

int Csec_plugin_activate_ID(Csec_plugin_t *plugin) {
  return 0;
}

int Csec_plugin_deactivate_ID(Csec_plugin_t *plugin) {
  return 0;
}

int Csec_plugin_initContext_ID(void **ctx,
			       enum Csec_service_type  svc_type,
			       enum Csec_context_type cxt_type,
			       int options) {
  id_context *tmp = calloc(1, sizeof(id_context));
  *ctx = tmp;
  return 0;
}

int Csec_plugin_clearContext_ID(void *ctx) {
  if (ctx != NULL) free (ctx);
  return 0;
}

/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int Csec_plugin_acquireCreds_ID(void *credentials,
				char *service_name,
				enum Csec_context_type cxt_type) {
  serrno = ENOTSUP;
  return -1;
}

/**
 * API function for the server to establish the context
 *
 */
int Csec_plugin_serverEstablishContextExt_ID(ctx, s, buf, len, client_id)
    void  *ctx;
    int s;
    char *buf;
    int len;
    Csec_id_t **client_id;
{
    csec_buffer_desc recv_tok;
    char *func = "Csec_plugin_serverEstablishContextExt_ID";
    int rc;
    uid_t uid;
    gid_t gid;
    id_creds  *creds;
    char username[CA_MAXUSRNAMELEN+1];
 

    if (ctx == NULL) {
      _Csec_errmsg(func, "ctx was null");
      return -1;
    }

    recv_tok.length = 0;
    if (_Csec_recv_token(s, &recv_tok, CSEC_NET_TIMEOUT, NULL) < 0) {
      _Csec_errmsg(func, "Could not receive token");
      return -1;
    }
  
    _Csec_trace(func, "%s\n", recv_tok.value);
    rc = sscanf(recv_tok.value, "%d %d %14s", &uid, &gid, username);
    if (rc != 3) {
      free(recv_tok.value);
      _Csec_errmsg(func, "Could not read uid and gid");
      return -1;
    }

    free(recv_tok.value);

    creds = (id_creds *) (&((id_context *)ctx)->local_id);

    creds->uid = uid;
    creds->gid = gid;
    strncpy(creds->username, username, CA_MAXUSRNAMELEN);

    if (client_id != NULL) {
      char buf[256];
      sprintf(buf, "%s:%d:%d", creds->username, uid, gid);
      *client_id = _Csec_create_id("ID", buf);
    }
    return 0;
}


#define MSGBUFCSEC 200

/**
 * API function for client to establish function with the server
 */
    int Csec_plugin_clientEstablishContext_ID(ctx, s)
    void *ctx;
    int s;
{

  uid_t uid;
  gid_t gid;
  csec_buffer_desc send_tok;
  char *func = "client_extablish_context";
  char buf[MSGBUFCSEC];
  struct passwd *p;

  _Csec_trace(func, "Entering\n");

  uid = geteuid();
  gid = getegid();

  p = getpwuid(uid);
  if (p == NULL) {
    _Csec_errmsg(func, "Could not look up user");
    return -1;
  }

  snprintf(buf, MSGBUFCSEC, "%d %d %s", uid, gid, p->pw_name);
  _Csec_trace(func, "%s\n", buf);
  
  send_tok.value = malloc(strlen(buf));
  if (send_tok.value == NULL) {
    _Csec_errmsg(func, "malloc: Could not allocate memory");
    return -1;
  }

  strncpy((char *)(send_tok.value), buf, strlen(buf)); 
  send_tok.length = strlen(buf);

  if (_Csec_send_token(s, &send_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
    _Csec_errmsg(func, "Could not send token");
    return -1;
  }
    
  free(send_tok.value);

  return 0;
 }


int Csec_plugin_map2name_ID(Csec_id_t *user_id,
			    Csec_id_t **mapped_id) {
  
  /* XXX protection in case the protocol is wrong ??? */
  char *p = NULL;
  char *username = NULL;
  char *func = "Csec_plugin_map2name_ID";
  
  if (user_id == NULL || mapped_id == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL parameter\n");
    return -1;
  }

  username = strdup(_Csec_id_name(user_id));
  p = strchr(username, ':');
  if (p == NULL) {
    free(username);
    serrno = ESEC_NO_PRINC;
    _Csec_errmsg(func, "Could not map: %s", _Csec_id_name(user_id));
    return -1;
  }

  *p = '\0';
  *mapped_id = _Csec_create_id(USERNAME_MECH, username);
  free(username);

  return 0;
}


int Csec_plugin_servicetype2name_ID(enum  Csec_service_type  svc_type,		
			     char *host,				
			     char *domain,				
			     char *service_name,			
			     int service_namelen ) {
  strncpy(service_name, "ID", service_namelen);
  return 0;
}


int Csec_plugin_getErrorMessage_ID(int *error, char **message) {

  if (error)
    *error = serrno;
  if (message) 
    *message = Csec_getErrorMessage();
  return 0;
}



int Csec_plugin_wrap_ID(void *plugin_context,		
			csec_buffer_t message,		
			csec_buffer_t crypt) {

  char *func = "Csec_plugin_wrap_ID";
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Operation not supported");
  return -1;
}

int Csec_plugin_unwrap_ID(void *plugin_context,		
			  csec_buffer_t crypt,		
			  csec_buffer_t message) {
  char *func = "Csec_plugin_unwrap_ID";
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Operation not supported");
  return -1;
	       
}
 

int Csec_plugin_isIdService_ID (Csec_id_t *id) {
  return -1;
}

int Csec_plugin_exportDelegatedCredentials_ID(void *plugin_context,
					   csec_buffer_t buffer) {
  char *func = "Csec_plugin_exportDelegatedCredentials_ID";
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Operation not supported");
  return -1;

}
