/*
 * $Id: Csec_plugin_GSS.c,v 1.11 2005/03/15 22:52:37 bcouturi Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Cauth_api.c - API function used for authentication in CASTOR
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

#include <sys/stat.h>

#ifndef lint
static char sccsid[] = "@(#)Csec_plugin_GSS.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
#endif

#include "Cglobals.h"
#include "Cthread_api.h"
#include <net.h>

#include "Csec_plugin.h"

#if defined KRB5
#define CURRENT_MECH "KRB5"
#if defined HEIMDAL
#include <gssapi.h>
#else /* HEIMDAL */
#if defined(linux)
#include <gssapi/gssapi_generic.h>
#define GSS_C_NT_USER_NAME (gss_OID)gss_nt_user_name   
#else /* linux */
#include <gssapi/gssapi.h>
#endif /* linux */
#endif /* HEIMDAL */
#else /* KRB5 */
#if defined(GSI)
#define CURRENT_MECH "GSI"
#include <globus_gss_assist.h>
#include <openssl/ssl.h>
#endif /* GSI */
#endif /* KRB5 */

#define TMPBUFSIZE 100

#ifdef GSI
#ifdef with_pthr_suffix
#define MECH _GSI_pthr
#else
#define MECH _GSI
#endif
#else
#define MECH _KRB5
#endif


/******************************************************************************/
/* Structure used used as context for the GSS plugin                          */
/*                                                                            */
/******************************************************************************/


/* Structure containing the GSS context */
typedef struct Csec_plugin_GSS_context {
  enum Csec_service_type  svc_type;
  enum Csec_context_type ctx_type;
  int options;
  char *service_name;
  OM_uint32 context_flags;
  gss_cred_id_t credentials; /*     gss_cred_id_t credentials; */
  gss_ctx_id_t connection_context;     /*     gss_ctx_id_t context; */
  gss_cred_id_t deleg_credentials; /* malloc buffer containing exported credentials from delegation */
} Csec_plugin_GSS_context_t;


/******************************************************************************/
/* Local util functions                                                       */
/*                                                                            */
/******************************************************************************/


static void DLL_DECL
_Csec_process_gssapi_err _PROTO ((char *m, OM_uint32 code,
				  OM_uint32 type));

static int DLL_DECL
_Csec_map_gssapi_err _PROTO ((OM_uint32 maj_stat,
			      OM_uint32 min_stat));

static void DLL_DECL 
_Csec_csec_to_gss _PROTO ((csec_buffer_t, gss_buffer_t));

static void DLL_DECL 
_Csec_gss_to_csec _PROTO ((csec_buffer_t, gss_buffer_t));

static int DLL_DECL 
_Csec_gss_set_peer_service_name _PROTO ((int s, Csec_plugin_GSS_context_t *pctx));

static int activate_lock;


/******************************************************************************/
/* Activation functions                                                       */
/*                                                                            */
/******************************************************************************/

int (CSEC_METHOD_NAME(Csec_plugin_activate, MECH))(ctx)
     Csec_plugin_t *ctx;
{
  char *func =  "Csec_plugin_activate";

#ifdef GSI
  SSL_CTX *ssl_context;
  static int once=0;

  _Csec_trace(func, "Calling globus_module_activate()\n");

  Cthread_mutex_lock(&activate_lock);
  /* Globus module activate/deactiate should be thread safe, but I mutex protect it here anyway */

  (void)globus_module_activate(GLOBUS_GSI_GSS_ASSIST_MODULE);
  (void)globus_module_activate(GLOBUS_GSI_GSSAPI_MODULE);

  if (!once) {
    /* This is because of an initalisation race in libssl, SSL_get_ex_data_X509_STORE_CTX_idx() */
    /* Creating and destroying an ssl_context at this point must be mutex protected */
    ssl_context = SSL_CTX_new(SSLv23_method());
    SSL_CTX_free(ssl_context);
    once++;
  }

  Cthread_mutex_unlock(&activate_lock);
#endif

  return 0;
}

int (CSEC_METHOD_NAME(Csec_plugin_deactivate, MECH))(ctx)
     Csec_plugin_t *ctx;
{
  char *func = "Csec_plugin_deactivate";
  
  _Csec_trace(func, "Entering\n");

#ifdef GSI
  _Csec_trace(func, "Calling globus_module_deactivate()s\n");

  (void)globus_module_deactivate(GLOBUS_GSI_GSSAPI_MODULE);
  (void)globus_module_deactivate(GLOBUS_GSI_GSS_ASSIST_MODULE);
#endif

  return 0;
}


/******************************************************************************/
/* Plugin Initialization functions                                            */
/*                                                                            */
/******************************************************************************/


int (CSEC_METHOD_NAME(Csec_plugin_initContext, MECH))(ctx, svc_type, ctx_type, options)
     void **ctx;
     enum Csec_service_type svc_type;
     enum Csec_context_type ctx_type;
     int options;
{
  char *func = "Csec_plugin_initContext";
  Csec_plugin_GSS_context_t *pctx;
  int rc = 0;

  if (ctx == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL context specified in Csec_plugin_initContext");
    return -1;
  }

  pctx =  (Csec_plugin_GSS_context_t *)calloc(1, sizeof(Csec_plugin_GSS_context_t));
  if (pctx == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory forCsec_plugin_GSS_context_t structure");
    return -1;
  }
  
  pctx->svc_type = svc_type;
  pctx->ctx_type = ctx_type;
  pctx->options = options;
  pctx->service_name = NULL;
  pctx->credentials =  GSS_C_NO_CREDENTIAL;
  pctx->deleg_credentials =  GSS_C_NO_CREDENTIAL;
  pctx->connection_context = GSS_C_NO_CONTEXT;
  *ctx = (void *)pctx;


  /* Now setting up the service name in case of a server context */
  /* In the case of the client context, we will wait until 
     the connection is established */
  if (ctx_type == CSEC_CONTEXT_SERVER) {
    char *host = NULL, *domain = NULL;
    char service_name[CA_MAXCSECNAMELEN +1];
    if(_Csec_get_local_host_domain(&host,
				   &domain) < 0) {
      return -1;
    }
    
    rc = CSEC_METHOD_NAME(Csec_plugin_servicetype2name, MECH)(svc_type,
							      host,
							      domain,
							      service_name,
							      CA_MAXCSECNAMELEN);
    if (rc == 0) {
      pctx->service_name = strdup(service_name);
    }
    
    if (host) free(host);
    if (domain) free (domain);
  }
  
  return rc;

}

int (CSEC_METHOD_NAME(Csec_plugin_clearContext, MECH))(ctx)
     void *ctx;
{
  Csec_plugin_GSS_context_t *pctx;
  OM_uint32 maj_stat, min_stat;
  gss_ctx_id_t *context;
  gss_cred_id_t *creds, *deleg_creds;
  char *func = "Csec_plugin_clearContext";

  if (ctx == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL context passed to %s", func);
    return -1;
  }

  pctx = (Csec_plugin_GSS_context_t *)ctx;
  if (pctx->service_name) free(pctx->service_name);
  context=&(pctx->connection_context);
  creds=&(pctx->credentials);
  deleg_creds = &(pctx->deleg_credentials);

  if (creds != GSS_C_NO_CREDENTIAL) {
    maj_stat = gss_release_cred(&min_stat, creds);
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("releasing credentials", maj_stat, min_stat);
    }
  }


  if (deleg_creds != GSS_C_NO_CREDENTIAL) {
    maj_stat = gss_release_cred(&min_stat, deleg_creds);
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("releasing credentials", maj_stat, min_stat);
    }
  }


  if (context != GSS_C_NO_CONTEXT) {
    maj_stat = gss_delete_sec_context(&min_stat,
				      context,
				      GSS_C_NO_BUFFER);
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("releasing connection context", maj_stat, min_stat);
    }
  }

  return 0;
}


/******************************************************************************/
/* Explicit credentials loading                                               */
/*                                                                            */
/******************************************************************************/



/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int (CSEC_METHOD_NAME(Csec_plugin_acquireCreds, MECH))(credentials, service_name, ctx_type)
     void *credentials;
     char *service_name;
     enum Csec_context_type ctx_type;
{
  gss_buffer_desc name_buf;
  gss_name_t server_name;
  OM_uint32 maj_stat, min_stat;
  char *func = "Csec_plugin_acquireCreds";
  gss_cred_usage_t usage;
  gss_cred_id_t *creds;
  
  if (credentials == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL credentials handle passed to %s", func);
    return -1;
  }
  
  creds = credentials;
  
  /* Set the type of context */
  if (ctx_type == CSEC_CONTEXT_CLIENT) {
    usage = GSS_C_INITIATE;
  } else {
    usage = GSS_C_ACCEPT;
  }
  
  if (service_name == NULL) {
    _Csec_trace(func, "Acquiring default credentials (ctx_type: %d)\n", ctx_type);
    server_name = GSS_C_NO_NAME;
  } else {
    _Csec_trace(func, "Acquiring credentials for <%s> (ctx_type: %d)\n", 
		service_name,
		ctx_type);
    /* Importing the service_name to a gss_buffer_desc */
    name_buf.length = strlen(service_name) + 1;
    name_buf.value = malloc(name_buf.length);
    strncpy(name_buf.value, service_name, strlen(service_name) );
    ((char *)name_buf.value)[name_buf.length -1] = '\0';
    
    maj_stat = gss_import_name(&min_stat, &name_buf,
			       (gss_OID) GSS_C_NT_USER_NAME, &server_name);
    /* Releasing the buffer as it has now been use to create the gss_name_t.
       It has to be cleared in all cases */
    (void) gss_release_buffer(&min_stat, &name_buf);    
    
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("importing name", maj_stat, min_stat);
      return -1;
    }
  }

  maj_stat = gss_acquire_cred(&min_stat, 
			      server_name, 
			      0,
			      GSS_C_NULL_OID_SET,
			      usage,
			      creds, 
			      NULL, 
			      NULL);

  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("acquiring credentials", maj_stat, min_stat);
    return -1;
  }

  if (server_name != GSS_C_NO_NAME) {
    (void) gss_release_name(&min_stat, &server_name);
  }
  
  {
    int maj_stat, min_stat;
    gss_name_t tmpname;
    gss_buffer_desc buf;

    buf.value = NULL;
    maj_stat = gss_inquire_cred(&min_stat,
				*creds,
				&tmpname,
				NULL,
				NULL,
				NULL);

    maj_stat = gss_display_name(&min_stat,
				tmpname,
				&buf,
				NULL);
    _Csec_trace(func, "Successfully acquired credentials for %s\n", (char *)buf.value);

    (void) gss_release_name(&min_stat, &tmpname);
    (void) gss_release_buffer(&min_stat, &buf);



  }

  return 0;
}

/******************************************************************************/
/* Context establishment for the server                                       */
/*                                                                            */
/******************************************************************************/



/**
 * API function for the server to establish the context
 *
 */
int (CSEC_METHOD_NAME(Csec_plugin_serverEstablishContextExt, MECH))(ctx, s, buf, len, client_id)

     void *ctx;
     int s;
     char *buf;
     int len;
     Csec_id_t **client_id;
{
  gss_buffer_desc send_tok, recv_tok;
  csec_buffer_desc csec_tok;
  gss_buffer_t client_disp;
  gss_buffer_desc tmpbuf;
  gss_name_t client;
  gss_OID doid = GSS_C_NO_OID;
  OM_uint32 maj_stat, min_stat, acc_sec_min_stat;
  OM_uint32           time_req;
  OM_uint32 context_flags;
  gss_ctx_id_t *context;
  gss_cred_id_t *server_creds;
  gss_cred_id_t *delegated_cred_handle;
  char *func= "Csec_plugin_serverEstablishContextExt";
  int ext_buf_read = 0;
  Csec_plugin_GSS_context_t *pctx;

  if (ctx == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL context passed to %s", func);
    return -1;
  }

  pctx = (Csec_plugin_GSS_context_t *)ctx;

  /* Have context/credentials point to the Csec_context_t structure */
  context=&(pctx->connection_context);
  
  if (pctx->credentials == NULL) {
    _Csec_trace(func, "Acquiring server credentials\n");

    if ((CSEC_METHOD_NAME(Csec_plugin_acquireCreds, MECH))(&(pctx->credentials), 
							   pctx->service_name,
							   CSEC_CONTEXT_SERVER)) {
      _Csec_errmsg(func, "Could not acquire credentials for mechanism");
      return -1;
    }  
  }

  server_creds=&(pctx->credentials);
  delegated_cred_handle = &(pctx->deleg_credentials);
  *delegated_cred_handle = GSS_C_NO_CREDENTIAL;
  *context = GSS_C_NO_CONTEXT;
  context_flags =  GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG;
  if (pctx->options & CSEC_OPT_DELEG_FLAG) {
    context_flags |= GSS_C_DELEG_FLAG;
  }
  do {

    /* Read the initial buffer if necessary */
    if (!ext_buf_read && buf != NULL && len > 0) {
      ext_buf_read = 1;
      _Csec_trace(func, "Ext buffer read - len: %d\n", len);
      recv_tok.length = len;
      recv_tok.value = (void *)malloc(len);
      if (recv_tok.value == NULL) {
	serrno = ESEC_SYSTEM;
	_Csec_errmsg(func, "Could not allocate space for receive token");
	return -1;
      }
      memcpy(recv_tok.value, buf, len);
    } else {
      _Csec_trace(func, "No ext buffer read\n");
      recv_tok.length = 0;
    }

    _Csec_gss_to_csec(&csec_tok,&recv_tok);
    if (_Csec_recv_token(s, &csec_tok, CSEC_NET_TIMEOUT, NULL) < 0)
      return -1;
    _Csec_csec_to_gss(&csec_tok,&recv_tok);
    
    maj_stat = gss_accept_sec_context(&acc_sec_min_stat,
				      context,
				      *server_creds,
				      &recv_tok,
				      GSS_C_NO_CHANNEL_BINDINGS,
				      &client,
				      &doid,
				      &send_tok,
				      (OM_uint32 *)&(context_flags),
				      &time_req,
				      delegated_cred_handle);

    /* Releasing the recv_tok, it is not needed at this point */
    (void) gss_release_buffer(&min_stat, &recv_tok);

    if (maj_stat!=GSS_S_COMPLETE && maj_stat!=GSS_S_CONTINUE_NEEDED) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("accepting context",
			       maj_stat,
			       acc_sec_min_stat);


      if (*context != GSS_C_NO_CONTEXT)
	gss_delete_sec_context(&min_stat,
			       context,
			       GSS_C_NO_BUFFER);
      return -1;
    }


    if (send_tok.length != 0) {

      _Csec_gss_to_csec(&csec_tok,&send_tok);
      if (_Csec_send_token(s, &csec_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
	serrno = ESEC_SYSTEM;
	_Csec_errmsg(func, "failure sending token");
	return -1;
      }
      _Csec_csec_to_gss(&csec_tok,&send_tok);

      (void) gss_release_buffer(&min_stat, &send_tok);
    } else {
      /* Token has 0 length */
      /* serrno = ESEC_SYSTEM; */
      /*             _Csec_errmsg(func, "Token has 0 length"); */
      /*             return -1; */
      /* BEWARE THIS IS NOT AN ERROR */
    }

    (void) gss_release_buffer(&min_stat, &send_tok);

  } while (maj_stat & GSS_S_CONTINUE_NEEDED);

  client_disp = &tmpbuf;
  maj_stat = gss_display_name(&min_stat, client, client_disp, &doid);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("displaying name", maj_stat, min_stat);
    return -1;
  }

  *client_id = _Csec_create_id(CURRENT_MECH, client_disp->value);
  (void) gss_release_buffer(&min_stat, client_disp);

  maj_stat = gss_release_name(&min_stat, &client);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("releasing name", maj_stat, min_stat);
    return -1;
  }


  {
    int maj_stat, min_stat;
    gss_name_t tmpname = GSS_C_NO_NAME;;
    gss_buffer_desc buf;
   

    if (delegated_cred_handle != GSS_C_NO_CREDENTIAL) {
      buf.value = NULL;
      maj_stat = gss_inquire_cred(&min_stat,
				  *delegated_cred_handle,
				  &tmpname,
				  NULL,
				  NULL,
				  NULL);
    
      maj_stat = gss_display_name(&min_stat,
				  tmpname,
				  &buf,
				  NULL);
      _Csec_trace(func, "Got delegated credentials for %s\n", (char *)buf.value);
     
      (void) gss_release_name(&min_stat, &tmpname);
      (void) gss_release_buffer(&min_stat, &buf);
    }         
  }

  _Csec_trace(func, "Success\n");
   return 0;
}



/******************************************************************************/
/* Context establishment for the client                                       */
/*                                                                            */
/******************************************************************************/



/**
 * API function for client to establish function with the server
 */

int (CSEC_METHOD_NAME(Csec_plugin_clientEstablishContext, MECH))(ctx, s)

     void *ctx;
     int s;
{

  gss_buffer_desc send_tok, recv_tok, *token_ptr;
  csec_buffer_desc csec_tok;
  gss_name_t target_name;
  OM_uint32 maj_stat, min_stat, init_sec_min_stat;
  OM_uint32 in_flags;
  gss_OID oid;
  gss_ctx_id_t *gss_context;
  char *func = "client_establish_context";
  Csec_plugin_GSS_context_t *pctx;

  if (ctx == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL context passed to %s", func);
    return -1;
  }

  pctx = (Csec_plugin_GSS_context_t *)ctx;

  _Csec_trace(func, "Entering\n");
  gss_context = &(pctx->connection_context);
   
  in_flags = GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG;
  if (pctx->options & CSEC_OPT_DELEG_FLAG) {
    in_flags |= GSS_C_DELEG_FLAG;
  }
  /* Choose default mechanism for the library */
  oid = GSS_C_NULL_OID;
  
  /* Make sure the service name is set in the context */
  if (_Csec_gss_set_peer_service_name(s, pctx)<0) {
    return -1;
  }

  /*
   * Import the name into target_name.  Use send_tok to save
   * local variable space.
   */

#ifdef GSI
  target_name = GSS_C_NO_NAME;
#else    
  if (strlen(pctx->service_name) > 0) {
    send_tok.value = (void *)pctx->service_name;
    send_tok.length = strlen(pctx->service_name) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok,
			       (gss_OID) GSS_C_NT_USER_NAME, &target_name);
    
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("parsing name", maj_stat, min_stat);
      return -1;
    }
    
    _Csec_trace(func, "Name parsed:<%s>\n", pctx->service_name);
  } else {
    target_name = GSS_C_NO_NAME;
  }
#endif


  /*
   * Perform the context-establishement loop.
   *
   * On each pass through the loop, token_ptr points to the token
   * to send to the server (or GSS_C_NO_BUFFER on the first pass).
   * Every generated token is stored in send_tok which is then
   * transmitted to the server; every received token is stored in
   * recv_tok, which token_ptr is then set to, to be processed by
   * the next call to gss_init_sec_context.
   *
   * GSS-API guarantees that send_tok's length will be non-zero
   * if and only if the server is expecting another token from us,
   * and that gss_init_sec_context returns GSS_S_CONTINUE_NEEDED if
   * and only if the server has another token to send us.
   */

  token_ptr = GSS_C_NO_BUFFER;
  *gss_context = GSS_C_NO_CONTEXT;
  pctx->context_flags = GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG;

  _Csec_trace(func, "Starting context establishment loop\n");

  do {

    maj_stat = gss_init_sec_context(&init_sec_min_stat,
				    GSS_C_NO_CREDENTIAL,
				    gss_context,
				    target_name,
				    oid,
				    in_flags,
				    0,
				    NULL, /* no channel bindings */
				    token_ptr,
				    NULL, /* ignore mech type */
				    &send_tok,
				    (OM_uint32 *)&(pctx->context_flags),
				    NULL); /* ignore time_rec */

    if (gss_context==NULL) {
      /* XXX */
      serrno = ESEC_SYSTEM;
      _Csec_errmsg(func, "Could not create context.");
      return -1;
    }

    if (token_ptr != GSS_C_NO_BUFFER)
      (void) gss_release_buffer(&min_stat, &recv_tok);

    if (maj_stat!=GSS_S_COMPLETE && maj_stat!=GSS_S_CONTINUE_NEEDED) {
      serrno = _Csec_map_gssapi_err(maj_stat, init_sec_min_stat);
      _Csec_process_gssapi_err("initializing context", maj_stat,
			       init_sec_min_stat);
      (void) gss_release_name(&min_stat, &target_name);
      if (*gss_context != GSS_C_NO_CONTEXT)
	gss_delete_sec_context(&min_stat, gss_context,
			       GSS_C_NO_BUFFER);
      return -1;
    }

    if (send_tok.length > 0) {
      int rc;

      
      _Csec_gss_to_csec(&csec_tok,&send_tok);
      rc = _Csec_send_token(s, &csec_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE);
      _Csec_csec_to_gss(&csec_tok,&send_tok);
      if (rc < 0) {
	(void) gss_release_buffer(&min_stat, &send_tok);
	(void) gss_release_name(&min_stat, &target_name);
	/* XXX */
	serrno = ESEC_SYSTEM;
	/* _Csec_errmsg(func, "error sending token !"); */
	return -1;
      }
    }
    (void) gss_release_buffer(&min_stat, &send_tok);


    if (maj_stat & GSS_S_CONTINUE_NEEDED) {

      recv_tok.length = 0;

      _Csec_gss_to_csec(&csec_tok,&recv_tok);
      if (_Csec_recv_token(s, &csec_tok, CSEC_NET_TIMEOUT, NULL) < 0) {
	(void) gss_release_name(&min_stat, &target_name);
	return -1;
      }
      _Csec_csec_to_gss(&csec_tok,&recv_tok);
      token_ptr = &recv_tok;
    }

  } while (maj_stat == GSS_S_CONTINUE_NEEDED);


#ifdef GSI

  /* In the case of GSI, the server name must be checked AFTER 
     the establishment of the context ! */
    
  /* Now check the server name */
  {
    OM_uint32 maj_stat, min_stat;
    gss_name_t src_name, tgt_name;
    OM_uint32 lifetime, tmpctx;
    gss_OID mech;
    int local, isopen;
    gss_buffer_desc server_name;
    char effective_service_name[CA_MAXCSECNAMELEN+1];
    
    maj_stat = gss_inquire_context(&min_stat,
				   *gss_context,
				   &src_name,
				   &tgt_name,
				   &lifetime,
				   &mech,
				   &tmpctx,
				   &local,
				   &isopen);
      
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err("Error inquiring context",
			       maj_stat,
			       min_stat);
      return -1;
    }
      
    maj_stat = gss_display_name(&min_stat, tgt_name, &server_name, (gss_OID *) NULL);
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err("Error displaying name", maj_stat, min_stat);
      return -1;
    }
      
    strncpy(effective_service_name, server_name.value, CA_MAXCSECNAMELEN);  
       
    (void)gss_release_buffer(&min_stat, &server_name);
    (void)gss_release_name(&min_stat, &tgt_name);
    (void)gss_release_name(&min_stat, &src_name);
      
    if (strstr(effective_service_name, pctx->service_name) == NULL) {
      char errbuf[ERRBUFSIZE+1];
      /* Service name does not match ! */
      snprintf(errbuf,
	       ERRBUFSIZE,
	       "Service name %s does not match expected %s\n",
	       effective_service_name,
	       pctx->service_name);
      _Csec_errmsg(func, errbuf);
      return -1;
    }
    
  }

#endif /* GSI */
      
  (void) gss_release_name(&min_stat, &target_name);

  return 0;
}

/**
 * This function is usefule when a binary is compiled statically with Csec.
 * Then when doing a dlopen on the plugin, this one might not share the
 * same serrno and per thread structures. In this case, this function is useful to
 * import the error message from the plugin environment to the program environment.
 */

int (CSEC_METHOD_NAME(Csec_plugin_getErrorMessage, MECH))(error, message)
     int *error;
     char **message;
{
  if (error)
    *error = serrno;
  if (message) 
    *message = Csec_getErrorMessage();
  return 0;
}

int (CSEC_METHOD_NAME(Csec_plugin_wrap, MECH))(plugin_context, message, crypt)
     void *plugin_context;
     csec_buffer_t message;
     csec_buffer_t crypt;
{
  char *func = "Csec_plugin_wrap";
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Wrap not supported");
  return -1;
}

int (CSEC_METHOD_NAME(Csec_plugin_unwrap, MECH))(plugin_context, crypt, message)
     void *plugin_context;
     csec_buffer_t crypt;
     csec_buffer_t message;
{
  char *func = "Csec_plugin_unwrap";
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Unwrap not supported");
  return -1;
}


int (CSEC_METHOD_NAME(Csec_plugin_exportDelegatedCredentials, MECH))(plugin_context,
								     buffer)
     void *plugin_context;
     csec_buffer_t buffer;
{
  char *func = "Csec_plugin_exportDelegatedCredentials ";
  Csec_plugin_GSS_context_t *pctx;
  OM_uint32 maj_stat, min_stat;
  gss_cred_id_t *deleg_creds;
  gss_buffer_desc tmpbuf;

  if (plugin_context == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL context passed to %s", func);
    return -1;
  }

  pctx = (Csec_plugin_GSS_context_t *)plugin_context;
  deleg_creds = &(pctx->deleg_credentials);

  if (deleg_creds == GSS_C_NO_CREDENTIAL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "Context has no delegated credentials");
    return -1;
  }


  /* The gssapi with krb5 may not have gss_export_cred.
     It was introduced only with V5R2 but may have
     different calling convention from globus
  */

 #ifdef GSI 
  
  maj_stat = gss_export_cred(&min_stat, *deleg_creds, GSS_C_NO_OID, 0, &tmpbuf);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("exporting delegated credential", maj_stat, min_stat);
    return -1;
  }

  buffer->value = tmpbuf.value;
  buffer->length = tmpbuf.length;

  return 0;
#else
    
  serrno = SEOPNOTSUP;
  _Csec_errmsg(func, "Operation not supported");
  return -1;

#endif

}

/******************************************************************************/
/* LOCAL FUNCTIONS IMPLEMENTATION                                             */
/*                                                                            */
/******************************************************************************/


/**
 * Used by _Csec_process_gssapi_err
 * Displays the GSS-API error messages in the error buffer
 */
static void _Csec_display_status_1(m, code, type, buf, bufsize)
     char *m;
     OM_uint32 code;
     int type;
     char *buf;
     int bufsize;
{
  OM_uint32 maj_stat, min_stat;
  gss_buffer_desc msg;
  OM_uint32 msg_ctx;


  msg_ctx = 0;
  while (1) {
    maj_stat = gss_display_status(&min_stat, code,
				  type, GSS_C_NULL_OID,
				  &msg_ctx, &msg);

    snprintf(buf, bufsize, "%s: %s ", m, (char *)msg.value);
    (void) gss_release_buffer(&min_stat, &msg);

    if (!msg_ctx)
      break;
  }
}

/**
 * Function that maps the GSS-API errors to a CASTOR serrno.
 */
static int _Csec_map_gssapi_err(maj_stat, min_stat)
     OM_uint32 maj_stat;
     OM_uint32 min_stat;
{

  int ret_serrno = ESEC_SYSTEM;

  /* Get the routine error number from the major status */
  maj_stat &= 0x00FF0000;
  maj_stat >>= 16;

  switch (maj_stat){
  case GSS_S_NO_CRED:
  case GSS_S_DEFECTIVE_CREDENTIAL:
  case GSS_S_CREDENTIALS_EXPIRED:
    ret_serrno = ESEC_BAD_CREDENTIALS;
    break;
  case GSS_S_NO_CONTEXT:
  case GSS_S_CONTEXT_EXPIRED:
    ret_serrno = ESEC_NO_CONTEXT;
    break;
  default:
    ret_serrno = ESEC_SYSTEM;
  }

  /*  printf("#0x%08x -> %d \n", maj_stat, ret_serrno); */
  return ret_serrno;

}


/**
 * Function to display the GSS-API errors
 */
static void _Csec_process_gssapi_err(msg, maj_stat_code, min_stat_code)
     char *msg;
     OM_uint32 maj_stat_code;
     OM_uint32 min_stat_code;
{

  char errbuf[ERRBUFSIZE];
  char *errbufp;
  char *func = "_Csec_process_gssapi_err";

  errbufp = errbuf;

  _Csec_display_status_1("GSS Error",
			 maj_stat_code,
			 GSS_C_GSS_CODE,
			 errbufp,
			 errbuf + ERRBUFSIZE - errbufp -1 );
  errbufp += strlen(errbufp);

  _Csec_display_status_1("MECH Error",
			 min_stat_code,
			 GSS_C_MECH_CODE,
			 errbufp,
			 errbuf + ERRBUFSIZE - errbufp -1 );

  _Csec_errmsg(msg, errbuf);

  /* serrno =  _Csec_map_gssapi_err(maj_stat, min_stat); */
  /* _Csec_display_status_1(msg, maj_stat, GSS_C_GSS_CODE); */
  /* _Csec_display_status_1(msg, min_stat, GSS_C_MECH_CODE); */
}

static void _Csec_csec_to_gss(csec_buffer_t csec, gss_buffer_t gss) {
  gss->value = csec->value;
  gss->length = csec->length;
}

static void _Csec_gss_to_csec(csec_buffer_t csec, gss_buffer_t gss) {
  csec->value = gss->value;
  csec->length = gss->length;
}



 static int _Csec_gss_set_peer_service_name(int s,
					    Csec_plugin_GSS_context_t *pctx) {

  char *host = NULL, *domain = NULL;
  char service_name[CA_MAXCSECNAMELEN +1];
  int rc;

  if(_Csec_get_peer_host_domain(s,
				&host,
				&domain) < 0) {
    return -1;
  }
    
  rc = CSEC_METHOD_NAME(Csec_plugin_servicetype2name, MECH)(pctx->svc_type,
							    host,
							    domain,
							    service_name,
							    CA_MAXCSECNAMELEN);
  if (rc == 0) {
    pctx->service_name = strdup(service_name);
  }
  
  if (host) free(host);
  if (domain) free (domain);
  return 0;
 }





 
 
