/*
 * $id$
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

#include <Cmutex.h>
#include <Cglobals.h>
#include <Cthread_api.h>
#include <net.h>

#include <Csec_plugin.h>

#define TMPBUFSIZE 100

/**
 * Functions for exchanging/printing tokens
 */
static void _Csec_process_gssapi_err _PROTO ((char *m, OM_uint32 code,
                                              OM_uint32 type));
static int _Csec_map_gssapi_err _PROTO ((OM_uint32 maj_stat,
                                         OM_uint32 min_stat));




/******************************************************************************/
/* EXPORTED FUNCTIONS */
/******************************************************************************/

/**
 * Initializes the Csec the context.
 * Just sets the area to 0 for the moment.
 */
int Csec_init_context_impl(ctx)
     Csec_context_t *ctx;
{

  memset(ctx, 0, sizeof(Csec_context_t));
  ctx->flags |= CSEC_CTX_INITIALIZED;
  return 0;
}


/**
 * Reinitializes the security context
 */
int Csec_reinit_context_impl(ctx)
     Csec_context_t *ctx;
{

  if (ctx->flags & CSEC_CTX_CONTEXT_ESTABLISHED) {
    Csec_delete_connection_context_impl(ctx);
  }

  if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
    Csec_delete_creds_impl(ctx);
  }

  memset(ctx, 0, sizeof(Csec_context_t));
  return 0;
}

/**
 * Deletes the security context inside the Csec_context_t
 */
int Csec_delete_connection_context_impl(ctx)
     Csec_context_t *ctx;
{
  OM_uint32 maj_stat, min_stat;

  maj_stat = gss_delete_sec_context(&min_stat, &(ctx->connection_context), NULL);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("deleting context", maj_stat, min_stat);
    return -1;
  }

  return 0;
}


/**
 * Deletes the credentials inside the Csec_context_t
 */
int Csec_delete_creds_impl(ctx)
     Csec_context_t *ctx;
{
  OM_uint32 maj_stat, min_stat;

  maj_stat = gss_release_cred(&min_stat, &(ctx->credentials));
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("deleting credentials", maj_stat, min_stat);
    return -1;
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
int Csec_acquire_creds_impl(ctx, service_name, is_client)
     Csec_context_t *ctx;
     char *service_name;
     int is_client;
{
  gss_buffer_desc name_buf;
  gss_name_t server_name;
  OM_uint32 maj_stat, min_stat;
  char *func = "Csec_acquire_creds_impl";
  gss_cred_usage_t usage;
  
  if (is_client) {
    usage = GSS_C_INITIATE;
  } else {
    usage = GSS_C_ACCEPT;
  }

  if (service_name == NULL) {
    Csec_trace(func, "Acquiring default credentials (is_client: %d)\n", is_client);
    server_name = GSS_C_NO_NAME;
  } else {
    Csec_trace(func, "Acquiring credentials for <%s> (is_client: %d)\n", 
	       service_name,
	       is_client);
    /* Importing the service_name to a gss_buffer_desc */
    name_buf.length = strlen(service_name) + 1;
    name_buf.value = malloc(name_buf.length);
    strncpy(name_buf.value, service_name, strlen(service_name) );
    
    maj_stat = gss_import_name(&min_stat, &name_buf,
			       (gss_OID) GSS_C_NT_USER_NAME, &server_name);
    /* Releasing the buffer as it has now been use to create the gss_name_t.
       It has to be cleared in all cases */
    (void) gss_release_buffer(&min_stat, &name_buf);    

    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("importing name", maj_stat, min_stat);
      ctx->flags &= ~CSEC_CTX_CREDENTIALS_LOADED;
      return -1;
    }
  }


  maj_stat = gss_acquire_cred(&min_stat, 
			      server_name, 
			      0,
			      GSS_C_NULL_OID_SET,
			      usage,
			      &(ctx->credentials), NULL, NULL);

  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("acquiring credentials", maj_stat, min_stat);
    ctx->flags &= ~CSEC_CTX_CREDENTIALS_LOADED;
    return -1;
  }

  if (server_name != GSS_C_NO_NAME) {
    (void) gss_release_name(&min_stat, &server_name);
  }
  
  /* Set the flags in the context object */
  ctx->flags |= CSEC_CTX_CREDENTIALS_LOADED;

  Csec_trace(func, "Successfully acquired credentials\n");
  return 0;
}

/**
 * API function for the server to establish the context
 *
 */
int Csec_server_establish_context_ext_impl(ctx, s, buf, len)
     Csec_context_t *ctx;
     int s;
     char *buf;
     int len;
{
  gss_buffer_desc send_tok, recv_tok;
  gss_buffer_t client_disp;
  gss_buffer_desc tmpbuf;
  gss_name_t client;
  gss_OID doid = GSS_C_NO_OID;
  OM_uint32 maj_stat, min_stat, acc_sec_min_stat;
  OM_uint32           time_req;
  gss_cred_id_t       delegated_cred_handle = GSS_C_NO_CREDENTIAL;
  gss_channel_bindings_t  input_chan_bindings = GSS_C_NO_CHANNEL_BINDINGS;
  gss_ctx_id_t *context;
  gss_cred_id_t *server_creds;
  char *func= "server_establish_context";
  int ext_buf_read = 0;

  /* Have context/credentials point to the Csec_context_t structure */
  context=&(ctx->connection_context);

  if (!(ctx->flags&CSEC_CTX_CREDENTIALS_LOADED)) {
    Csec_trace(func, "Acquiring server credentials\n");
    if (strlen(ctx->local_name) == 0) {
      serrno = EINVAL;
      Csec_errmsg(func, "No service name specified to load credentials");
      return -1;
    } else {
      if (Csec_acquire_creds_impl(ctx, ctx->local_name, Csec_context_is_client(ctx))<0) {
	Csec_errmsg(func, "Could no acquire credentials for mechanism");
	return -1;
      }
    }
  }
  server_creds=&(ctx->credentials);

  /*     if (!(ctx->flags&CSEC_CTX_CREDENTIALS_LOADED)) { */
  /*         server_creds=&(ctx->credentials); */
  /*     } else { */
  /*         server_creds = GSS_C_NO_CREDENTIAL; */
  /*     } */
    
  client_disp = &tmpbuf;

  *context = GSS_C_NO_CONTEXT;

  do {

    /* Read the initial buffer if necessary */
    if (!ext_buf_read && buf != NULL && len > 0) {
      ext_buf_read = 1;
      Csec_trace(func, "Ext buffer read - len: %d\n", len);
      recv_tok.length = len;
      recv_tok.value = (void *)malloc(len);
      if (recv_tok.value == NULL) {
	serrno = ESEC_SYSTEM;
	Csec_errmsg(func, "Could not allocate space for receive token");
	return -1;
      }
      memcpy(recv_tok.value, buf, len);
    } else {
      Csec_trace(func, "No ext buffer read\n");
      recv_tok.length = 0;
    }

    if (_Csec_recv_token(s, &recv_tok, CSEC_NET_TIMEOUT, NULL) < 0)
      return -1;

    maj_stat = gss_accept_sec_context(
				      &acc_sec_min_stat,
				      context,
				      *server_creds,
				      &recv_tok,
				      input_chan_bindings,
				      &client,
				      &doid,
				      &send_tok,
				      (OM_uint32 *)&(ctx->context_flags),
				      &time_req,
				      &delegated_cred_handle);

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


      if (_Csec_send_token(s, &send_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
	serrno = ESEC_SYSTEM;
	Csec_errmsg(func, "failure sending token");
	return -1;
      }

      (void) gss_release_buffer(&min_stat, &send_tok);
    } else {
      /* Token has 0 length */
      /* serrno = ESEC_SYSTEM; */
      /*             Csec_errmsg(func, "Token has 0 length"); */
      /*             return -1; */
      /* BEWARE THIS IS NOT AN ERROR */
    }

    (void) gss_release_buffer(&min_stat, &send_tok);

  } while (maj_stat & GSS_S_CONTINUE_NEEDED);


  maj_stat = gss_display_name(&min_stat, client, client_disp, &doid);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("displaying name", maj_stat, min_stat);
    return -1;
  }

  strncpy(ctx->peer_name, client_disp->value, CA_MAXCSECNAMELEN);
  (void) gss_release_buffer(&min_stat, client_disp);


  maj_stat = gss_release_name(&min_stat, &client);
  if (maj_stat != GSS_S_COMPLETE) {
    serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
    _Csec_process_gssapi_err("releasing name", maj_stat, min_stat);
    return -1;
  }

  /* Setting the flag in the context object ! */
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
  return 0;
}


/**
 * API function for client to establish function with the server
 */
int Csec_client_establish_context_impl(ctx, s)
     Csec_context_t *ctx;
     int s;
{

  gss_buffer_desc send_tok, recv_tok, *token_ptr;
  gss_name_t target_name;
  OM_uint32 maj_stat, min_stat, init_sec_min_stat;
  OM_uint32 in_flags;
  gss_OID oid;
  gss_ctx_id_t *gss_context;
  char *func = "client_extablish_context";

  Csec_trace(func, "Entering\n");
  gss_context = &(ctx->connection_context);
  
  /* Set flags */
  in_flags = GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG;

  /* Choose default mechanism for the library */
  oid = GSS_C_NULL_OID;
  
  /*
   * Import the name into target_name.  Use send_tok to save
   * local variable space.
   */
#ifdef GSI
  target_name = GSS_C_NO_NAME;
#else    
  if (strlen(ctx->peer_name) > 0) {
    send_tok.value = (void *)ctx->peer_name;
    send_tok.length = strlen(ctx->peer_name) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok,
			       (gss_OID) GSS_C_NT_USER_NAME, &target_name);
    
    if (maj_stat != GSS_S_COMPLETE) {
      serrno = _Csec_map_gssapi_err(maj_stat, min_stat);
      _Csec_process_gssapi_err("parsing name", maj_stat, min_stat);
      return -1;
    }
    
    Csec_trace(func, "Name parsed:<%s>\n", ctx->peer_name);
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

  Csec_trace(func, "Starting context establishment loop\n");

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
				    (OM_uint32 *)&(ctx->context_flags),
				    NULL); /* ignore time_rec */

    if (gss_context==NULL) {
      /* XXX */
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Could not create context.");
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

      if (_Csec_send_token(s, &send_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
	(void) gss_release_buffer(&min_stat, &send_tok);
	(void) gss_release_name(&min_stat, &target_name);
	/* XXX */
	serrno = ESEC_SYSTEM;
	/* Csec_errmsg(func, "error sending token !"); */
	return -1;
      }
    }
    (void) gss_release_buffer(&min_stat, &send_tok);


    if (maj_stat & GSS_S_CONTINUE_NEEDED) {

      recv_tok.length = 0;

      if (_Csec_recv_token(s, &recv_tok, CSEC_NET_TIMEOUT, NULL) < 0) {
	(void) gss_release_name(&min_stat, &target_name);
	return -1;
      }
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
      
    strncpy(ctx->effective_peer_name, server_name.value, CA_MAXCSECNAMELEN);  
       
    (void)gss_release_buffer(&min_stat, &server_name);
    (void)gss_release_name(&min_stat, &tgt_name);
    (void)gss_release_name(&min_stat, &src_name);
      
    if (strstr(ctx->effective_peer_name, ctx->peer_name) == NULL) {
      char errbuf[ERRBUFSIZE+1];
      /* Service name does not match ! */
      snprintf(errbuf,
	       ERRBUFSIZE,
	       "Service name %s does not match expected %s\n",
	       ctx->effective_peer_name,
	       ctx->peer_name);
      Csec_errmsg(func, errbuf);
      return -1;
    }
      
  }

#endif /* GSI */
      
  (void) gss_release_name(&min_stat, &target_name);

  /* Setting the flag in the context object ! */
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;

  return 0;
}






/******************************************************************************/
/* LOCAL FUNCTIONS */
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

  Csec_errmsg(msg, errbuf);

  /* serrno =  _Csec_map_gssapi_err(maj_stat, min_stat); */
  /* _Csec_display_status_1(msg, maj_stat, GSS_C_GSS_CODE); */
  /* _Csec_display_status_1(msg, min_stat, GSS_C_MECH_CODE); */
}
