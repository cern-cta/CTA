/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_plugin_GSS.c,v $ $Revision: 1.13 $ $Date: 2005/12/07 13:33:17 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

/*
 * Csec_plugin_GSS.c - Plugin function used for authentication in CASTOR
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

#include "Cglobals.h"
#include "Cmutex.h"
#include <net.h>

#include "Csec_plugin.h"

#if defined KRB5
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
#include <globus_gss_assist.h>
#include <globus_gsi_proxy.h>
#include "openssl/ssl.h"
#if defined(USE_VOMS)
#include "gssapi_openssl.h"
#include "globus_gsi_credential.h"
#include "voms_apic.h"
#endif
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

/* To be checked: definition of GLOBUS_NULL */
#ifndef  GLOBUS_NULL
#define GLOBUS_NULL	NULL
#endif

/**
 * Functions for exchanging/printing tokens
 */
static void _Csec_process_gssapi_err _PROTO ((FPARG, char *m, OM_uint32 code,
                                              OM_uint32 type));
static int _Csec_map_gssapi_err _PROTO ((OM_uint32 maj_stat,
                                         OM_uint32 min_stat));
static int _Csec_delete_deleg_creds _PROTO ((Csec_context_t *));

static void DLL_DECL _Csec_csec_to_gss _PROTO ((csec_buffer_t, gss_buffer_t));

static void DLL_DECL _Csec_gss_to_csec _PROTO ((csec_buffer_t, gss_buffer_t));

static int DLL_DECL _Csec_make_target_name _PROTO ((FPARG, const char *server_dn, gss_name_t *target_name_P));

static int DLL_DECL _Csec_get_voms_creds _PROTO ((Csec_context_t *ctx, 
						  gss_ctx_id_t context_handle));


/**
 * Locks
 */

static int activate_lock;

/******************************************************************************/
/* EXPORTED FUNCTIONS */
/******************************************************************************/

int (CSEC_METHOD_NAME(Csec_activate, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
#ifdef GSI
  char *func = "Csec_activate_GSI";
#else
  char *func = "Csec_activate_KRB5";
#endif

#ifdef GSI
  SSL_CTX *ssl_context;
  static int once=0;

  Csec_trace(func, "Calling globus_module_activate()s\n");

  Cmutex_lock(&activate_lock, -1);
  /* Globus module activate/deactiate should be thread safe, but I mutex protect it here anyway */

  (void)globus_module_activate(GLOBUS_GSI_GSS_ASSIST_MODULE);
  (void)globus_module_activate(GLOBUS_GSI_GSSAPI_MODULE);
  (void)globus_module_activate(GLOBUS_GSI_CREDENTIAL_MODULE);

  if (!once) {
    /* This is because of an initalisation race in libssl, SSL_get_ex_data_X509_STORE_CTX_idx() */
    /* Creating and destroying an ssl_context at this point must be mutex protected */
    ssl_context = SSL_CTX_new(SSLv23_method());
    SSL_CTX_free(ssl_context);
    once++;
  }

  Cmutex_unlock(&activate_lock);
#endif

  return 0;
}

int (CSEC_METHOD_NAME(Csec_deactivate, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
#ifdef GSI
  char *func = "Csec_deactivate_GSI";
#else
  char *func = "Csec_deactivate_KRB5";
#endif

#ifdef GSI
  Csec_trace(func, "Calling globus_module_deactivate()s\n");

  (void)globus_module_deactivate(GLOBUS_GSI_CREDENTIAL_MODULE);
  (void)globus_module_deactivate(GLOBUS_GSI_GSSAPI_MODULE);
  (void)globus_module_deactivate(GLOBUS_GSI_GSS_ASSIST_MODULE);
#endif

  return 0;
}


/**
 * Not used.
 */

int (CSEC_METHOD_NAME(Csec_init_context, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
  return 0;
}


/**
 * Not used.
 */
int (CSEC_METHOD_NAME(Csec_reinit_context, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
  return 0;
}

/**
 * Deletes the security context inside the Csec_context_t
 */
int (CSEC_METHOD_NAME(Csec_delete_connection_context, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
  OM_uint32 maj_stat, min_stat;

  maj_stat = gss_delete_sec_context(&min_stat, &(ctx->connection_context), NULL);
  if (maj_stat != GSS_S_COMPLETE) {
    _Csec_process_gssapi_err(FP, "deleting context", maj_stat, min_stat);
    return -1;
  }

  return 0;
}

/**
 * Deletes only the (exported) delegated credential
 * This routine will clear the corresponding flag for delegated
 * credential in the Csec_context_t.
 */
static int _Csec_delete_deleg_creds(Csec_context_t *ctx)
{
  OM_uint32 maj_stat, min_stat;

  if (ctx->flags & CSEC_CTX_DELEG_CRED_LOADED) {
    free(ctx->deleg_credentials);
    ctx->flags &= ~CSEC_CTX_DELEG_CRED_LOADED;
  }

  return 0;
}


/**
 * Deletes the credentials inside the Csec_context_t
 * (including any delegated credentials still inside the context)
 */
int (CSEC_METHOD_NAME(Csec_delete_creds, MECH))(FP,ctx)
     FPARG;
     Csec_context_t *ctx;
{
  OM_uint32 maj_stat, min_stat;

  if (ctx->flags & CSEC_CTX_CREDENTIALS_LOADED) {
    maj_stat = gss_release_cred(&min_stat, &(ctx->credentials));
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "deleting credentials", maj_stat, min_stat);
      return -1;
    }
  }

  /* and any delegated credential */
  return _Csec_delete_deleg_creds(ctx);
}



/**
 * API function to load the server credentials.
 * It is stored in a thread specific variable
 *
 * This function caches the credentials in the Csec_context_t object.
 * This function must be called again to refresh the credentials.
 */
int (CSEC_METHOD_NAME(Csec_acquire_creds, MECH))(FP, ctx, service_name, is_client)
     FPARG;
     Csec_context_t *ctx;
     char *service_name;
     int is_client;
{
  gss_name_t server_name = GSS_C_NO_NAME;
  OM_uint32 maj_stat, min_stat;
  int ret = -1;
  gss_cred_id_t *cred_handle_P = GLOBUS_NULL;
  int save_errno;

#ifdef GSI
  char *func = "Csec_acquire_creds_GSI";
#else
  char *func = "Csec_acquire_creds_KRB5";
#endif
  gss_cred_usage_t usage;

  if (is_client) {
    usage = GSS_C_INITIATE;
  } else {
    usage = GSS_C_ACCEPT;
  }

  cred_handle_P = &(ctx->credentials);
  *cred_handle_P = GSS_C_NO_CREDENTIAL;

  if (service_name == NULL) {
    Csec_trace(func, "Acquiring default credentials (is_client: %d)\n", is_client);
  } else {
    gss_buffer_desc name_buf;

    Csec_trace(func, "Acquiring credentials for <%s> (is_client: %d)\n", 
	       service_name,
	       is_client);

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
      _Csec_process_gssapi_err(FP, "importing name", maj_stat, min_stat);
      goto error;
    }
  }

  maj_stat = gss_acquire_cred(&min_stat, 
			      server_name, 
			      0,
			      GSS_C_NULL_OID_SET,
			      usage,
			      cred_handle_P, NULL, NULL);

  if (maj_stat != GSS_S_COMPLETE) {

#ifdef GSI

    /* With GSI a failure to acquire credentials (eg. due to expired credentials) gives error
       GSS_S_FAILURE (rather than a more specific error, such as GSS_S_CREDENTIALS_EXPIRED)
       To help, in the case of GSS_S_FAILURE, we check back through the chain of errors
       to see if there are any from the credential module.
       If there are some errors we try to reset the major error to something more specific,
       which in turn will allow a more revelant error code to be set for the user.
    */

    if ((maj_stat & (0xffL << GSS_C_ROUTINE_ERROR_OFFSET)) == GSS_S_FAILURE && min_stat != GLOBUS_SUCCESS) {
      globus_object_t *error_obj = globus_error_get((globus_result_t) min_stat);
      globus_object_t *curr_error = error_obj;
      int replace_error=0;
      OM_uint32 new_error;

      Csec_trace(func, "gss_acquire_cred gave error GSS_S_FAILURE, checking for specific errors from credential module\n");

      do {
        if(globus_object_get_type(curr_error) == GLOBUS_ERROR_TYPE_GLOBUS) {
          globus_module_descriptor_t *source_module = globus_error_get_source(curr_error);
          int error_type = globus_error_get_type(curr_error);
          if (source_module == GLOBUS_GSI_CREDENTIAL_MODULE) {
            Csec_trace(func, "The credential module reported an error type: %d\n",error_type);
            switch(error_type) {

              case GLOBUS_GSI_CRED_ERROR_WITH_CRED_CERT_CHAIN:
              case GLOBUS_GSI_CRED_ERROR_WITH_CRED_PRIVATE_KEY:
              case GLOBUS_GSI_CRED_ERROR_WITH_CRED_CERT:
              case GLOBUS_GSI_CRED_ERROR_VERIFYING_CRED:
              case GLOBUS_GSI_CRED_ERROR_CHECKING_PROXY:
              case GLOBUS_GSI_CRED_ERROR_WITH_CRED:
              case GLOBUS_GSI_CRED_ERROR_SUBJECT_CMP:
                replace_error++;
                new_error = GSS_S_DEFECTIVE_CREDENTIAL;
                break;

              case GLOBUS_GSI_CRED_ERROR_READING_PROXY_CRED:
              case GLOBUS_GSI_CRED_ERROR_READING_HOST_CRED:
              case GLOBUS_GSI_CRED_ERROR_READING_SERVICE_CRED:
              case GLOBUS_GSI_CRED_ERROR_READING_CRED:
              case GLOBUS_GSI_CRED_ERROR_NO_CRED_FOUND:
                replace_error++;
                new_error = GSS_S_NO_CRED;
                break;

              default:
                break;
            }
          }
        }
      } while((curr_error = globus_error_get_cause(curr_error)) != NULL);

      if (replace_error>0) {
        maj_stat &= ~(0xffL << GSS_C_ROUTINE_ERROR_OFFSET);
        maj_stat |= new_error;
        if (new_error == GSS_S_DEFECTIVE_CREDENTIAL) {
          Csec_trace(func, "Use error GSS_S_DEFECTIVE_CREDENTIAL\n");
        } else if (new_error == GSS_S_NO_CRED) {
          Csec_trace(func, "Use error GSS_S_NO_CRED\n");
        } else {
          Csec_trace(func, "Use error 0x%08x\n",new_error);
        }
      } else {
        Csec_trace(func, "Could not determine any more specific error\n");
      }
      min_stat = (OM_uint32) globus_error_put(error_obj);
    }
#endif

    _Csec_process_gssapi_err(FP, "acquiring credentials", maj_stat, min_stat);
    goto error;
  }

  ret = 0;

error:
  save_errno = serrno;
  if (server_name != GSS_C_NO_NAME) {
    (void) gss_release_name(&min_stat, &server_name);
  }

  if (ret) {
    if (cred_handle_P != GLOBUS_NULL && *cred_handle_P != GSS_C_NO_CREDENTIAL) {
      (void) gss_release_cred(&min_stat, cred_handle_P);
    }
    ctx->flags &= ~CSEC_CTX_CREDENTIALS_LOADED;
    Csec_trace(func, "Failure to acquire credentials\n");
  } else {
    ctx->flags |= CSEC_CTX_CREDENTIALS_LOADED;
    Csec_trace(func, "Successfully acquired credentials\n");
  }
  serrno = save_errno;
  return(ret);
}

/**
 * API function for the server to establish the context
 *
 */
int (CSEC_METHOD_NAME(Csec_server_establish_context_ext, MECH))(FP,ctx, s, buf, len)
     FPARG;
     Csec_context_t *ctx;
     int s;
     char *buf;
     int len;
{
  csec_buffer_desc csec_tok;
  gss_buffer_desc recv_tok = GSS_C_EMPTY_BUFFER, send_tok = GSS_C_EMPTY_BUFFER;
  gss_buffer_desc tmpbuf = GSS_C_EMPTY_BUFFER, client_display = GSS_C_EMPTY_BUFFER;
  gss_name_t target_name = GSS_C_NO_NAME, client_name = GSS_C_NO_NAME;
  gss_OID doid = GSS_C_NO_OID;
  OM_uint32 maj_stat, min_stat, acc_sec_min_stat;
  OM_uint32 time_req;
  gss_cred_id_t delegated_cred_handle = GSS_C_NO_CREDENTIAL;
  gss_channel_bindings_t input_chan_bindings = GSS_C_NO_CHANNEL_BINDINGS;
  gss_ctx_id_t *context;
  gss_cred_id_t *server_creds;
  int ext_buf_read = 0;
  char *func= "server_establish_context_ext";
  int save_errno;

  /* Have context/credentials point to the Csec_context_t structure */
  context=&(ctx->connection_context);
  *context = GSS_C_NO_CONTEXT;

  if (!(ctx->flags&CSEC_CTX_CREDENTIALS_LOADED)) {
    Csec_trace(func, "Acquiring server credentials\n");
    if (strlen(ctx->local_name) == 0) {
      serrno = EINVAL;
      Csec_errmsg(func, "No service name specified to load credentials");
      goto error;
    } else {
      if ((CSEC_METHOD_NAME(Csec_acquire_creds, MECH))(FP, ctx, ctx->local_name, Csec_context_is_client(ctx))<0) {
	Csec_errmsg(func, "Could not acquire the local server credentials");
        goto error;
      }
    }
  }
  server_creds=&(ctx->credentials);
  
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
        goto error;
      }
      memcpy(recv_tok.value, buf, len);
    } else {
      Csec_trace(func, "No ext buffer read\n");
      recv_tok.length = 0;
    }

    _Csec_gss_to_csec(&csec_tok,&recv_tok);
    if (_Csec_recv_token(s, &csec_tok, CSEC_NET_TIMEOUT, NULL) < 0) {
      goto error;
    }
    _Csec_csec_to_gss(&csec_tok,&recv_tok);

    maj_stat = gss_accept_sec_context(
				      &acc_sec_min_stat,
				      context,
				      *server_creds,
				      &recv_tok,
				      input_chan_bindings,
				      &client_name,
				      &doid,
				      &send_tok,
				      (OM_uint32 *)&(ctx->context_flags),
				      &time_req,
				      &delegated_cred_handle);

    /* Releasing the recv_tok, it is not needed at this point */
    (void) gss_release_buffer(&min_stat, &recv_tok);

    if (maj_stat!=GSS_S_COMPLETE && maj_stat!=GSS_S_CONTINUE_NEEDED) {
      _Csec_process_gssapi_err(FP, "accepting context",
			       maj_stat,
			       acc_sec_min_stat);
      goto error;
    }


    if (send_tok.length != 0) {
      _Csec_gss_to_csec(&csec_tok,&send_tok);
      if (_Csec_send_token(s, &csec_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE) < 0) {
	Csec_errmsg(func, "failure sending token");
        goto error;
      }
      _Csec_csec_to_gss(&csec_tok,&send_tok);
    } else {
      /* Token has 0 length */
      /* BEWARE THIS IS NOT AN ERROR */
    }

    (void) gss_release_buffer(&min_stat, &send_tok);
  } while (maj_stat & GSS_S_CONTINUE_NEEDED);

  /* remove any previous delegated credentials */
  if (_Csec_delete_deleg_creds(ctx)<0) {
    goto error;
  }

#if defined(GSI) && defined(USE_VOMS)
  _Csec_get_voms_creds(ctx, *context);
#endif



  /* keep new delegated credential, if any */
  if (delegated_cred_handle != GSS_C_NO_CREDENTIAL) {

    /* The gsiapi with krb5 may not have gss_export_cred.
       It was introduced only with V5R2 but may have
       different calling convention from globus
     */

#ifdef GSI
    maj_stat = gss_export_cred(&min_stat, delegated_cred_handle, GSS_C_NO_OID, 0, &tmpbuf);
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "exporting delegeated credential", maj_stat, min_stat);
      goto error;
    }

    ctx->deleg_credentials = malloc(tmpbuf.length);
    if (ctx->deleg_credentials == NULL) {
      Csec_errmsg(func, "Could not allocate memory for buffer");
      serrno = ESEC_SYSTEM;
      goto error;
    }

    memcpy(ctx->deleg_credentials, tmpbuf.value, tmpbuf.length);
    ctx->deleg_credentials_len = tmpbuf.length;
    ctx->flags |= CSEC_CTX_DELEG_CRED_LOADED;

    (void) gss_release_buffer(&min_stat, &tmpbuf);
#endif

    maj_stat = gss_release_cred(&min_stat, &delegated_cred_handle);
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "releasing delegated credentials", maj_stat, min_stat);
      goto error;
    }
  }

  maj_stat = gss_display_name(&min_stat, client_name, &client_display, &doid);
  if (maj_stat != GSS_S_COMPLETE) {
    _Csec_process_gssapi_err(FP, "Error displaying name", maj_stat, min_stat);
    goto error;
  }

  strncpy(ctx->effective_peer_name, client_display.value, CA_MAXCSECNAMELEN);

  maj_stat = gss_release_buffer(&min_stat, &client_display);
  if (maj_stat != GSS_S_COMPLETE) {
    _Csec_process_gssapi_err(FP, "releasing client_display buffer", maj_stat, min_stat);
    goto error;
  }

  /* If we believe the client is presenting a service certifiate, then verify that it
     matches the peer identity */

#ifdef GSI
  if (Csec_isIdAService("GSI", ctx->effective_peer_name)>=0) {
#else
  if (Csec_isIdAService("KRB5", ctx->effective_peer_name)>=0) {
#endif
      int rc;

    /* peer_name was set by Csec_get_peer_service_name() and should contain the
       name of our peer, formatted as a service DN */

    if (_Csec_make_target_name(FP, ctx->peer_name, &target_name)<0) {
      char errbuf[ERRBUFSIZE+1];
      /* we must fail here as we won't be able to check the peer's identity */
      snprintf(errbuf,ERRBUFSIZE,
               "Unable to construct expected target name: Tried using name '%s'\n",
               ctx->peer_name);
      Csec_errmsg(func, errbuf);
      goto error;
    }

    maj_stat = gss_compare_name(&min_stat,
                                client_name,
                                target_name,
                                &rc);

    if (GSS_ERROR(maj_stat)) {
      _Csec_process_gssapi_err(FP, "establish connection on the server: failure to compare peer name",
			       maj_stat,
			       min_stat);
      goto error;
    }

    if (!rc) {
      char errbuf[ERRBUFSIZE+1];
      serrno = ESEC_BAD_CREDENTIALS;
      snprintf(errbuf,ERRBUFSIZE,
               "Mismatch detected between expected peer identity and actual identity: '%s' and '%s'\n",
               ctx->peer_name,
               ctx->effective_peer_name);
      Csec_errmsg(func, errbuf);
      /* nb at this point the client will already consider that this was a successful authentication */
      goto error;
    }
    (void) gss_release_name(&min_stat, &target_name);
  }

  maj_stat = gss_release_name(&min_stat, &client_name);
  if (maj_stat != GSS_S_COMPLETE) {
    _Csec_process_gssapi_err(FP, "releasing name", maj_stat, min_stat);
    goto error;
  }

  /* Setting the flag in the context object ! */
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;
  return 0;

error:
  save_errno = serrno;
  (void) gss_release_buffer(&min_stat, &client_display);
  (void) gss_release_buffer(&min_stat, &send_tok);
  (void) gss_release_buffer(&min_stat, &recv_tok);
  (void) gss_release_buffer(&min_stat, &tmpbuf);
  (void) gss_release_name(&min_stat, &client_name);
  (void) gss_release_name(&min_stat, &target_name);
  (void) _Csec_delete_deleg_creds(ctx);
  (void) gss_release_cred(&min_stat, &delegated_cred_handle);
  if (*context != GSS_C_NO_CONTEXT) {
    (void) gss_delete_sec_context(&min_stat, context, GSS_C_NO_BUFFER);
  }
  serrno = save_errno;
  return -1;
}


/**
 * API function for client to establish function with the server
 */

int (CSEC_METHOD_NAME(Csec_client_establish_context, MECH))(FP,ctx, s)
     FPARG;
     Csec_context_t *ctx;
     int s;
{
  csec_buffer_desc csec_tok;
  gss_buffer_desc send_tok = GSS_C_EMPTY_BUFFER, recv_tok = GSS_C_EMPTY_BUFFER;
  gss_buffer_desc server_display = GSS_C_EMPTY_BUFFER;
  gss_buffer_t token_ptr;
  gss_name_t target_name = GSS_C_NO_NAME, server_name = GSS_C_NO_NAME;
  OM_uint32 maj_stat, min_stat, init_sec_min_stat;
  OM_uint32 in_flags;
  gss_OID oid;
  gss_ctx_id_t *gss_context;
  gss_cred_id_t client_cred_handle = GSS_C_NO_CREDENTIAL;
  char *func = "client_extablish_context";
  int save_errno;

  Csec_trace(func, "Entering\n");
  gss_context = &(ctx->connection_context);
  *gss_context = GSS_C_NO_CONTEXT;
  
  /* Set flags */
  in_flags = GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG;
#ifdef GSI
  if (ctx->sec_flags & CSEC_OPT_DELEG_FLAG) {
    in_flags |= GSS_C_DELEG_FLAG;
  }
#endif
  /* Choose default mechanism for the library */
  oid = GSS_C_NULL_OID;

  /* explictly acquire client credentials for GSI, so we can explictly catch expired credentials */
#ifdef GSI
  if (!(ctx->flags&CSEC_CTX_CREDENTIALS_LOADED)) {
    Csec_trace(func, "Acquiring client credentials\n");
    if ((CSEC_METHOD_NAME(Csec_acquire_creds, MECH))(FP, ctx, NULL, Csec_context_is_client(ctx))<0) {
      Csec_errmsg(func, "Could not acquire the local user (i.e. client) credentials");
      goto error;
    } else {
      client_cred_handle = (ctx->credentials);
    }
  }
#endif
  
  /*
   * Import the name into target_name.
   */
  if (_Csec_make_target_name(FP, ctx->peer_name, &target_name)<0) {
    char errbuf[ERRBUFSIZE+1];
    /* we must fail here as gss_init_sec_context won't be able to check the server's identity */
    snprintf(errbuf,ERRBUFSIZE,
               "Unable to construct expected target name: Tried using name '%s'\n",
               ctx->peer_name);
    Csec_errmsg(func, errbuf);
    goto error;
  }

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

  Csec_trace(func, "Starting context establishment loop\n");

  token_ptr = GSS_C_NO_BUFFER;

  do {

    maj_stat = gss_init_sec_context(&init_sec_min_stat,
				    client_cred_handle,
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
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Could not create context.");
      goto error;
    }

    if (token_ptr != GSS_C_NO_BUFFER) {
      (void) gss_release_buffer(&min_stat, token_ptr);
      token_ptr = GSS_C_NO_BUFFER;
    }

    if (maj_stat!=GSS_S_COMPLETE && maj_stat!=GSS_S_CONTINUE_NEEDED) {
      _Csec_process_gssapi_err(FP, "initializing context", maj_stat,
			       init_sec_min_stat);
      goto error;
    }

    if (send_tok.length > 0) {
      int rc;  
      _Csec_gss_to_csec(&csec_tok,&send_tok);
      rc = _Csec_send_token(s, &csec_tok, CSEC_NET_TIMEOUT, CSEC_TOKEN_TYPE_HANDSHAKE);
      _Csec_csec_to_gss(&csec_tok,&send_tok);
      if (rc < 0) {
        goto error;
      }
    }
    (void) gss_release_buffer(&min_stat, &send_tok);

    if (maj_stat & GSS_S_CONTINUE_NEEDED) {
      int rc;
      recv_tok.length = 0;

      _Csec_gss_to_csec(&csec_tok,&recv_tok);
      rc = _Csec_recv_token(s, &csec_tok, CSEC_NET_TIMEOUT, NULL);
      _Csec_csec_to_gss(&csec_tok,&recv_tok);
      if (rc < 0) {
        goto error;
      }
      token_ptr = &recv_tok;
    }
  } while (maj_stat & GSS_S_CONTINUE_NEEDED);

  /* find the peer name to fill in ctx->effective_peer_name */

  {
    gss_name_t src_name;
    OM_uint32 lifetime, tmpctx;
    gss_OID mech;
    int local, isopen;
      
    maj_stat = gss_inquire_context(&min_stat,
				   *gss_context,
				   &src_name,
				   &server_name,
				   &lifetime,
				   &mech,
				   &tmpctx,
				   &local,
				   &isopen);
      
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "Error inquiring context",
			       maj_stat,
			       min_stat);
      goto error;
    }

    (void)gss_release_name(&min_stat, &src_name);
      
    maj_stat = gss_display_name(&min_stat, server_name, &server_display, (gss_OID *) NULL);
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "Error displaying name", maj_stat, min_stat);
      goto error;
    }
  }
      
  strncpy(ctx->effective_peer_name, server_display.value, CA_MAXCSECNAMELEN);  
  
  (void) gss_release_buffer(&min_stat, &server_display);
  (void) gss_release_name(&min_stat, &server_name);
  (void) gss_release_name(&min_stat, &target_name);

  /* Setting the flag in the context object ! */
  ctx->flags |= CSEC_CTX_CONTEXT_ESTABLISHED;

  return 0;

error:
  save_errno = serrno;
  (void) gss_release_buffer(&min_stat, &send_tok);
  (void) gss_release_buffer(&min_stat, &recv_tok);
  (void) gss_release_buffer(&min_stat, &server_display);
  (void) gss_release_name(&min_stat, &server_name);
  (void) gss_release_name(&min_stat, &target_name);
  if (*gss_context != GSS_C_NO_CONTEXT) {
    (void) gss_delete_sec_context(&min_stat, gss_context, GSS_C_NO_BUFFER);
  }
  serrno = save_errno;
  return -1;
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

#ifndef GSI
  /* With GSI the GSS_S_* errors are already in the shifted position */
  maj_stat >>= 16;
#endif

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

  /* printf("#0x%08x -> %d \n", maj_stat, ret_serrno); */
  return ret_serrno;

}


/**
 * Function to display the GSS-API errors
 */
static void _Csec_process_gssapi_err(FP, msg, maj_stat_code, min_stat_code)
     FPARG;
     char *msg;
     OM_uint32 maj_stat_code;
     OM_uint32 min_stat_code;
{

  char errbuf[ERRBUFSIZE];
  char *errbufp;
  int errn;

  errn = _Csec_map_gssapi_err(maj_stat_code, min_stat_code);
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

  serrno = errn;
}

/* preserve serrno */
static void _Csec_csec_to_gss(csec_buffer_t csec, gss_buffer_t gss) {
  gss->value = csec->value;
  gss->length = csec->length;
}

/* preserve serrno */
static void _Csec_gss_to_csec(csec_buffer_t csec, gss_buffer_t gss) {
  csec->value = gss->value;
  csec->length = gss->length;
}

static int _Csec_make_target_name(FPARG, const char *server_dn, gss_name_t *target_name_P) {
  char *func= "_Csec_make_target_name";
  gss_buffer_desc send_tok;
  char *p;
  OM_uint32 maj_stat, min_stat;

  *target_name_P = GSS_C_NO_NAME;

#ifdef GSI
  if (server_dn != NULL &&
      strlen(server_dn)>6 &&
      !strncasecmp(server_dn, "/CN=", 4) &&
      (p = strstr(&server_dn[4], "/")) != NULL) {
    
    size_t service_len;
    size_t host_len;
    char *stbuf;

    service_len = p - server_dn - 4;
    host_len = strlen(server_dn) - service_len - 5;

    stbuf=malloc(service_len+host_len+2);
    if (stbuf==NULL) {
      Csec_errmsg(func, "Could not allocate space to build target name");
      serrno = ENOMEM;
      return -1;
    }

    /* fill stbuf with service@host */
    strncpy(stbuf, &server_dn[4], service_len);
    stbuf[service_len] = '@';
    strncpy(&stbuf[service_len+1], &server_dn[4+service_len+1], host_len);
    stbuf[service_len+1+host_len] = '\0';

    Csec_trace(func, "Name parsed:<%s> to %s\n", server_dn, stbuf);

    send_tok.value = stbuf;
    send_tok.length = strlen(stbuf) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok, GSS_C_NT_HOSTBASED_SERVICE, target_name_P);

    free(stbuf);

    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "importing name", maj_stat, min_stat);
      return -1;
    }

  } else {
    serrno = EINVAL;
    return -1;
  }
#else
  if (server_dn != NULL &&
    strlen(server_dn) > 0) {
    send_tok.value = (void *)server_dn;
    send_tok.length = strlen(server_dn) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok,
                               (gss_OID) GSS_C_NT_USER_NAME, target_name_P);
    
    if (maj_stat != GSS_S_COMPLETE) {
      _Csec_process_gssapi_err(FP, "parsing name", maj_stat, min_stat);
      return -1;
    }

    Csec_trace(func, "Name parsed:<%s>\n", server_dn);
  } else {
    serrno = EINVAL;
    return -1;
  }
#endif

  /* a check to ensure we never reach here without a name */
  if (*target_name_P == GSS_C_NO_NAME) {
    serrno = EINVAL;
    return -1;
  }

  return 0;
}

#if defined(GSI) && defined(USE_VOMS)

static int _Csec_get_voms_creds(Csec_context_t *ctx, 
                                gss_ctx_id_t context_handle){

  int error= 0;
  X509 *px509_cred= NULL;
  STACK_OF(X509) *px509_chain = NULL;
  struct vomsdata *vd= NULL;
  struct voms **volist;
  struct voms vo;
  gss_ctx_id_desc * context;
  gss_cred_id_t cred;  
  /* Internally a gss_cred_id_t type is a pointer to a gss_cred_id_desc */
  gss_cred_id_desc *       cred_desc = NULL;
  globus_gsi_cred_handle_t gsi_cred_handle;
  X509 * px509 = NULL;
  int ret = -1;

  /* Downcasting the context structure  */
  context = (gss_ctx_id_desc *) context_handle;
  cred = context->peer_cred_handle;

  /* cast to gss_cred_id_desc */
  if (cred == GSS_C_NO_CREDENTIAL) {
    goto leave;
  }

  cred_desc = (gss_cred_id_desc *) cred;
  
  /* Getting the X509 certicate */
  gsi_cred_handle = cred_desc->cred_handle;
  if (globus_gsi_cred_get_cert(gsi_cred_handle, &px509_cred) != GLOBUS_SUCCESS) {
    goto leave;
  }
  
  /* Getting the certificate chain */
  if (globus_gsi_cred_get_cert_chain (gsi_cred_handle, &px509_chain) != GLOBUS_SUCCESS) {
    goto leave;
  }
  
  if ((vd = VOMS_Init (NULL, NULL)) == NULL) {
    /* XXX Error processing ? */
    goto leave;
  }
  
  if (VOMS_Retrieve (px509_cred, px509_chain, RECURSE_CHAIN, vd, &error) == 0) {
    /* XXX Error processing ? */
    goto leave;
  }
  
  volist = vd->data;
  
  if (volist !=NULL) {
    int i = 0;
    int nbfqan;
    
    /* Copying the voname */
    if ((*volist)->voname != NULL) {
      ctx->voname = strdup((*volist)->voname);
    }
    
    
    /* Counting the fqans before allocating the array */
    while( volist[0]->fqan[i] != NULL) {
      i++;
    }
    nbfqan = i;
    
    if (nbfqan > 0) {
      ctx->fqan = malloc(sizeof(char *) * (i+1));
      if (ctx->fqan != NULL) {
	for (i=0; i<nbfqan; i++) {
	  ctx->fqan[i] = strdup( volist[0]->fqan[i]);   
	}
	ctx->fqan[nbfqan] = NULL;
	ctx->nbfqan = nbfqan;
      }
    } /* if (nbfqan > 0) */
  }

  ret = 0;

leave:  
  if (vd) VOMS_Destroy (vd);
  if (px509_chain) sk_X509_pop_free(px509_chain,X509_free);
  if (px509_cred) X509_free(px509_cred);

  return(ret);
}


#endif
