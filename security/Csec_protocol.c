/*
 * $id$
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_protocol.c 
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_protocol_policy.c CERN IT/ADC/CA Benjamin Couturier";
#endif
		
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include "serrno.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <osdep.h>
#include <marshall.h>
#include <Csec.h>
#include <Csec_protocol.h>

#define DEFAULT_BUFFER_SIZE 512


/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options. 
 * This function takes an empty csec_buffer_t  and allocates the buffer
 * 
 *
 * The message contains:
 * LONG Protocol version
 * LONG options
 * LONG Has authorization ID
 * If Has authorization ID:
 *   STRING Mechanism
 *   STRING id
 * LONG Number of supported protocols
 * One STRING per supported protocol

 */
int _Csec_client_buildRequestMessage(Csec_context_t *ctx,
				     csec_buffer_t request) {
  char *buffer, *p;
  int buffer_len = DEFAULT_BUFFER_SIZE;
  char *func = "_Csec_client_buildRequestMessage";
  int i;

  /* Allocating a buffer for the message */
  request->value = malloc(buffer_len);
  if (request->value ==NULL) {
    serrno = ENOMEM;
    _Csec_errmsg(func, "Could not allocate memory for buffer");
    return -1;
  }
  request->length = buffer_len;

  /* Now marshalling the request */
  p = buffer = request->value;
  marshall_LONG(p, CSEC_VERSION);
  marshall_LONG(p, ctx->options);
  if (ctx->authorization_id != NULL) {
    marshall_LONG(p, 1);
    marshall_STRING(p, _Csec_id_mech(ctx->authorization_id));
    marshall_STRING(p, _Csec_id_name(ctx->authorization_id));
  } else {
    marshall_LONG(p, 0);
  }
  marshall_LONG(p, ((unsigned long)ctx->nb_supported_protocols));
  for(i = 0; i<ctx->nb_supported_protocols; i++) {
    marshall_STRING(p, _Csec_protocol_name(ctx->supported_protocols[i]));
  }

  request->length = p - buffer;
  return 0;
  /* XXX Buffer checking ? */

}


/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_server_parseRequestMessage(Csec_context_t *ctx,
				     csec_buffer_t request) {

  char *p, *buffer;
  char aid_mech[CA_MAXCSECPROTOLEN+1];
  char aid_name[CA_MAXCSECNAMELEN+1];
  char protocol[CA_MAXCSECPROTOLEN+1];
  int version, options, hasAuthorizationId, i;
  Csec_id_t *authorizationId;
  char *func = " _Csec_server_parseRequestMessage";

  /* Now marshalling the request */
  p = buffer = request->value;
  unmarshall_LONG(p, version);
  unmarshall_LONG(p, ctx->peer_options);
  unmarshall_LONG(p, hasAuthorizationId);
  if (hasAuthorizationId) {
    unmarshall_STRINGN(p, aid_mech, CA_MAXCSECPROTOLEN);
    unmarshall_STRINGN(p, aid_name, CA_MAXCSECNAMELEN);
  }
  ctx->authorization_id = _Csec_create_id(aid_mech, aid_name);
  unmarshall_LONG(p, ctx->nb_peer_protocols);
  if (ctx->nb_peer_protocols > 0) {
    (ctx->peer_protocols) = (Csec_protocol_t **)malloc(ctx->nb_peer_protocols 
						    * sizeof(Csec_protocol_t *));
    if (ctx->peer_protocols == NULL) {
      serrno = ENOMEM;
      _Csec_errmsg(func, "Could not allocate memory for peer protocols");
      return -1;
    }
    for (i=0; i<ctx->nb_peer_protocols; i++) {
      unmarshall_STRINGN(p, protocol, CA_MAXCSECPROTOLEN);
      ctx->peer_protocols[i] = _Csec_create_protocol(protocol);
    } 
  }
  return 0;
}


/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_server_buildResponseMessage(Csec_context_t *ctx,
				      csec_buffer_t response,
				      int negociationStatus,
				      Csec_protocol_t *chosenProtocol,
				      int acceptedOptions) {
  char *buffer, *p;
  int buffer_len = DEFAULT_BUFFER_SIZE;
  int i;
  char *func = "_Csec_server_buildResponseMessage";

  /* Allocating a buffer for the message */
  response->value = malloc(buffer_len);
  if (response->value ==NULL) {
    serrno = ENOMEM;
    _Csec_errmsg(func, "Could not allocate memory for buffer");
    return -1;
  }
  response->length = buffer_len;

  /* Now marshalling the response */
  p = buffer = response->value;
  marshall_LONG(p, CSEC_VERSION);
  marshall_LONG(p, acceptedOptions);
  marshall_LONG(p, negociationStatus);
  if (negociationStatus == PROT_STAT_OK) {
    marshall_LONG(p, 1);
    marshall_STRING(p, _Csec_protocol_name(chosenProtocol));
  } else {
    marshall_LONG(p, ((unsigned long)ctx->nb_supported_protocols));
    for(i = 0; i<ctx->nb_supported_protocols; i++) {
      marshall_STRING(p, _Csec_protocol_name(ctx->supported_protocols[i]));
    }
  }
  response->length = p - buffer;
  return 0;

  /* XXX Buffer checking ? */
}



/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_client_parseResponseMessage(Csec_context_t *ctx,
				      csec_buffer_t response) {
  char *p, *buffer;
  char protocol[CA_MAXCSECPROTOLEN+1];
  int version, options, negociationStatus, i;
  Csec_id_t *authorizationId;
  char *func = " _Csec_client_parseResponseMessage";

  /* Now marshalling the response */
  p = buffer = response->value;
  unmarshall_LONG(p, version);
  unmarshall_LONG(p, ctx->peer_options);
  unmarshall_LONG(p, ctx->protocolNegociationStatus);  
  unmarshall_LONG(p, ctx->nb_peer_protocols);
  if (ctx->nb_peer_protocols > 0) {
    (ctx->peer_protocols) = (Csec_protocol_t **)malloc(ctx->nb_peer_protocols 
						    * sizeof(Csec_protocol_t *));
    if (ctx->peer_protocols == NULL) {
      serrno = ENOMEM;
      _Csec_errmsg(func, "Could not allocate memory for peer protocols");
      return -1;
    }
    for (i=0; i<ctx->nb_peer_protocols; i++) {
      unmarshall_STRINGN(p, protocol, CA_MAXCSECPROTOLEN);
      ctx->peer_protocols[i] = _Csec_create_protocol(protocol);
    } 
  }
  return 0;
}


/**
 * Checks the request from the client and 
 * XXX Add documentation concerning the algorithm
 * of the negociation on client side and server side !
 */
int _Csec_server_checkClientRequest(Csec_context_t *ctx,
				    int *negociationStatus,
				    Csec_protocol_t **chosenProtocol,
				    int *acceptedOptions) {

  int clientprot, serverprot;

  /* Setting the protocol */
  if (negociationStatus != NULL) 
    *negociationStatus =   PROT_STAT_NOK;
  ctx->protocolNegociationStatus = PROT_STAT_OK;
 
  /* Checking the protocol */
  for(clientprot = 0; clientprot < ctx->nb_peer_protocols; clientprot++) {
    for(serverprot = 0; serverprot < ctx->nb_supported_protocols; serverprot++) {
      if (0 == _Csec_protocol_cmp(ctx->supported_protocols[serverprot],
				  ctx->peer_protocols[clientprot])) {
	ctx->protocol = ctx->supported_protocols[serverprot];
	if (negociationStatus != NULL) 
	  *negociationStatus =  PROT_STAT_OK;
	ctx->protocolNegociationStatus = PROT_STAT_OK;
	if (chosenProtocol != NULL) 
	  *chosenProtocol = ctx->protocol;
      }
    }  /* for clientprot */
  } /* for clientprot */
  

  /* Checking the options */
  if (acceptedOptions != NULL) 
    *acceptedOptions = ctx->options & ctx->peer_options;
  return 0;

}


/**
 * Checks the response from the server
 * XXX Add documentation concerning the algorithm
 * of the negociation on client side and server side !
 */
int _Csec_client_checkServerRequest(Csec_context_t *ctx,
				    int *negociationStatus,
				    Csec_protocol_t **chosenProtocol,
				    int *acceptedOptions) {


  if (negociationStatus != NULL) 
    *negociationStatus =   ctx->protocolNegociationStatus;

  if (ctx->protocolNegociationStatus ==  PROT_STAT_OK
      && ctx->nb_peer_protocols >0) {
    ctx->protocol = ctx->peer_protocols[0];
  } else {
    ctx->protocol = NULL;
  }

  if (chosenProtocol != NULL) 
    *chosenProtocol = ctx->protocol;

  if (acceptedOptions != NULL) 
    *acceptedOptions = ctx->peer_options;

  return 0;

}



/**
 * Negociate the protocol with the client !
 */
int   _Csec_server_negociateProtocol (Csec_context_t *ctx,
				      int socket) {

   /* 
     Steps:
     1- Get client address from socket
     2- Lookup protocols accepted by server for that client
     3- Read request from client
     4- Prepare/send response to the client
  */

  csec_buffer_desc requestbuf, responsebuf;
  csec_buffer_t request, response;
  int rc, token_type;
  int negociationStatus, acceptedOptions;
  Csec_protocol_t *chosenProtocol;
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  char *func = "_Csec_server_negociateProtocol";

  request = &requestbuf;
  response = &responsebuf;
  request->value = response->value = NULL;
  request->length = response->length = 0;

  /* 1- Getting the client  IP address */
  rc = getpeername(socket, (struct sockaddr *)&from, &fromlen);
  if (rc < 0) {
    _Csec_errmsg(func, "Could not get peer name: %s", strerror(errno));
    return -1;
  }

  /* 2- Load the protocols accepted by the srever for that client */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    _Csec_server_lookup_protocols(from.sin_addr.s_addr,
				  &(ctx->supported_protocols),
				  &(ctx->nb_supported_protocols));
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  }


  /* 3- get request from client */
  rc =  _Csec_recv_token(socket, 
			 request, 
			 CSEC_NET_TIMEOUT, 
			 &token_type);
  if (rc != 0) {
    /* We have an error receiving the response, stop there */  
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return rc;
  }


  /* Prepare the request for the server */
  if (_Csec_server_parseRequestMessage(ctx, request) != 0) {
    /* serrno and message already set ! */
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return -1;
  }


  /* Analyzing the client request */
  if( _Csec_server_checkClientRequest(ctx,
				      &negociationStatus,
				      &chosenProtocol,
				      &acceptedOptions) != 0) {
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return -1;
  }

  /* Building the response */
  if(_Csec_server_buildResponseMessage(ctx,
				       response,
				       negociationStatus,
				       chosenProtocol,
				       acceptedOptions)) {
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return -1;
  }


  /* Send the response */
  rc =  _Csec_send_token(socket, 
			 response, 
			 CSEC_NET_TIMEOUT, 
			 CSEC_TOKEN_TYPE_PROTOCOL_RESP);
  if (rc != 0) {
    /* We have an error sending the request, stop there */  
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return rc;
  } 


  if (negociationStatus != PROT_STAT_OK) {
    _Csec_errmsg(func, "Could not agree on protocol with the client");
    return -1;
  }
    

  return 0;
}



/**
 * Negociate the protocol with the server !
 */
int  _Csec_client_negociateProtocol (Csec_context_t *ctx,
				     int socket) {
  /* 
     Steps:
     1- Lookup protocols accepted by the client
     2- Send request to server
     3- Wait for response and draws the conclusions !
     */

  csec_buffer_t request, response;
  csec_buffer_desc requestbuf, responsebuf;
  int rc, token_type;
  int negociationStatus, acceptedOptions;
  Csec_protocol_t *chosenProtocol;
  char *func = "_Csec_client_negociateProtocol";

  request = &requestbuf;
  response = &responsebuf;
  request->value = response->value = NULL;
  request->length = response->length = 0;

  /* Load the protocols from the environment */
  if (!(ctx->flags & CSEC_CTX_PROTOCOL_LOADED)) {
    _Csec_client_lookup_protocols(&(ctx->supported_protocols),
				  &(ctx->nb_supported_protocols));
    ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  }

  /* Prepare the request for the server */
  if (_Csec_client_buildRequestMessage(ctx, request) != 0) {
    /* serrno and message already set ! */
    return -1;
  }

  /* Send the request */
  rc =  _Csec_send_token(socket, 
			 request, 
			 CSEC_NET_TIMEOUT, 
			 CSEC_TOKEN_TYPE_PROTOCOL_REQ);
  if (rc != 0) {
    /* We have an error sending the request, stop there */  
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return rc;
  } 


  /* In this first version we wait for the server response 
     before going on */
  rc =  _Csec_recv_token(socket, 
			 response, 
			 CSEC_NET_TIMEOUT, 
			 &token_type);
  if (rc != 0) {
    /* We have an error receiving the response, stop there */  
    _Csec_clear_token(request);
    _Csec_clear_token(response);
    return rc;
  }



  /* If we successfully received a response
     parse it and check the status of the protocol negociation */
  rc = _Csec_client_parseResponseMessage(ctx, response);
  _Csec_clear_token(request);
  _Csec_clear_token(response);
  if (rc != 0) {
    return rc;
  }
  
  
  /* Now check the result of the protocol negociation */
  _Csec_client_checkServerRequest(ctx,
				  &negociationStatus,
				  &chosenProtocol,
				  &acceptedOptions);
  
  if (negociationStatus != PROT_STAT_OK) {
    _Csec_errmsg(func, "Could not agree on protocol with server");
    return -1;
  }

  ctx->options |= acceptedOptions;

  return 0;
}
