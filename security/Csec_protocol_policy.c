/*
 * $id$
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_protocol_policy.c 
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_protocol_policy.c CERN IT/ADC/CA Benjamin Couturier";
#endif
		
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <string.h>
#include <serrno.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include <osdep.h>
#include <Csec.h>
#include <Csec_protocol_policy.h>


EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

#define TMPBUFSIZE 300

/**
 * Returns the the list of protocols usable, as a client.
 */
int Csec_client_lookup_protocols(Csec_protocol **protocols, int *nbprotocols) {

  char *p, *q, *ctx;
  char *buf;
  int entry = 0;
  Csec_protocol *prots;
  char *func="Csec_client_lookup_protocols";

  Csec_trace(func, "Looking up protocols from the environment\n");

  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv (CSEC_MECH)) 
	|| (p = (char *)getconfent (CSEC_CONF_SECTION, 
				    CSEC_CONF_ENTRY_MECH, 0)))) {
    p = CSEC_DEFAULT_MECHS;
  }

  Csec_trace(func, "Protocols looked up are <%s>\n", p);
  
  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d\n",
		strlen(p)+1);
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &ctx);
  while (q  != NULL) {
    if (strlen(q) > 0) entry++;
    q = strtok_r(NULL, " \t", &ctx);
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == prots) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d\n",
		entry * sizeof(Csec_protocol));
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &ctx);
  while (q != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, PROTID_SIZE);
      q = strtok_r(NULL, " \t", &ctx);
      entry ++;
    }
  }

  *protocols = prots;
  free(buf);
  return 0;
}

/**
 * Creates the list of authorized protocols for a given client, depending on its IP address.
 */
int Csec_server_lookup_protocols(long client_address,
				 Csec_protocol **protocols,
				 int *nbprotocols) {
  char *p, *q, *ctx;
  char *buf;
  int entry = 0;
  Csec_protocol *prots;
  char *func = "Csec_server_lookup_protocols";
  struct in_addr a;

  /* The client parameter is not currently used, but has been added for a later version
     In any case, a client_address of 0 should then load ALL the protocols
     available, independently of the address */

  a.s_addr = client_address;
  Csec_trace(func, "Looking for allowed security protocols for %s\n", inet_ntoa (a));
	     
  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv (CSEC_AUTH_MECH)) 
	|| (p = (char *)getconfent (CSEC_CONF_SECTION, CSEC_CONF_ENTRY_AUTHMECH, 0)))) {
    p = CSEC_DEFAULT_MECHS;
  }

  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &ctx);
  while (q  != NULL) {
    if (strlen(q) > 0) entry++;
    q = strtok_r(NULL, " \t", &ctx);
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == buf) {
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &ctx);
  while (q != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, PROTID_SIZE);
      q = strtok_r(NULL, " \t", &ctx);
      entry ++;
    }
  }

  free(buf);
  *protocols = prots;

  return 0;


}



int Csec_server_set_protocols(Csec_context_t *ctx, int socket) {
  int rc;
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  char *func= "Csec_server_set_protocols";

  /* Getting the peer IP address */
  rc = getpeername(socket, (struct sockaddr *)&from, &fromlen);
  if (rc < 0) {
    Csec_errmsg(func, "Could not get peer name: %s\n", strerror(errno));
    return -1;
  }

  rc = Csec_server_lookup_protocols(from.sin_addr.s_addr,
				    &(ctx->protocols), 
				    &(ctx->nb_protocols));
  if (rc != 0) {
    Csec_errmsg(func, "Could not get security protocols for client IP: %s",
		inet_ntoa (from.sin_addr));
    return rc;
  }
  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;

  return 0;
  
}


/**
 * Sends the initial buffer to indicate the method
 */
int Csec_client_send_protocol(int s, int timeout, Csec_context_t *ctx) {

  gss_buffer_desc buf;
  char *func = "Csec_client_send_protocol";
  int rc;
  
  if (ctx == NULL) {
    serrno = EINVAL;
    return -1;
  }
     
  if (!(ctx->flags&CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_NO_CONTEXT;
    return -1;
  }
     
  buf.length = strlen(ctx->protocols[ctx->current_protocol].id) + 1;
  buf.value = (char *)malloc(buf.length);
  strncpy(buf.value, ctx->protocols[ctx->current_protocol].id, buf.length);
  *(((char *)buf.value) + buf.length -1) = '\0';

  Csec_trace(func, "Sending %d bytes, <%s>\n", buf.length, (char *)buf.value);
     
  rc = _Csec_send_token(s, &buf, timeout, CSEC_TOKEN_TYPE_PROTOCOL_REQ);

  free(buf.value);
  return rc;
     
}
 

/**
 * Sends the initial buffer to indicate the method
 */
int Csec_server_receive_protocol(int s, int timeout, Csec_context_t *ctx, char *buf, int len) {

  gss_buffer_desc gssbuf;
  char *func = "Csec_server_receive_protocol";
  char protid[PROTID_SIZE];
  int received_token_type = 0;

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


  /* Loading the list of allowed protocols for the given client */
  if (Csec_server_set_protocols(ctx, s) < 0) {
    return -1;
  }


  /* Setting up buffers and receiving data */
  if (len > 0) {
    gssbuf.length = len;
    gssbuf.value = (char *)malloc(len);
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
     
  if (_Csec_recv_token(s, &gssbuf, timeout, &received_token_type)<0) {
    Csec_errmsg(func, "Could not read protocol token");
    return -1;
  }
     
  if (received_token_type != CSEC_TOKEN_TYPE_PROTOCOL_REQ) {
    Csec_errmsg(func, "Token has type %d instead of %d",
		received_token_type, CSEC_TOKEN_TYPE_PROTOCOL_REQ);
    free(gssbuf.value);
    return -1;
    
  }

  strncpy(protid, gssbuf.value, PROTID_SIZE);
  free(gssbuf.value);
     
  Csec_trace(func, "Received initial token, for protocol <%s>\n",
	     protid);

  /* Checking the requested protocol with the list of allowed ones */
  {
    int i;
    int protocol_ok = 0;

    for(i=0; i<ctx->nb_protocols; i++) {
      Csec_trace(func, "Comparing with protocol <%s>", ctx->protocols[i].id );
      if(strcmp(protid, ctx->protocols[i].id) == 0) {
	ctx->current_protocol = i;
	protocol_ok = 1;
	Csec_trace(func, "Accepting protocol <%s>", protid);
	break;
      } 
    } 

    if (protocol_ok) {
      if ( Csec_get_shlib(ctx) == NULL) {
	serrno = ESEC_NO_SECMECH;
	Csec_errmsg(func, "Could not load shared library for protocol <%s>\n", ctx->protocols[ctx->current_protocol].id);
	return -1; 
      } 
    } else {
      Csec_trace(func, "Cannot accept protocol <%s>", protid);
    }
    return Csec_server_send_response(s, timeout, ctx, protocol_ok); 
  }
} /* Csec_receive_protocol */




/**
 * Sends the response to the client
 */
int Csec_server_send_response(int s, int timeout, Csec_context_t *ctx, int protocol_ok) {
  
  gss_buffer_desc buf;
  char *func = "Csec_server_send_response";
  int rc;
  char *p;
  int buf_len = 200;

  if (ctx == NULL) {
    serrno = EINVAL;
    return -1;
  }
     
  if (!(ctx->flags&CSEC_CTX_INITIALIZED)) {
    serrno = ESEC_NO_CONTEXT;
    return -1;
  }

  buf.value = (char *)malloc(buf_len + 50);
  if (buf.value == NULL) {
    Csec_errmsg(func, "Could not allocate space for buffer");
    return -1;
  }

  p = buf.value;
  if (protocol_ok) {
    marshall_STRING(p, PROT_REQ_OK);
  } else {
    int i;
    
    marshall_STRING(p, PROT_REQ_NOK);
    marshall_LONG(p, ctx->nb_protocols);
    
    for (i=0; i<ctx->nb_protocols; i++) {
      marshall_STRING(p, ctx->protocols[i].id); 
      if ( (p - (char *)buf.value) >= buf_len ) {
	buf_len *= 2;
	buf.value = (char *)realloc(buf.value, buf_len + 50);
	if (buf.value == NULL) {
	  Csec_errmsg(func, "Could not allocate space for buffer");
	  return -1;
	}
      }
    }
  }

  buf.length = p - (char *)buf.value;

  Csec_trace(func, "Sending %d bytes, <%s>\n", buf.length, (char *)buf.value);
     
  rc = _Csec_send_token(s, &buf, timeout, CSEC_TOKEN_TYPE_PROTOCOL_RESP);

  return rc;

}


/**
 * Reads the server response
 */
int Csec_client_receive_server_response(int s, int timeout, Csec_context_t *ctx) {

  gss_buffer_desc gssbuf;
  char *func = "Csec_client_receive_server_response";
  int received_token_type = 0;


  Csec_trace(func, "Entering\n");
     
  if (ctx == NULL) {
    serrno = EINVAL;
    return -1;
  }

  if (!(ctx->flags&CSEC_CTX_INITIALIZED)) {
    Csec_trace(func, "Context not initialized !\n");
    serrno = ESEC_NO_CONTEXT;
    return -1;
  }

  gssbuf.length = 0;
  if (_Csec_recv_token(s, &gssbuf, timeout, &received_token_type)<0) {
    Csec_errmsg(func, "Could not read protocol token");
    return -1;
  }
     
  if (received_token_type != CSEC_TOKEN_TYPE_PROTOCOL_RESP) {
    Csec_errmsg(func, "Token has type %d instead of %d",
		received_token_type, CSEC_TOKEN_TYPE_PROTOCOL_RESP);
    free(gssbuf.value);
    return -1;    
  }


  {
    char *p;
    char buf[TMPBUFSIZE+1];

    p = gssbuf.value;
    unmarshall_STRINGN(p, buf, TMPBUFSIZE);

    if (strcmp(buf, PROT_REQ_OK)==0) {
      ctx->protocol_negociation_status = PROT_STAT_OK;
      free(gssbuf.value);
      return 0;
    } else if (strcmp(buf, PROT_REQ_NOK)==0) {
      int i;
      ctx->protocol_negociation_status = PROT_STAT_NOK;
      unmarshall_LONG(p, ctx->nb_peer_protocols);
      
      /* Checking the number of protocols returned */
      if (ctx->nb_peer_protocols <= 0) {
	Csec_errmsg(func, 
		    "Server response: %d protocols supported\n",
		    ctx->nb_peer_protocols);
	free(gssbuf.value);
	return -1;
      }
      
      /* Creating the list of peer protocols in the buffer */
      ctx->peer_protocols = (Csec_protocol *)malloc(ctx->nb_peer_protocols * sizeof(Csec_protocol));
      if (ctx->peer_protocols == NULL) {
	Csec_errmsg(func, "Could not allocate memory for buffer");
	serrno = ESEC_SYSTEM;
	return -1;
      }

      for (i=0; i<ctx->nb_peer_protocols; i++) {
	unmarshall_STRINGN(p, ctx->peer_protocols[i].id, PROTID_SIZE);
      }
    } else {
      ctx->protocol_negociation_status = PROT_STAT_NOK;
      Csec_errmsg(func, 
		  "Server response: bad message structure received <%s>\n",
		  buf);
      free(gssbuf.value);
    }
    
    return 0;
  }
} /* Csec_client_receive_server_response */





int Csec_client_negociate_protocol(int socket, int timeout, Csec_context_t *ctx) {
  char *func = "Csec_client_negociate_protocol";
  int rc;
  
  rc = Csec_client_send_protocol(socket, timeout, ctx);
  if (rc != 0) {
    return -1;
  }
  
  rc = Csec_client_receive_server_response(socket, timeout, ctx);
  if (rc != 0) {
    return -1;
  }
  
  if (ctx->protocol_negociation_status == PROT_STAT_NOK) {
    char tmpbuf[TMPBUFSIZE+1];
    char *p;
    int i;
    
    p = tmpbuf;
    p += snprintf(p, TMPBUFSIZE, "Server does not support %s, it supports:", ctx->protocols[ctx->current_protocol].id);
    for (i=0; i<ctx->nb_peer_protocols; i++) {
	p += snprintf(p, (tmpbuf+TMPBUFSIZE) - p, " %s", ctx->peer_protocols[i].id);
    }
    
    Csec_errmsg(func, tmpbuf);
    serrno = 22;
    return -1; /* XXX */
  }
  
  return 0;

} /* Csec_client_negociate_protocols */


/**
 * Initializes the protocols in the context from a list rather than
 * from the environment variables.
 */
int Csec_initialize_protocols_from_list(Csec_context_t *ctx,
					Csec_protocol *protocol) {
  int rc, i;
  Csec_protocol *p = protocol;
  char *func = "Csec_initialize_protocols_from_list";

  if (ctx == NULL || protocol == NULL) {
    serrno = EINVAL;
    Csec_errmsg(func, "NULL parameter ctx:%p protocols:%p\n",
		ctx, protocol);
    return -1;
  }
  
  for (i = 0; p[i].id[0] != '\0'; i++);
  /* BEWARE, empty loop */
  
  ctx->nb_protocols = i;
  ctx->protocols = (Csec_protocol *)malloc(ctx->nb_protocols * sizeof(Csec_protocol));
  if (ctx->protocols == NULL) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d\n",
		ctx->nb_protocols * sizeof(Csec_protocol));
      return -1;
  }
  memcpy(ctx->protocols, protocol, ctx->nb_protocols * sizeof(Csec_protocol));
  ctx->current_protocol = 0;
  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  
  return 0;
} /* Csec_initialize_protocols_from_list */

