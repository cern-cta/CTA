/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_protocol_policy.c,v $ $Revision: 1.11 $ $Date: 2005/12/12 15:24:36 $ IT/ADC/CA Benjamin Couturier";
#endif
		
/*
 * Csec_protocol_policy.c 
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <string.h>
#include "serrno.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include <osdep.h>
#include "Csec.h"
#include "Csec_protocol_policy.h"


EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));
static int DLL_DECL _is_proto_deleg_able _PROTO((char *));
static int DLL_DECL _add_to_bigbuf _PROTO((char *,csec_buffer_desc *, size_t *, char *, char **));
static int DLL_DECL _check_short_resp _PROTO((char *,csec_buffer_desc *, char *));

#define TMPBUFSIZE 1500
#define MAXNETLISTLEN 1024

static int _add_to_bigbuf(char *func, csec_buffer_desc *bigbuf, size_t *bigbuf_size, char *tmpbuffer, char **pp) {
  size_t size = *pp - tmpbuffer;
  if (bigbuf->length + size > *bigbuf_size) {
    void *new;
    size_t new_size;

    new_size = *bigbuf_size * 2;
    if (new_size < bigbuf->length + size) new_size = bigbuf->length + size;

    new = realloc((void *)(bigbuf->value), new_size);
    if (new == NULL) {
      Csec_errmsg(func, "Could not allocate memory for buffer");
      serrno = ENOMEM;
      free(bigbuf->value);
      return -1;
    }
    bigbuf->value = new;
    *bigbuf_size = new_size;
  }

  memcpy((char *)bigbuf->value + bigbuf->length, tmpbuffer, size);
  bigbuf->length += size;
  *pp = tmpbuffer;

  return 0;
}

/* Indicates whether authentication protocol, proto, is able to support delegation
   It is assumed that if delegation is supported it is optional and may be enabled
   or disabled
*/
static int _is_proto_deleg_able(char *proto) {
  if (strcmp(proto,"GSI")==0) {
    return 1;
  } else {
    return 0;
  }
}

/* Builds the lsit of protocols to consider for the connection from
 * the total list available. Takes into account whether delegation is
 * required
 */
int Csec_setup_protocols_to_offer(Csec_context_t *ctx) {
  char *func="Csec_setup_protocols_to_offer";
  int i, nb_protocols;

  Csec_trace(func, "Checking which protocols to offer\n");

  if (ctx->protocols != NULL) {
    free(ctx->protocols);
    ctx->protocols = NULL;
    ctx->nb_protocols = 0;
  }

  nb_protocols=0;
  for(i=0;i<ctx->nb_total_protocols;i++) {
    if (!(ctx->sec_flags & CSEC_OPT_DELEG_FLAG) || _is_proto_deleg_able(ctx->total_protocols[i].id)) {
      nb_protocols++;
    }
  }

  ctx->protocols = malloc(sizeof(Csec_protocol) * nb_protocols);
  if (ctx->protocols == NULL) {
    Csec_errmsg(func, "Could not allocate memory for buffer");
    serrno = ENOMEM;
    return -1;
  }

  ctx->nb_protocols = nb_protocols;
  nb_protocols=0;
  for(i=0;i<ctx->nb_total_protocols;i++) {
    if (!(ctx->sec_flags & CSEC_OPT_DELEG_FLAG) || _is_proto_deleg_able(ctx->total_protocols[i].id)) {
      memcpy(&ctx->protocols[nb_protocols], &ctx->total_protocols[i], sizeof(Csec_protocol));
      nb_protocols++;
    }
  }

  Csec_trace(func, "Out of a possible %d will offer %d\n",ctx->nb_total_protocols,ctx->nb_protocols);

  return 0;
}
  

/**
 * Returns the list of protocols available to the client.
 */
int Csec_client_lookup_protocols(Csec_protocol **protocols, int *nbprotocols) {

  char *p, *q, *tokctx;
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
    serrno = 0;
  }

  Csec_trace(func, "Protocols looked up are <%s>\n", p);
  
  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d",
		strlen(p)+1);
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &tokctx);
  while (q  != NULL) {
    if (strlen(q) > 0) entry++;
    q = strtok_r(NULL, " \t", &tokctx);
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == prots) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d",
		entry * sizeof(Csec_protocol));
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &tokctx);
  while (q != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, CA_MAXCSECPROTOLEN);
      q = strtok_r(NULL, " \t", &tokctx);
      entry++;
    }
  }

  *protocols = prots;
  free(buf);
  return 0;
}

/**
 * Creates the list of authorized protocols for a given client, possibly depending
 * on its IP address. Currently doesn't support differentiating the protocol lists
 * by address.
 */
int Csec_server_lookup_protocols(long client_address,
				 Csec_protocol **protocols,
				 int *nbprotocols) {
  char *p, *q, *tokctx;
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
    serrno = 0;
  }

  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    serrno = ENOMEM;
    Csec_errmsg(func, "Error allocating buffer of size %d",
		strlen(p)+1);
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &tokctx);
  while (q  != NULL) {
    if (strlen(q) > 0) entry++;
    q = strtok_r(NULL, " \t", &tokctx);
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == prots) {
    serrno = ENOMEM;
    Csec_errmsg(func, "Error allocating buffer of size %d",
		entry * sizeof(Csec_protocol));
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  q = strtok_r(buf, " \t", &tokctx);
  while (q != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, CA_MAXCSECPROTOLEN);
      q = strtok_r(NULL, " \t", &tokctx);
      entry++;
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
    Csec_errmsg(func, "Could not get peer name: %s", strerror(errno));
    return -1;
  }

  rc = Csec_server_lookup_protocols(from.sin_addr.s_addr,
  		      &(ctx->total_protocols), 
		      &(ctx->nb_total_protocols));
  if (rc != 0) {
    Csec_errmsg(func, "Could not get security protocols for client IP: %s",
	 inet_ntoa (from.sin_addr));
    return rc;
  }
  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;

  return 0;
}

static int _check_short_resp(char *func, csec_buffer_desc *buff, char *p) {
  if (p - (char *)(buff->value) > buff->length) {
    Csec_errmsg(func, "Response from the client was short");
    free(buff->value);
    serrno = ESEC_BAD_PEER_RESP;
    return -1;
  }

  return 0;
}

/**
 * Takes request from client, and sends the response
 */
int Csec_server_negociate_protocol(int s, int timeout, Csec_context_t *ctx, char *init_buf, int init_len) {
  char *func = "Csec_server_negociate_protocol";
  size_t bigbuf_size = 200;
  csec_buffer_desc bigbuf;
  char tmpbuffer[TMPBUFSIZE+1];
  int i,j,rc;
  int selected_peer_protocol;
  unsigned long l, version, *peer_flags, failure_reason;
  int received_token_type;
  char *p;

  Csec_trace(func, "Entering\n");

  bigbuf.length = init_len;
  if (init_len) bigbuf.value = init_buf;

  if (_Csec_recv_token(s, &bigbuf, timeout, &received_token_type)<0) {
    Csec_errmsg(func, "Could not read protocol token");
    return -1;
  }

  if (received_token_type != CSEC_TOKEN_TYPE_PROTOCOL_REQ) {
    Csec_errmsg(func, "Token has type %d instead of %d",
                received_token_type, CSEC_TOKEN_TYPE_PROTOCOL_REQ);
    free(bigbuf.value);
    serrno = ESEC_BAD_PEER_RESP;
    return -1;
  }

  p = (char *)bigbuf.value;
  unmarshall_LONG(p, ctx->peer_version);

  unmarshall_LONG(p, l);
  if (l) {
    unmarshall_STRINGN(p, ctx->client_authorization_mech, CA_MAXCSECPROTOLEN);
    unmarshall_STRINGN(p, ctx->client_authorization_id, CA_MAXCSECNAMELEN);
    ctx->client_authorization_mech[CA_MAXCSECPROTOLEN] = '\0';
    ctx->client_authorization_id[CA_MAXCSECNAMELEN] = '\0';
    ctx->flags |= CSEC_CTX_AUTHID_AVAIL;
  }

  unmarshall_LONG(p, l);
  Csec_trace(func, "Client offering %d protocols\n",l);

  if (l>0 && l<=MAXNETLISTLEN) {
    ctx->nb_peer_protocols = l;
    ctx->peer_protocols = malloc(sizeof(Csec_protocol) * ctx->nb_peer_protocols);
    if (ctx->peer_protocols == NULL) {
      Csec_errmsg(func, "Could not allocate memory for buffer");
      free(bigbuf.value);
      serrno = ENOMEM;
      return -1;
    }
  } else if (l != 0)  {
    free(bigbuf.value);
    Csec_errmsg(func, "Client sent too many protocols");
    serrno = ESEC_BAD_PEER_RESP;
    return -1;
  } else {
    ctx->nb_peer_protocols = 0;
    ctx->peer_protocols = NULL;
  }

  for(i=0;i<ctx->nb_peer_protocols;i++) {
    unmarshall_STRINGN(p, ctx->peer_protocols[i].id, CA_MAXCSECPROTOLEN);
    ctx->peer_protocols[i].id[CA_MAXCSECPROTOLEN] = '\0';
    if (_check_short_resp(func,&bigbuf, p)<0) {
      return -1;
    }
  }

  peer_flags = NULL;
  if (ctx->nb_peer_protocols>0) {
    peer_flags = (unsigned long *) calloc(ctx->nb_peer_protocols, sizeof(unsigned long));
    if (peer_flags == NULL) {
      Csec_errmsg(func, "Could not allocate memory for buffer");
      free(bigbuf.value);
      serrno = ENOMEM;
      return -1;
    }

    /* number of sets of proto flags */
    unmarshall_LONG(p, l);

    if (l>MAXNETLISTLEN) {
      free(bigbuf.value);
      free(peer_flags);
      Csec_errmsg(func, "Client sent too many sets of flags");
      serrno = ESEC_BAD_PEER_RESP;
      return -1;
    }

    for(i=0;i<l;i++) {
      unsigned long flags,nindexes;
      unmarshall_LONG(p, flags);
      unmarshall_LONG(p, nindexes);

      if (nindexes>MAXNETLISTLEN) {
        free(bigbuf.value);
        free(peer_flags);
        Csec_errmsg(func, "Client sent too many indexes");
        serrno = ESEC_BAD_PEER_RESP;
        return -1;
      }

      for(j=0;j<nindexes;j++) {
        unsigned long index;
        unmarshall_LONG(p, index);

        if (_check_short_resp(func,&bigbuf, p)<0) {
          free(peer_flags);
          return -1;
        }

        if (index < ctx->nb_peer_protocols) {
          peer_flags[index] |= flags;
        }
      }
    }
  } /* ctx->nb_peer_protocols>0 */

  if (_check_short_resp(func,&bigbuf, p)<0) {
    if (peer_flags != NULL) free(peer_flags);
    return -1;
  }

  free(bigbuf.value);

  /* Prune our list of protocols, depending on whether we
     require delegation and the protocol supports it */

  if (Csec_setup_protocols_to_offer(ctx)<0) {
    if (peer_flags != NULL) free(peer_flags);
    return -1;
  }

  /* now find common protocol, if possible */
  /* Checking the requested protocols with the list of allowed ones & consistency of delegation req */

  selected_peer_protocol = -1;
  ctx->current_protocol = -1;

  for(i=0; i<ctx->nb_peer_protocols && ctx->current_protocol < 0; i++) {
    for(j=0; j<ctx->nb_protocols && ctx->current_protocol < 0; j++) {
      Csec_trace(func, "Comparing with client protocol <%s> with server <%s>\n", ctx->peer_protocols[i].id,ctx->protocols[j].id );
      if(strcmp(ctx->peer_protocols[i].id, ctx->protocols[j].id) == 0) {
        /* Are the delegation requirements incompatible? */
        if (((ctx->sec_flags & CSEC_OPT_DELEG_FLAG)   && (peer_flags[i] & CSEC_OPT_NODELEG_FLAG)) ||
            ((ctx->sec_flags & CSEC_OPT_NODELEG_FLAG) && (peer_flags[i] & CSEC_OPT_DELEG_FLAG))) {
          continue;
        }

        ctx->peer_sec_flags = peer_flags[i];

        if ((ctx->sec_flags & CSEC_OPT_DELEG_FLAG) || (peer_flags[i] & CSEC_OPT_DELEG_FLAG)) {
          ctx->peer_sec_flags |= CSEC_OPT_DELEG_FLAG;
          ctx->sec_flags |= CSEC_OPT_DELEG_FLAG;
        } else {
          ctx->peer_sec_flags |= CSEC_OPT_NODELEG_FLAG;
          ctx->sec_flags |= CSEC_OPT_NODELEG_FLAG;
        }

        ctx->current_protocol = j;
        selected_peer_protocol = i;

	Csec_trace(func, "Accepting protocol <%s>\n", ctx->protocols[ctx->current_protocol].id);
      }
    }
  } 

  if (peer_flags != NULL) free(peer_flags);

  bigbuf.value = (char *)malloc(bigbuf_size);
  if (bigbuf.value == NULL) {
    Csec_errmsg(func, "Could not allocate memory for buffer");
    serrno = ENOMEM;
    return -1;
  }
  bigbuf.length = 0;

  /* Construct reply packet for client */

  /* Version */

  p = tmpbuffer;
  version = CSEC_VERSION;
  marshall_LONG(p, version);

  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  /* Indicate whether we were able to agree on one of the offered protocols */
  if (selected_peer_protocol >= 0) {
    marshall_STRING(p, PROT_REQ_OK);
    ctx->protocol_negociation_status = PROT_STAT_OK;
  } else {
    marshall_STRING(p, PROT_REQ_NOK);
    ctx->protocol_negociation_status = PROT_STAT_NOK;
    failure_reason = ESEC_PROTNOTSUPP;
    marshall_LONG(p, failure_reason);
  }

  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  if (ctx->protocol_negociation_status == PROT_STAT_OK) {
    marshall_LONG(p, ((unsigned long)selected_peer_protocol));
    marshall_LONG(p, ctx->peer_sec_flags);
  } else {
    /* List of possible protocols to send to the client */
    marshall_LONG(p, ((unsigned long)ctx->nb_protocols));

    for(i=0;i<ctx->nb_protocols;i++) {
      marshall_STRING(p, ctx->protocols[i].id);
      if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
        return -1;
      }
    }

    i = (ctx->sec_flags | (CSEC_OPT_DELEG_FLAG & CSEC_OPT_NODELEG_FLAG)) ? 1 : 0;

    if (ctx->nb_protocols>0 && i) {
      /* We have a requirement. We apply this flag to all the protos we offer.
         If we require delegation those protocols which don't support it
         have already been removed */

      marshall_LONG(p, 1L);
      marshall_LONG(p, ctx->sec_flags);
      marshall_LONG(p, ((unsigned long)ctx->nb_protocols));
      for(i=0;i<ctx->nb_protocols;i++) {
        marshall_LONG(p, ((unsigned long)i));
        if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
          return -1;
        }
      }
    } else if (ctx->nb_protocols>0) {
      /* We don't have any special requirements on delegation. But must indicate
         that no delegation is possible on those protocols that don't support it */
      j = 0;
      for(i=0;i<ctx->nb_protocols;i++) {
        if (!_is_proto_deleg_able(ctx->protocols[i].id)) j++;
      }
      if (j) {
        marshall_LONG(p, 1L);
        marshall_LONG(p, ((unsigned long)(ctx->sec_flags | CSEC_OPT_NODELEG_FLAG)));
        marshall_LONG(p, ((unsigned long)j));
         for(i=0;i<ctx->nb_protocols;i++) {  
          if (!_is_proto_deleg_able(ctx->protocols[i].id)) marshall_LONG(p, ((unsigned long)i));
        }
      } else {
        marshall_LONG(p, 0L);
      }
    }
  }
  
  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  /* Send the reply packet to the client */

  Csec_trace(func, "Sending %d bytes\n", bigbuf.length);
  
  rc = _Csec_send_token(s, &bigbuf, timeout, CSEC_TOKEN_TYPE_PROTOCOL_RESP);

  free(bigbuf.value);

  if (rc)
    return rc;

  if (ctx->protocol_negociation_status != PROT_STAT_OK) {
    if (failure_reason == ESEC_PROTNOTSUPP) {
      serrno = ESEC_PROTNOTSUPP;
    } else {
      serrno = EINVAL;
    }
    Csec_errmsg(func,"Could not negociate an authentication method with the client");
    return -1;
  }

  return 0;
}


int Csec_client_negociate_protocol(int s, int timeout, Csec_context_t *ctx) {
  char *func = "Csec_client_negociate_protocol";
  size_t bigbuf_size = 200;
  csec_buffer_desc bigbuf;
  char tmpbuffer[TMPBUFSIZE+1];
  int received_token_type;
  unsigned long l, version, failure_reason;
  int i,j,rc;
  char *p;

  Csec_trace(func,"Entering\n");

  /* Prune our list of protocols, depending on whether we
     require delegation and the protocol supports it */

  if (Csec_setup_protocols_to_offer(ctx)<0) {
    return -1;
  }

  bigbuf.value = (char *)malloc(bigbuf_size);
  if (bigbuf.value == NULL) {
    Csec_errmsg(func, "Could not allocate memory for buffer");
    serrno = ENOMEM;
    return -1;
  }
  bigbuf.length = 0;

  /* Construct request packet for server */

  /* Version */

  p = tmpbuffer;
  version = CSEC_VERSION;
  marshall_LONG(p, version);

  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  /* Authorization Id to send to the server */
  
  i = (ctx->flags & CSEC_CTX_AUTHID_AVAIL) ? 1 : 0;
  marshall_LONG(p, ((unsigned long)i));

  if (i) {
    marshall_STRING(p, ctx->client_authorization_mech);
    marshall_STRING(p, ctx->client_authorization_id);
  }

  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  /* List of possible protocols to send to the server */

  marshall_LONG(p, ((unsigned long)ctx->nb_protocols));
  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }

  for(i=0;i<ctx->nb_protocols;i++) {
    marshall_STRING(p, ctx->protocols[i].id);
    if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
      return -1;
    }
  }

  /* Proto flags to assign to the list of protocols we are sending.
     At the moment the flags are only the delegation YES/NO indicators */
  i = (ctx->sec_flags | (CSEC_OPT_DELEG_FLAG & CSEC_OPT_NODELEG_FLAG)) ? 1 : 0;

  if (ctx->nb_protocols>0 && i) {
    /* We have a requirement. We apply this flag to all the protos we offer.
       If we require delegation those protocols which don't support it
       have already been removed */

    marshall_LONG(p, 1L);
    marshall_LONG(p, ctx->sec_flags);
    marshall_LONG(p, ((unsigned long)ctx->nb_protocols));
    for(i=0;i<ctx->nb_protocols;i++) {
      marshall_LONG(p, ((unsigned long)i));
    }
  } else if (ctx->nb_protocols>0) {
    /* We don't have any special requirements on delegation. But must indicate
       that no delegation is possible on those protocols that don't support it */
    j = 0;
    for(i=0;i<ctx->nb_protocols;i++) {
      if (!_is_proto_deleg_able(ctx->protocols[i].id)) j++;
    }
    if (j) {
      marshall_LONG(p, 1L);
      marshall_LONG(p, ((unsigned long)(ctx->sec_flags | CSEC_OPT_NODELEG_FLAG)));
      marshall_LONG(p, ((unsigned long)j));
       for(i=0;i<ctx->nb_protocols;i++) {  
        if (!_is_proto_deleg_able(ctx->protocols[i].id)) marshall_LONG(p, ((unsigned long)i));
      }
    } else {
      marshall_LONG(p, 0L);
    }
  }
  
  if (_add_to_bigbuf(func, &bigbuf, &bigbuf_size, tmpbuffer, &p) < 0) {
    return -1;
  }
  
  /* Send the packet to the server */

  Csec_trace(func, "Sending %d bytes\n", bigbuf.length);
  
  rc = _Csec_send_token(s, &bigbuf, timeout, CSEC_TOKEN_TYPE_PROTOCOL_REQ);

  free(bigbuf.value);

  if (rc)
    return rc;

  /* now read reply */

  bigbuf.length = 0;
  if (_Csec_recv_token(s, &bigbuf, timeout, &received_token_type)<0) {
    Csec_errmsg(func, "Could not read protocol token");
    return -1;
  }
   
  if (received_token_type != CSEC_TOKEN_TYPE_PROTOCOL_RESP) {
    Csec_errmsg(func, "Token has type %d instead of %d",
                received_token_type, CSEC_TOKEN_TYPE_PROTOCOL_RESP);
    free(bigbuf.value);
    serrno = ESEC_BAD_PEER_RESP;
    return -1;
  }

  p = (char *)bigbuf.value;
  unmarshall_LONG(p, ctx->peer_version);

  unmarshall_STRINGN(p, tmpbuffer, TMPBUFSIZE);
  tmpbuffer[TMPBUFSIZE] = '\0';

  if (strcmp(tmpbuffer, PROT_REQ_OK)==0) {
    /* Server agreed */
    ctx->protocol_negociation_status = PROT_STAT_OK;
    unmarshall_LONG(p, l);
    ctx->current_protocol = l;
    unmarshall_LONG(p, ctx->peer_sec_flags);

    /* Server must respond with a definite delegation YES/NO */
    /* Also check the response is consistent with what we wanted */
    if ( !(ctx->peer_sec_flags & (CSEC_OPT_DELEG_FLAG | CSEC_OPT_NODELEG_FLAG)) ||
        ((ctx->sec_flags & CSEC_OPT_DELEG_FLAG)   && (ctx->peer_sec_flags & CSEC_OPT_NODELEG_FLAG)) ||
        ((ctx->sec_flags & CSEC_OPT_NODELEG_FLAG) && (ctx->peer_sec_flags & CSEC_OPT_DELEG_FLAG))) {
      free(bigbuf.value);
      Csec_errmsg(func, "Server responded with inconsistent delegation requirements");
      serrno = ESEC_BAD_PEER_RESP;
      return -1;
    } else {
      /* Make sure we have the definite delegation decision */
      if ((ctx->sec_flags & CSEC_OPT_DELEG_FLAG) || (ctx->peer_sec_flags & CSEC_OPT_DELEG_FLAG)) {
        ctx->sec_flags |= CSEC_OPT_DELEG_FLAG;
      } else {
        ctx->sec_flags |= CSEC_OPT_NODELEG_FLAG;
      }
    }
  } else {
    /* Server rejected our request. It should indicate the list of protocols it
       would accept from us */
    ctx->protocol_negociation_status = PROT_STAT_NOK;
    unmarshall_LONG(p, failure_reason); /* Failure reason */
    unmarshall_LONG(p, l);
    if (l>0 && l<=MAXNETLISTLEN) {
      ctx->nb_peer_protocols = l;
      ctx->peer_protocols = malloc(sizeof(Csec_protocol) * ctx->nb_peer_protocols);
      if (ctx->peer_protocols == NULL) {
        Csec_errmsg(func, "Could not allocate memory for buffer");
        free(bigbuf.value);
        serrno = ENOMEM;
        return -1;
      }
    } else if (l != 0) {
      free(bigbuf.value);
      Csec_errmsg(func, "Client sent too many protocols");
      serrno = ESEC_BAD_PEER_RESP;
      return -1;
    } else {
      ctx->nb_peer_protocols = 0;
      ctx->peer_protocols = NULL;
    }

    for(i=0;i<ctx->nb_peer_protocols;i++) {
      unmarshall_STRINGN(p, ctx->peer_protocols[i].id, CA_MAXCSECPROTOLEN);
      ctx->peer_protocols[i].id[CA_MAXCSECPROTOLEN] = '\0';
      if (_check_short_resp(func,&bigbuf, p)<0) {
        return -1;
      }
    }

    if (ctx->nb_peer_protocols > 0) {
      /* The list of protos flags. At the moment we don't do anything with these */
      unmarshall_LONG(p, l);

      if (l>MAXNETLISTLEN) {
        free(bigbuf.value);
        Csec_errmsg(func, "Server sent too many sets of flags");
        serrno = ESEC_BAD_PEER_RESP;
        return -1;
      }

      for(i=0;i<l;i++) {
        unsigned long flags,nindexes;
        unmarshall_LONG(p, flags);
        unmarshall_LONG(p, nindexes);

        if (nindexes>MAXNETLISTLEN) {
          free(bigbuf.value);
          Csec_errmsg(func, "Server sent too many indexes");
          serrno = ESEC_BAD_PEER_RESP;
          return -1;
        }

        for(j=0;j<nindexes;j++) {
          unsigned long index;
          unmarshall_LONG(p, index);

          if (_check_short_resp(func,&bigbuf, p)<0) {
            return -1;
          }

        }
      }    
    } /* ctx->nb_peer_protocols > 0 */
  }

  if (_check_short_resp(func,&bigbuf, p)<0) {
    return -1;
  }

  free(bigbuf.value);

  /* This block is just to log some debug/set error message information about the protocol exchange */
  {
    char local_protos[TMPBUFSIZE+1], peer_protos[TMPBUFSIZE+1];

    p = local_protos;
    p[0] = '\0';    
    for(i=0;i<ctx->nb_protocols;i++) {
      p += snprintf(p, (local_protos+TMPBUFSIZE) - p, "%s ", ctx->protocols[i].id);
    }
    if (p-- && strlen(local_protos)>0 && *p==' ') *p='\0';
  
    /* NB The peer only sends its list if it didn't match one of our offered protos */
    p = peer_protos;
    p[0] = '\0';
    for(i=0;i<ctx->nb_peer_protocols;i++) {
      p += snprintf(p, (peer_protos+TMPBUFSIZE) - p, "%s ", ctx->peer_protocols[i].id);
    }
    if (p-- && strlen(peer_protos)>0 && *p==' ') *p='\0';

    if (ctx->protocol_negociation_status == PROT_STAT_NOK) {
      if (failure_reason == ESEC_PROTNOTSUPP) {
        if (strlen(local_protos)==0 && strlen(peer_protos)==0) {
          Csec_errmsg(func, "Neither server nor client have any protocols to offer");
        } else if (strlen(peer_protos)==0) {
          Csec_errmsg(func, "Server has no protocols to offer, we offered %s",local_protos);
        } else if (strlen(local_protos)==0) {
          Csec_errmsg(func, "Server offered %s, but we had no local protocols to offer",peer_protos);
        } else {    
          Csec_errmsg(func, "Server/Client could not agree on a common protocol and/or delegation requirements");
        }
      }
    } else {
      Csec_trace(func, "Server chose %s\n",ctx->protocols[ctx->current_protocol].id);
    }
  }

  if (ctx->protocol_negociation_status == PROT_STAT_NOK) {
    if (failure_reason == ESEC_PROTNOTSUPP) {
      serrno = ESEC_PROTNOTSUPP;
    } else {
      Csec_errmsg(func,"Could not negociate an authentication method with the server");
      serrno = EINVAL;
    }
    return -1;
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
    Csec_errmsg(func, "NULL parameter ctx:%p protocols:%p",
		ctx, protocol);
    return -1;
  }
  
  for (i = 0; p[i].id[0] != '\0'; i++);
  /* BEWARE, empty loop */
  
  ctx->nb_total_protocols = i;
  ctx->total_protocols = (Csec_protocol *)malloc(ctx->nb_total_protocols * sizeof(Csec_protocol));
  if (ctx->total_protocols == NULL) {
    serrno = ESEC_NO_SECPROT;
    Csec_errmsg(func, "Error allocating buffer of size %d",
		ctx->nb_total_protocols * sizeof(Csec_protocol));
      return -1;
  }
  memcpy(ctx->total_protocols, protocol, ctx->nb_total_protocols * sizeof(Csec_protocol));
  ctx->current_protocol = -1;
  ctx->flags |= CSEC_CTX_PROTOCOL_LOADED;
  
  return 0;
} /* Csec_initialize_protocols_from_list */
