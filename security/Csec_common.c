/*
 * $id$
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_api_common.c - Common functions in the Csecurity API 
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_api_common.c,v 1.1 2004/01/12 10:31:39 CERN IT/ADC/CA Benjamin Couturier";
#endif

#include <stdlib.h>
#include <serrno.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <Cnetdb.h>
#include <unistd.h>
#include <Cpwd.h>
#include <socket_timeout.h>
#include <Csec_api.h>
#include <Csec_common.h>

int Cdomainname(char *name, int namele);
EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

/* *********************************************** */
/* Csec_context_t Utilities                        */
/*                                                 */
/* *********************************************** */

Csec_context_t *_Csec_init_context(Csec_context_t *ctx, 
				   enum Csec_context_type type,
				   enum Csec_service_type service_type) {
  char *func = "_Csec_init_context";

  _Csec_trace(func, "Entering\n");

  memset(ctx, '\0', sizeof(Csec_context_t));
  ctx->magic = (type == CSEC_CONTEXT_SERVER
		?CSEC_CONTEXT_MAGIC_SERVER_1
		:CSEC_CONTEXT_MAGIC_CLIENT_1);
  ctx->context_type = type;
  ctx->service_type = service_type;
  ctx->flags = CSEC_CTX_INITIALIZED|CSEC_CTX_SERVICE_TYPE_SET;
  ctx->delegated_credentials.value = NULL;
  ctx->delegated_credentials.length = 0;
  ctx->peer_username = NULL;
  ctx->peer_uid = -1;
  ctx->peer_gid = -1;
  return ctx;
}

Csec_context_t*_Csec_create_context(enum Csec_context_type type,
				    enum Csec_service_type service_type) {
  char *func = "_Csec_create_context";

  Csec_context_t *ctx = malloc(sizeof(Csec_context_t));
  if (ctx == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for context structure");
    return NULL;
  }
  return _Csec_init_context(ctx, type, service_type);
}

void _Csec_delete_context(Csec_context_t *ctx) {
  _Csec_clear_context(ctx);
  free(ctx);
}


void _Csec_clear_context(Csec_context_t *ctx) {

  char *func = "_Csec_delete_context";

  _Csec_trace(func, "Entering\n");

  int i;
  for(i=0; i<ctx->nb_supported_protocols; i++) {
    _Csec_delete_protocol(ctx->supported_protocols[i]);
  }
  
  for(i=0; i<ctx->nb_peer_protocols; i++) {
    _Csec_delete_protocol(ctx->peer_protocols[i]);
  }
  
  _Csec_delete_id(ctx->local_id);
  _Csec_delete_id(ctx->peer_id);
  _Csec_delete_id(ctx->authorization_id);

  /* Cleanup the plugin context and the plugin itself */
  if (ctx->plugin != NULL) {
    ctx->plugin->Csec_plugin_clearContext(ctx->plugin_context);
    _Csec_unload_plugin(ctx->plugin);
  }

  /* Cleanup the delegated credentials if they have been exported */
  if (ctx->delegated_credentials.value) 
    free(ctx->delegated_credentials.value);

  if(ctx->peer_username) {
    free(ctx->peer_username);
  }
}


int _Csec_check_context(Csec_context_t *ctx, char *func) {
  if (ctx == NULL) {
    _Csec_errmsg(func, "Context is NULL");
    serrno = EINVAL;
    return -1;
  }
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    _Csec_errmsg(func, "Context not initialized");
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }
  return 0;
}



/* *********************************************** */
/* Csec_protocol_t Utilities                       */
/*                                                 */
/* *********************************************** */


Csec_protocol_t *_Csec_create_protocol(const char *prot) {
  Csec_protocol_t *tmp;
  char *func = "_Csec_create_protocol";


  if (prot == NULL) return NULL;

  tmp  = calloc(sizeof(Csec_protocol_t), 1);
  if (tmp == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for protocol structure");
    return NULL;
  }

  tmp->name = (char *)strdup(prot);
  if (tmp->name == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for protocol string");
    return NULL;
  }
  
  return tmp;
}


void _Csec_delete_protocol(Csec_protocol_t *prot) {
  if (prot == NULL) return;
  if (prot->name != NULL) free(prot->name);
  free(prot);
}


Csec_protocol_t *_Csec_dup_protocol(const Csec_protocol_t *prot) {
  if (prot == NULL) return NULL;
  return _Csec_create_protocol(prot->name);
}

char *_Csec_protocol_name(const Csec_protocol_t *prot) {
  return prot->name;
}

Csec_protocol_t *_Csec_init_protocol(Csec_protocol_t *protocol,
				     const char *name) {
  char *func = "_Csec_init_protocol";
  protocol->name = (char *)strdup(name);
  if (protocol->name == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for protocol string");
    return NULL;
  }
  
  return protocol;
}

int _Csec_protocol_cmp(const Csec_protocol_t *prot1,
		       const Csec_protocol_t *prot2) {
  if (prot1 == NULL || prot2 == NULL) return -1;
  return strcmp(prot1->name, prot2->name);
}




/* *********************************************** */
/* Csec_id_t Utilities                             */
/*                                                 */
/* *********************************************** */

Csec_id_t *_Csec_create_id(const char *mech, 
			   const char *name) {

  Csec_id_t *tmp;
  char *func = "_Csec_create_id";

  tmp  = calloc(sizeof(Csec_id_t), 1);
  if (tmp == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for id structure");
    return NULL;
  }

  tmp->mech = (char *) strdup(mech);
  if (tmp->mech == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for mech string");
    return NULL;
  }
 
  tmp->name = (char *)strdup(name);
  if (tmp->name == NULL) {
    serrno = SEINTERNAL;
    _Csec_errmsg(func, "Could not allocate memory for name string");
    return NULL;
  }
 
  return tmp;
}

Csec_id_t *_Csec_dup_id(const Csec_id_t *id) {
  if (id == NULL) return NULL;
  return _Csec_create_id(id->mech, id->name);
}

void _Csec_delete_id(Csec_id_t *id) {
  if (id == NULL) return;
  if (id->mech != NULL) free(id->mech);
  if (id->name != NULL) free(id->name);
  free(id);
}


char * _Csec_id_mech(const Csec_id_t *id) {
  if (id == NULL) return NULL;
  return id->mech;
}

char * _Csec_id_name(const Csec_id_t *id) {
  if (id == NULL) return NULL;
  return id->name;
}





/* *********************************************** */
/* TRACE Utilities                                 */
/*                                                 */
/* *********************************************** */

/**
 * Checks the environment to setup the trace mode,
 * if CSEC_TRACE is set
 * If CSEC_TRACEFILE is set, the output is written to that file,
 * otherwise, it is sent to stderr.
 */
int _Csec_setup_trace() {
  char *envar;

  struct Csec_api_thread_info *thip;

  if (_Csec_apiinit (&thip))
    return  -1;

  thip->trace_mode=0;
  thip->trace_file[0]= thip->trace_file[CA_MAXNAMELEN]= '\0';

  envar = getenv(CSEC_TRACE);
  if (envar != NULL) {
    thip->trace_mode=1;
    envar = getenv(CSEC_TRACEFILE);
    if (envar != NULL) {
      strncpy(thip->trace_file, envar, CA_MAXNAMELEN);
    }
  }

  return 0;
}


int _Csec_trace(char *func, char *msg, ...) {
  va_list args;
  char prtbuf[SECPRTBUFSZ+1];
  struct Csec_api_thread_info *thip;
  int funlen;

  if (_Csec_apiinit (&thip)) {
    return  -1;
  }

  if (!thip->trace_mode) {
    return 0;
  }

  va_start (args, msg);
  if (func)
    sprintf (prtbuf, "%s: ", func);
  else
    *prtbuf = '\0';
  funlen = strlen(prtbuf);

  vsnprintf (prtbuf + funlen ,  SECPRTBUFSZ - funlen -1, msg, args);
  prtbuf[SECPRTBUFSZ]='\0';

  if (thip->trace_file[0] != '\0') {
    int fd;
    fd = open(thip->trace_file, O_CREAT|O_WRONLY|O_APPEND, 0666);
    if (fd <0) return -1;
    write(fd, prtbuf, strlen(prtbuf));
    close(fd);

  } else {
    fprintf (stderr, "%s", prtbuf);
  }

  return 0;
}


/* *********************************************** */
/* Csec tokens/IO  Utilities                       */
/*                                                 */
/* *********************************************** */

/**
 * Sends a csec_buffer_t over a socket
 */
int _Csec_send_token(int s, 
		     csec_buffer_t tok, 
		     int timeout, 
		     int token_type) {

  int datalen, ret;
  char *func = "_Csec_send_token";
  char headbuf[HEADBUFSIZE];
  char *p;
  int headlen;
  U_LONG magic = CSEC_TOKEN_MAGIC_1;
  /* U_LONG type = CSEC_TOKEN_TYPE_HANDSHAKE; */
  U_LONG type = token_type;
  datalen = tok->length;

 _Csec_trace(func, "Sending packet Magic: %xd Type: %d, Len: %d\n",
	     magic, type, datalen);

  p = headbuf;
  marshall_LONG(p, magic);
  marshall_LONG(p, type);
  marshall_LONG(p, datalen);
  headlen =  p - headbuf;

  _Csec_print_token(tok);
    
  ret = netwrite_timeout(s, headbuf, headlen, timeout);
  if (ret < 0) {
    /* We keep the serrno from netwrite */
    _Csec_errmsg(func, "Error sending token length");
    return -1;
  } else if (ret != headlen) {
    serrno = ESEC_SYSTEM;
    _Csec_errmsg(func, "Bad token length");
    return -1;
  }

  ret = netwrite_timeout(s, tok->value, tok->length, timeout);
  if (ret < 0) {
    /* We keep the serrno from netwrite */
    _Csec_errmsg(func, "Error sending token data");
    return -1;
  } else if (ret != tok->length) {
    serrno = ESEC_SYSTEM;
    _Csec_errmsg(func, "Bad token data length");
    return -1;
  }

  return 0;
}



/**
 * Reads a csec_buffer_t from a socket
 */
int _Csec_recv_token(int s, 
		     csec_buffer_t tok, 
		     int timeout, 
		     int *rtype) {
  int ret;
  char *func = "_Csec_recv_token";
  char headbuf[3 * LONGSIZE];
  char *p;
  int len, headlen;
  int data_already_read=0;
  int header_already_read = 0;
  char *prefetched_data;
  U_LONG magic;
  U_LONG type;

  headlen = 3 * LONGSIZE;

  /* In this case some data has already been prefetched into tok,
     and we consider this was the first bytes sent by the client */

 _Csec_trace(func, "Entering. tok->length: %d\n", tok->length);

  if (tok->length >= headlen) {
    /* More (or equal) the header was already read */
    data_already_read = tok->length - headlen;
   _Csec_trace(func,
	       "Header already read. Nb bytes Data already read: %d\n",
	       data_already_read);
    memcpy(headbuf, tok->value, headlen);
    if (data_already_read > 0) {
      prefetched_data = (char *)malloc(data_already_read);
      if (prefetched_data == NULL) {
	serrno = ESEC_SYSTEM;
	_Csec_errmsg(func, "Could not allocate space for token");
	return -1;
      }
      memcpy(prefetched_data, (char *)tok->value + headlen, data_already_read);
    }
    free(tok->value);


  } else if (tok->length > 0 && tok->length < headlen) {
    /* Less than the header was read */
    header_already_read = tok->length;
   _Csec_trace(func, "Bytes of header already read: %d\n",
	       header_already_read);
    memcpy(headbuf, tok->value, header_already_read);
    free(tok->value);

    /* Nothing was read */
    ret = netread_timeout(s, headbuf + header_already_read,
			  headlen - header_already_read, timeout);

    if (ret < 0) {
      /* We keep the serrno from netread */
      _Csec_errmsg(func, "Error reading token length");
      return -1;
    } else if (ret == 0) {
      serrno = ESEC_SYSTEM;
      _Csec_errmsg(func, "Connection dropped by remote end");
      return -1;
    } else if (ret !=  headlen - header_already_read) {
      serrno = ESEC_SYSTEM;
      _Csec_errmsg(func, "Bad header length: %d",
		  header_already_read + ret);
      return -1;
    }


  } else {
    /* Nothing was read */
   _Csec_trace(func, "Nothing was prefetched\n");
    ret = recv(s, headbuf, headlen, 0);
    /* ret = netread_timeout(s, headbuf, headlen, timeout); */
    if (ret < 0) {
      /* We keep the serrno from netread */
      _Csec_errmsg(func, "Error reading token length");
      return -1;
    } else if (ret == 0) {
      serrno = ESEC_SYSTEM;
      _Csec_errmsg(func, "Connection dropped by remote end");
      return -1;
    } else if (ret != headlen) {
      serrno = ESEC_SYSTEM;
      _Csec_errmsg(func, "Bad token length: %d", ret);
      return -1;
    }

  }

  p = headbuf;
  unmarshall_LONG(p, magic);
  unmarshall_LONG(p, type);
  unmarshall_LONG(p, len);

  if (rtype != NULL) {
    *rtype = type;
  }

 _Csec_trace(func, "Receiving packet Magic: %xd Type: %d, Len: %d\n",
	     magic, type, len);

  if (magic != CSEC_TOKEN_MAGIC_1) {
    serrno = ESEC_BAD_MAGIC;
    _Csec_errmsg(func, "Received magic: %d expecting %d",
		magic, CSEC_TOKEN_MAGIC_1);
    return -1;
  }

  tok->length = len;
  tok->value = (char *) malloc(tok->length + 1); /* Be nice and ensure null at end of data, incase someone expects a string */
  if (tok->value == NULL) {
    serrno = ESEC_SYSTEM;
    _Csec_errmsg(func, "Could not allocate space for token");
    return -1;
  }
  ((char *)tok->value)[tok->length] = '\0';

  if (data_already_read>0) {
    memcpy(tok->value, prefetched_data, data_already_read);
  }

  ret = netread_timeout(s, (char *) ( (char*)tok->value+data_already_read),
			(int)(tok->length - data_already_read), timeout);

  if (ret < 0) {
    /* We keep the serrno from netread */
    _Csec_errmsg(func, "Could not read token data");
    free(tok->value);
    return -1;
  } else if (ret != tok->length - data_already_read) {
    serrno = ESEC_SYSTEM;
    _Csec_errmsg(func, "Bad token data length. Received %d rather than %d",
		ret, tok->length - data_already_read);
    free(tok->value);
    return -1;
  }

  _Csec_print_token(tok);

  return 0;
}


/**
 * Dumps the token in hexa
 */
void _Csec_print_token(csec_buffer_t tok) {
  int i;
  /* int j; */
  unsigned char *p = tok->value;

  for (i=0; i < tok->length; i++, p++) {
   _Csec_trace(NULL, "%02x ", *p);
    if ((i % 16) == 15) {
     _Csec_trace(NULL, "\t        ");
      /*  for (j=15; j >= 0; j--) { */
      /*                  _Csec_trace(NULL, "%c", *(p-j)); */
      /*               } */
     _Csec_trace(NULL, "\n");
    }
  }
 _Csec_trace(NULL, "\n\n");
}



void _Csec_clear_token(csec_buffer_t tok) {
  if (tok != NULL) {
    if (tok->value != NULL) free(tok->value);
  }
}


/* *********************************************** */
/* Protocol lookup/setup Utilities                 */
/*                                                 */
/* *********************************************** */



/**
 * Returns the list of protocols available to the client.
 */
int _Csec_client_lookup_protocols(Csec_protocol_t ***protocols, 
				  int *nbprotocols) {

  char *p, *q, *tokctx;
  char *buf;
  int entry = 0;
  Csec_protocol_t **prots;
  char *func="_Csec_client_lookup_protocols";

  _Csec_trace(func, "Looking up protocols from the environment\n");

  if (protocols == NULL
      || nbprotocols == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL parameter protocols:%p nbprotocols: %p",
		protocols, nbprotocols);
    return -1;
  }

  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv (CSEC_MECH)) 
	|| (p = (char *)getconfent (CSEC_CONF_SECTION, 
				    CSEC_CONF_ENTRY_MECH, 0)))) {
    p = CSEC_DEFAULT_MECHS;
  }

  _Csec_trace(func, "Protocols looked up are <%s>\n", p);
  
  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    serrno = ESEC_NO_SECPROT;
    _Csec_errmsg(func, "Error allocating buffer of size %d",
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
  prots = (Csec_protocol_t **)malloc(entry * sizeof(Csec_protocol_t *));
  if (NULL == prots) {
    serrno = ESEC_NO_SECPROT;
    _Csec_errmsg(func, "Error allocating buffer of size %d",
		entry * sizeof(Csec_protocol_t));
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
      prots[entry] = _Csec_create_protocol(q);
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
int _Csec_server_lookup_protocols(long client_address,
				 Csec_protocol_t ***protocols,
				 int *nbprotocols) {
  char *p, *q, *tokctx;
  char *buf;
  int entry = 0;
  Csec_protocol_t **prots;
  char *func = "_Csec_server_lookup_protocols";
  struct in_addr a;

  if (protocols == NULL
      || nbprotocols == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL parameter protocols:%p nbprotocols: %p",
		 protocols, nbprotocols);
  return -1;
  }


  /* The client parameter is not currently used, but has been added for a later version
     In any case, a client_address of 0 should then load ALL the protocols
     available, independently of the address */

  a.s_addr = client_address;
  _Csec_trace(func, "Looking for allowed security protocols for %s\n", inet_ntoa (a));
  
  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv (CSEC_AUTH_MECH)) 
	|| (p = (char *)getconfent (CSEC_CONF_SECTION, CSEC_CONF_ENTRY_AUTHMECH, 0)))) {
    p = CSEC_DEFAULT_MECHS;
  }

  buf = (char *)malloc(strlen(p)+1);
  if (NULL == buf) {
    serrno = ENOMEM;
    _Csec_errmsg(func, "Error allocating buffer of size %d",
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
  prots = (Csec_protocol_t **)malloc(entry * sizeof(Csec_protocol_t *));
  if (NULL == prots) {
    serrno = ENOMEM;
    _Csec_errmsg(func, "Error allocating buffer of size %d",
		entry * sizeof(Csec_protocol_t *));
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
       prots[entry] = _Csec_create_protocol(q);
      q = strtok_r(NULL, " \t", &tokctx);
      entry++;
    }
  }

  free(buf);
  *protocols = prots;

  return 0;
}

/**
 * Initializes the protocols in the context from a list rather than
 * from the environment variables.
 */
int _Csec_initialize_protocols_from_list(Csec_protocol_t *list,
					 Csec_protocol_t ***protocols, 
					int *nbprotocols) {
    
  int  i;
  Csec_protocol_t *p = list;
  char *func = "_Csec_initialize_protocols_from_list";

  if (list == NULL 
      || protocols == NULL
      || nbprotocols == NULL) {
    serrno = EINVAL;
    _Csec_errmsg(func, "NULL parameter ctx:%p protocols:%p nbprotocols: %p",
		list,  protocols, nbprotocols);
    return -1;
  }
  
  for (i = 0; p[i].name[0] != '\0'; i++);
  /* BEWARE, empty loop */
  
  *nbprotocols = i;
  *protocols = (Csec_protocol_t **)malloc(i * sizeof(Csec_protocol_t *));
  if (*protocols == NULL) {
    serrno = ESEC_NO_SECPROT;
    _Csec_errmsg(func, "Error allocating buffer of size %d",
		i * sizeof(Csec_protocol_t *));
    return -1;
  }

  for(i = 0; i < *nbprotocols; i++) {
    (*protocols)[i] = _Csec_create_protocol(_Csec_protocol_name(&(list[i])));
  }

  
  return 0;
} /* Csec_initialize_protocols_from_list */




/* *********************************************** */
/* Service name utilities                          */
/*                                                 */
/* *********************************************** */

/**
 * Returns the principal that the service on local machine should have
 */
int _Csec_get_local_host_domain(char **host,
				char **domain) {


  char *pos;
  char hostname[CA_MAXHOSTNAMELEN+1];
  char domainname[CA_MAXHOSTNAMELEN+1];
  char *func = "_Csec_get_local_host_domain";
  
  if (host == NULL || domain == NULL) {
    _Csec_errmsg(func, "Host or domain is NULL");
    serrno = EINVAL;
    return -1;
  }

  gethostname(hostname, CA_MAXHOSTNAMELEN);
    
  pos = strchr(hostname, '.');
  if (pos==NULL) {
    /*  We don't have the domain */
    if (Cdomainname(domainname, sizeof(domainname)) <  0) {
      _Csec_errmsg(func, "Could not get domain name: <%s>", sstrerror(serrno));
      serrno = ESEC_SYSTEM;
      return -1;
    }
    
    *host = strdup(hostname);
    *domain = strdup(domainname);

  } else {
    
    /* client host contains host and domain */
    *pos++ = '\0';
    *host = strdup(hostname);
    *domain = strdup(pos);

  }

  return 0;

}



/**
 * Returns the host/domain name of the connected peer
 */
int _Csec_get_peer_host_domain(int s,
			       char **host,
			       char **domain) {
  

  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  int rc;
  struct hostent *hp;
  char *clienthost;
  char *pos;
  char hostname[CA_MAXHOSTNAMELEN+1];
  char domainname[CA_MAXHOSTNAMELEN+1];
  char *func = "_Csec_get_peer_host_domain";



  /* Getting the peer IP address */
  rc = getpeername(s, (struct sockaddr *)&from, &fromlen);
  if (rc < 0) {
    _Csec_errmsg(func, "Could not get peer name: %s\n", strerror(errno));
    return -1;
  }
    
  /* Looking up name */
  hp = Cgethostbyaddr ((char *)(&from.sin_addr),
		       sizeof(struct in_addr), from.sin_family);
  if (hp == NULL)
    clienthost = inet_ntoa (from.sin_addr);
  else
    clienthost = (char *)(hp->h_name) ;

  strncpy(hostname, clienthost, CA_MAXHOSTNAMELEN); 
  hostname[CA_MAXHOSTNAMELEN] = '\0';
    
  pos = strchr(clienthost, '.');
  if (pos==NULL) {
    /*  We don't have the domain */
    if (Cdomainname(domainname, sizeof(domainname))<0) {
      _Csec_errmsg(func, "Could not get domain name: <%s>", sstrerror(serrno));
      serrno = ESEC_SYSTEM;
      return -1;
    }

  } else {
        
    /* client host contains host and domain */
    if ((pos-clienthost) + 1 < sizeof(hostname)){
      memcpy(hostname, clienthost, (pos-clienthost)); 
      hostname[pos -clienthost] = '\0';
    } else {
      _Csec_errmsg(func, "Host buffer too short");
      serrno = ESEC_SYSTEM;
      return -1;            
    }

    if (strlen(pos)+1 > sizeof(domainname)) {
      _Csec_errmsg(func, "Domain buffer too short");
      serrno = ESEC_SYSTEM;
      return -1;
    } else {
      strncpy(domainname, pos, sizeof(domainname));
    }
  }

  *host = strdup(hostname);
  *domain = strdup(domainname);
    
  return rc;

}


/* *********************************************** */
/* uid/gid lookup                                  */
/*                                                 */
/* *********************************************** */


/**
 * Maps a username to the uid/gid couple corresponding
 */
int _Csec_name2id(char *name, uid_t *uid, uid_t *gid) {
  char *func = "Csec_name2id";
  struct passwd *pw;
  uid_t luid;
  gid_t lgid;
    
  pw = Cgetpwnam(name);
  if (pw == NULL) {
    _Csec_trace(func, "Could not find uid/gid for <%s>\n", name);
    _Csec_errmsg(func,"Could not find uid/gid for <%s>", name);
    serrno = ESEC_NO_USER;
    return -1;
  }
     
  luid = pw->pw_uid;
  lgid = pw->pw_gid;
  _Csec_trace(func, "%s mapped to %d/%d\n", name, luid, lgid);

  if (uid != NULL && gid != NULL) {
    *uid=luid;
    *gid=lgid;
  } else {
    serrno=EINVAL;
    return -1;
  }
     
  return 0;
}
