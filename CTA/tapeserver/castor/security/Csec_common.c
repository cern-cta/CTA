/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_common.c - Common function in the Csecurity API 
 */

#include <osdep.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "Cglobals.h"
#include "serrno.h"
#include "Cpwd.h"
#include "marshall.h"
#include <netinet/in.h>
#include <net.h>
#include <sys/socket.h>
#include <unistd.h>

#include "Csec.h"

/**
 * Checks the environment to setup the trace mode,
 * if CSEC_TRACE is set
 * If CSEC_TRACEFILE is set, the output is written to that file,
 * otherwise, it is sent to stderr.
 */
int Csec_setup_trace() {
  char *envar;

  struct Csec_api_thread_info *thip;

  if (Csec_apiinit (&thip))
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


int Csec_trace(char *func, char *msg, ...) {
  va_list args;
  char prtbuf[SECPRTBUFSZ+1];
  struct Csec_api_thread_info *thip;
  int funlen;

  if (Csec_apiinit (&thip)) {
    return  -1;
  }

  if (!thip->trace_mode) {
    return 0;
  }

  va_start (args, msg);
  if (func) {
    snprintf (prtbuf, SECPRTBUFSZ+1, "%s: ", func);
    prtbuf[SECPRTBUFSZ] = 0;
  } else {
    *prtbuf = '\0';
  }
  funlen = strlen(prtbuf);

  vsnprintf (prtbuf + funlen ,  SECPRTBUFSZ - funlen -1, msg, args);
  prtbuf[SECPRTBUFSZ]='\0';

  if (thip->trace_file[0] != '\0') {
    int fd;
    fd = open(thip->trace_file, O_CREAT|O_WRONLY|O_APPEND, 0666);
    if (fd <0) {
      va_end (args);
      return -1;
    }
    write(fd, prtbuf, strlen(prtbuf));
    close(fd);

  } else {
    fprintf (stderr, "%s", prtbuf);
  }

  va_end (args);
  return 0;
}


/**
 * Maps a username to the uid/gid couple corresponding
 */
int Csec_name2id(char *name, uid_t *uid, uid_t *gid) {
  char *func = "Csec_name2id";
  struct passwd *pw;
  uid_t luid;
  gid_t lgid;
    
  pw = Cgetpwnam(name);
  if (pw == NULL) {
    Csec_trace(func, "Could not find uid/gid for <%s>\n", name);
    Csec_errmsg(func,"Could not find uid/gid for <%s>", name);
    serrno = ESEC_NO_USER;
    return -1;
  }
     
  luid = pw->pw_uid;
  lgid = pw->pw_gid;
  Csec_trace(func, "%s mapped to %d/%d\n", name, luid, lgid);

  if (uid != NULL && gid != NULL) {
    *uid=luid;
    *gid=lgid;
  } else {
    errno=EINVAL;
    return -1;
  }
     
  return 0;
}

/**
 * Sends a csec_buffer_t over a socket
 */
int _Csec_send_token(int s,
                     csec_buffer_t tok,
                     int timeout,
                     int token_type)
{
  int datalen, ret;
  char *func = "_Csec_send_token";
  char *p;
  csec_buffer_desc buf;
  U_LONG magic = CSEC_TOKEN_MAGIC_1;
  U_LONG type = token_type;

  datalen = tok->length;

  Csec_trace(func, "Sending packet Magic: %x Type: %x, Len: %d\n",
	     magic, type, datalen);

  buf.length = 3*LONGSIZE + tok->length;
  buf.value = malloc(buf.length);
  p = buf.value;
  if (p == NULL) {
    serrno = ENOMEM;
    Csec_errmsg(func, "Could not allocate space for a buffer");
    return -1;
  }

  marshall_LONG(p, magic);
  marshall_LONG(p, type);
  marshall_LONG(p, datalen);

  memcpy(p, tok->value, tok->length);

  _Csec_print_token(tok);
    
  ret = netwrite_timeout(s, buf.value, buf.length, timeout);
  free(buf.value);
  if (ret < 0) {
    /* We keep the serrno from netwrite */
    Csec_errmsg(func, "Error sending token length and data");
    return -1;
  } else if (ret != (int)buf.length) {
    serrno = ESEC_SYSTEM;
    Csec_errmsg(func, "Bad token length");
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
                     int *rtype)
{
  int ret;
  char *func = "_Csec_recv_token";
  char headbuf[3 * LONGSIZE];
  char *p;
  unsigned int len, headlen;
  int data_already_read=0;
  int header_already_read = 0;
  char *prefetched_data = NULL;
  U_LONG magic;
  U_LONG type;

  headlen = 3 * LONGSIZE;

  /* In this case some data has already been prefetched into tok,
     and we consider this was the first bytes sent by the client */

  Csec_trace(func, "Entering. tok->length: %d\n", tok->length);

  if (tok->length >= headlen) {
    /* More (or equal) the header was already read */
    data_already_read = tok->length - headlen;
    Csec_trace(func,
	       "Header already read. Nb bytes Data already read: %d\n",
	       data_already_read);
    memcpy(headbuf, tok->value, headlen);
    if (data_already_read > 0) {
      prefetched_data = (char *)malloc(data_already_read);
      if (prefetched_data == NULL) {
	serrno = ESEC_SYSTEM;
	Csec_errmsg(func, "Could not allocate space for token");
	return -1;
      }
      memcpy(prefetched_data, (char *)tok->value + headlen, data_already_read);
    }
    free(tok->value);
    tok->value = NULL;
    tok->length = 0;

  } else if (tok->length > 0 && tok->length < headlen) {
    /* Less than the header was read */
    header_already_read = tok->length;
    Csec_trace(func, "Bytes of header already read: %d\n",
	       header_already_read);
    memcpy(headbuf, tok->value, header_already_read);
    free(tok->value);
    tok->value = NULL;
    tok->length = 0;

    /* Nothing was read */
    ret = netread_timeout(s, headbuf + header_already_read,
			  headlen - header_already_read, timeout);

    if (ret < 0) {
      /* We keep the serrno from netread */
      Csec_errmsg(func, "Error reading token length");
      return -1;
    } else if (ret == 0) {
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Connection dropped by remote end");
      return -1;
    } else if (ret !=  (int)headlen - header_already_read) {
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Bad header length: %d",
		  header_already_read + ret);
      return -1;
    }


  } else {
    /* Nothing was read */
    Csec_trace(func, "Nothing was prefetched\n");
    ret = netread_timeout(s, headbuf, headlen, timeout);
    if (ret < 0) {
      /* We keep the serrno from netread */
      Csec_errmsg(func, "Error reading token length");
      return -1;
    } else if (ret == 0) {
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Connection dropped by remote end");
      return -1;
    } else if (ret != (int)headlen) {
      serrno = ESEC_SYSTEM;
      Csec_errmsg(func, "Bad token length: %d", ret);
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

  Csec_trace(func, "Receiving packet Magic: %x Type: %x, Len: %d\n",
	     magic, type, len);

  if (magic != CSEC_TOKEN_MAGIC_1) {
    serrno = ESEC_BAD_MAGIC;
    Csec_errmsg(func, "Received magic: %x expecting %x",
		magic, CSEC_TOKEN_MAGIC_1);
    if (prefetched_data!=NULL) free(prefetched_data);
    return -1;
  }

  if (len==0) {
    serrno = ESEC_SYSTEM;
    Csec_errmsg(func,"Token length was zero");
    if (prefetched_data!=NULL) free(prefetched_data);
    return -1;
  }

  tok->value = (char *) malloc(len + 1);
  if (tok->value == NULL) {
    serrno = ESEC_SYSTEM;
    Csec_errmsg(func, "Could not allocate space for token");
    if (prefetched_data!=NULL) free(prefetched_data);
    return -1;
  }
  tok->length = len;

  /* add a null at end of data, in case someone expects strings to be null terminated */
  ((char *)tok->value)[tok->length] = '\0';

  if (data_already_read>0) {
    memcpy(tok->value, prefetched_data, data_already_read);
    free(prefetched_data);
    prefetched_data=NULL;
  }

  ret = netread_timeout(s, (char *) ( (char*)tok->value+data_already_read),
			(int)(tok->length - data_already_read), timeout);

  if (ret < 0) {
    /* We keep the serrno from netread */
    Csec_errmsg(func, "Could not read token data");
    free(tok->value);
    return -1;
  } else if (ret != (int)(tok->length - data_already_read)) {
    serrno = ESEC_SYSTEM;
    Csec_errmsg(func, "Bad token data length. Received %d rather than %d",
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
void _Csec_print_token(csec_buffer_t tok)
{
  unsigned int i;
  int l=0;
  char buf[50];
  unsigned char *p = tok->value;

  for (i=0; i < tok->length; i++, p++) {
    snprintf(buf+l,50-l,"%02x ",*p); 
    l+=3;
    if(l>=48) {
      Csec_trace(NULL, "%s\n",buf);
      l=0;
    }    	
  }
  if(l)  Csec_trace(NULL, "%s\n",buf);
  Csec_trace(NULL,"\n");
}


int check_ctx(Csec_context_t *ctx, char *func) {
  if (!(ctx->flags& CSEC_CTX_INITIALIZED)) {
    Csec_errmsg(func, "Context not initialized");
    serrno = ESEC_CTX_NOT_INITIALIZED;
    return -1;
  }
  return 0;
}

/**
 * Checks whether we have a client context or a server context
 */
int Csec_context_is_client(Csec_context_t *ctx) {
  if (ctx->magic & CSEC_CONTEXT_MAGIC_CLIENT_MASK) {
    return 1;
  } else {
    return 0;
  }
}
