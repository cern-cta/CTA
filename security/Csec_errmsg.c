/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_errmsg.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <serrno.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>

#include "Csec.h"

/* Csec_errmsg - send error message to error buffer in per-thread structure */
int DLL_DECL
_Csec_errmsg(char *func, char *msg, ...) {
  
  va_list args;
  int save_errno;
  struct Csec_api_thread_info *thip;
  int funlen=0;
  
  save_errno = errno;

  if (_Csec_apiinit (&thip))
    return -1;

  va_start (args, msg);

  /* Either the error buffer is set, in which case we write to it,
     or we write directly to stderr */
  if (thip->errbuf != NULL) {
    if (func) {
      snprintf (thip->errbuf, thip->errbufsize, "%s: ", func);
    }
    funlen = strlen(thip->errbuf);
    vsnprintf (thip->errbuf + funlen, thip->errbufsize - funlen -1, msg, args);
    thip->errbuf[thip->errbufsize] = '\0';
    _Csec_trace("ERROR", "%s\n", thip->errbuf);
  } else {
    if (func) {
      fprintf(stderr, "%s ", func);
    }
    vfprintf (stderr, msg, args);
    fprintf(stderr, "\n");
  }
  errno = save_errno;
  return 0;
}

/* Csec_getErrorMessage - send error message to user defined client buffer or to stderr */
char *Csec_getErrorMessage() {
  return Csec_geterrmsg();
}

/* Csec_getErrorMessage - send error message to user defined client buffer or to stderr */
char *Csec_geterrmsg() {

  struct Csec_api_thread_info *thip;
  if (_Csec_apiinit (&thip))
    return NULL;

  return thip->errbuf;

}


int Csec_seterrbuf(char *buffer, int buflen) {
  struct Csec_api_thread_info *thip;
  if (_Csec_apiinit (&thip))
    return (-1);

  thip->errbuf = buffer;
  thip->errbufsize = buflen;
  return 0;
}


/* *********************************************** */
/* Csec error and apiinit                          */
/*                                                 */
/* *********************************************** */
                                                                                                                             
void _Csec_sync_error(Csec_plugin_t *plugin) {
  char *msg;
  int errnb;
  struct Csec_api_thread_info *thip;
  
  if (_Csec_apiinit (&thip))
    return;
  
  if (plugin == NULL) return;
  plugin->Csec_plugin_getErrorMessage(&errnb, &msg);
  
  if (errnb != serrno) serrno =errnb;
  if (msg != NULL && thip->errbuf != NULL && thip->errbuf != msg ) {
    fprintf(stderr, "ooooooooooooooooHaving to copy the error !\n");
/*     strncpy(thip->errbuf, msg, thip->errbufsize); */
  } else {
/*     fprintf(stderr, "oooooooooooooooo No copy needed !!!!!!!\n"); */
  }
}
