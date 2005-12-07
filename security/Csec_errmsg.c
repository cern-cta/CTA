/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csec_errmsg.c,v $ $Revision: 1.4 $ $Date: 2005/12/07 10:19:21 $ CERN IT/ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>

#include "Csec.h"
#include "serrno.h"

/* Csec_errmsg - send error message to error buffer in per-thread structure */
/* should preserve errno, serrno */
int DLL_DECL
Csec_errmsg(char *func, char *msg, ...) {
  
  va_list args;
  int save_errno, save_serrno;
  struct Csec_api_thread_info *thip;
  int funlen=0;
  
  save_errno = errno;
  save_serrno = serrno;

  if (Csec_apiinit (&thip))
    return (-1);
  va_start (args, msg);
  
  if (func) {
    snprintf (thip->errbuf, ERRBUFSIZE, "%s: ", func);
  }
   
  funlen = strlen(thip->errbuf);
  
  vsnprintf (thip->errbuf + funlen, ERRBUFSIZE - funlen -1, msg, args);
  thip->errbuf[ERRBUFSIZE] = '\0';
    
  Csec_trace("ERROR", "%s\n", thip->errbuf);
  
  errno = save_errno;
  serrno = save_serrno;
  return (0);
}



/* Csec_errErrorMessage - send error message to user defined client buffer or to stderr */
char *Csec_getErrorMessage() {

  struct Csec_api_thread_info *thip;
  if (Csec_apiinit (&thip))
    return NULL;

  return thip->errbuf;

}
