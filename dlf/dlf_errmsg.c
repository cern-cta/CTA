/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlf_errmsg.c,v $ $Revision: 1.2 $ $Date: 2003/09/08 13:30:13 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include "dlf.h"
#include "dlf_api.h"

/* dlf_seterrbuf - set receiving buffer for error messages */

dlf_seterrbuf(char *buffer, int buflen)
{
  struct dlf_api_thread_info *thip;

  if (dlf_apiinit (&thip))
    return (-1);
  thip->errbufp = buffer;
  thip->errbuflen = buflen;
  return (0);
}

/* dlf_errmsg - send error message to user defined client buffer or to stderr */

dlf_errmsg(const char *args, ...)
{
  va_list ap;
  char *msg;
  char prtbuf[PRTBUFSZ];
  int save_errno;
  struct dlf_api_thread_info *thip;

  save_errno = errno;
  if (dlf_apiinit (&thip))
    return (-1);
  va_start (ap, args);
  msg = va_arg (ap, char *);
  if (args)
    sprintf (prtbuf, "%s: ", args);
  else
    *prtbuf = '\0';
  vsprintf (prtbuf + strlen(prtbuf), msg, ap);
  va_end(ap);
  if (thip->errbufp) {
    if (strlen (prtbuf) < thip->errbuflen) {
      strcpy (thip->errbufp, prtbuf);
    } else {
      strncpy (thip->errbufp, prtbuf, thip->errbuflen - 2);
      thip->errbufp[thip->errbuflen-2] = '\n';
      thip->errbufp[thip->errbuflen-1] = '\0';
    }
  } else {
    fprintf (stderr, "%s", prtbuf);
  }
  errno = save_errno;
  return (0);
}
