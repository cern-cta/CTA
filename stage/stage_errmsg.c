/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_errmsg.c,v $ $Revision: 1.1 $ $Date: 2000/01/26 08:36:25 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <varargs.h>
#include <sys/types.h>
#include <stdlib.h>
#include "serrno.h"
#include "stage.h"
#include "Cthread_api.h"

int stage_gettsd _PROTO((char **, int *, char **, int *));
int stage_settsd _PROTO((char **, int *, char **, int *));

static int errbufp_key = 0;
static int errbuflen_key = 0;
static int outbufp_key = 0;
static int outbuflen_key = 0;

/* stage_gettsd - Get Thread-Specific variables */
int stage_gettsd(errbufp_p,errbuflen_p,outbufp_p,outbuflen_p)
     char **errbufp_p;
     int *errbuflen_p;
     char **outbufp_p;
     int *outbuflen_p;
{
  char *errbufp;
  int errbuflen;
  char *outbufp;
  int outbuflen;

  /* This function is to be called with either the two first arguments */
  /* not NULL, either the two last arguments not NULL, or all not NULL */
  if (((errbufp_p == NULL && errbuflen_p != NULL) ||
       (errbufp_p != NULL && errbuflen_p == NULL)) ||
      ((outbufp_p == NULL && outbuflen_p != NULL) ||
       (outbufp_p != NULL && outbuflen_p == NULL))) {
    serrno = SEINTERNAL;
    return(-1);
  }

  if (errbufp_p != NULL && errbuflen_p != NULL) {
    /* User want to get TSD for error messages */
    if (Cthread_getspecific(&errbufp_key,(void **) &errbufp) != 0 ||
        Cthread_getspecific(&errbuflen_key,(void **) &errbuflen) != 0) {
      return(-1);
    }
    *errbufp_p = errbufp;
    *errbuflen_p = errbuflen;
  }

  if (outbufp_p != NULL && outbuflen_p != NULL) {
    /* User want to get TSD for output messages */
    if (Cthread_getspecific(&outbufp_key,(void **) &outbufp) != 0 ||
        Cthread_getspecific(&outbuflen_key,(void **) &outbuflen) != 0) {
      return(-1);
    }
    *outbufp_p = outbufp;
    *outbuflen_p = outbuflen;
  }

  return(0);
}

/* stage_settsd - Set Thread-Specific variables */
int stage_settsd(errbufp_p,errbuflen_p,outbufp_p,outbuflen_p)
     char **errbufp_p;
     int *errbuflen_p;
     char **outbufp_p;
     int *outbuflen_p;
{
  /* This function is to be called with either the two first arguments */
  /* not NULL, either the two last arguments not NULL, or all not NULL */
  if (((errbufp_p == NULL && errbuflen_p != NULL) ||
       (errbufp_p != NULL && errbuflen_p == NULL)) ||
      ((outbufp_p == NULL && outbuflen_p != NULL) ||
       (outbufp_p != NULL && outbuflen_p == NULL))) {
    serrno = SEINTERNAL;
    return(-1);
  }

  if (errbufp_p != NULL && errbuflen_p != NULL) {
    /* User want to set TSD for error messages */
    if (Cthread_setspecific(&errbufp_key,(void *) *errbufp_p) != 0 ||
        Cthread_getspecific(&errbuflen_key,(void *) *errbuflen_p) != 0) {
      return(-1);
    }
  }

  if (outbufp_p != NULL && outbuflen_p != NULL) {
    /* User want to set TSD for output messages */
    if (Cthread_getspecific(&outbufp_key,(void *) *outbufp_p) != 0 ||
        Cthread_getspecific(&outbuflen_key,(void *) *outbuflen_p) != 0) {
      return(-1);
    }
  }

  return(0);
}

/*	stage_seterrbuf - set receiving buffer for error messages */

int DLL_DECL stage_seterrbuf(buffer, buflen)
     char *buffer;
     int buflen;
{
  return(stage_settsd(&buffer,&buflen,NULL,NULL));
}

/*	stage_setoutbuf - set receiving buffer for output messages */

int DLL_DECL stage_setoutbuf(buffer, buflen)
     char *buffer;
     int buflen;
{
  return(stage_settsd(NULL,NULL,&buffer,&buflen));
}

/*	stage_geterrbuf - get receiving buffer for error messages */

int DLL_DECL stage_geterrbuf(buffer, buflen)
     char **buffer;
     int *buflen;
{
  return(stage_gettsd(buffer,buflen,NULL,NULL));
}

/*	stage_getoutbuf - get receiving buffer for output messages */

int DLL_DECL stage_getoutbuf(buffer, buflen)
     char **buffer;
     int *buflen;
{
  return(stage_gettsd(NULL,NULL,buffer,buflen));
}

/* stage_errmsg - send error message to user defined client buffer or to stderr */

int DLL_DECL stage_errmsg(va_alist)
     va_dcl
{
  va_list args;
  char *func;
  char *msg;
  char prtbuf[PRTBUFSZ];
  int save_errno;
  char *errbufp;
  int errbuflen;

  if (stage_geterrbuf(&errbufp,&errbuflen) != 0) {
    return(-1);
  }

  save_errno = errno;
  va_start (args);
  func = va_arg (args, char *);
  msg = va_arg (args, char *);
  if (func)
    sprintf (prtbuf, "%s: ", func);
  else
    *prtbuf = '\0';
  vsprintf (prtbuf + strlen(prtbuf), msg, args);

  /* We check length of the error buffer */
  if (errbufp == NULL) {
    /* Error TSD not yet created */
    if ((errbufp = (char *) calloc(1,strlen(prtbuf) + 1)) == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    errbuflen = (int) (strlen(prtbuf) + 1);
    if (stage_seterrbuf(errbufp,errbuflen) != 0) {
      free(errbufp);
      return(-1);
    }
  } else {
    /* We check current error TSD length */
    if (errbuflen <= 0) {
      /* Impossible */
      serrno = SEINTERNAL;
      return(-1);
    }
    if (errbuflen < (int) (strlen(prtbuf) + 1)) {
      char *dummy;
      if ((dummy = (char *) calloc(1,strlen(prtbuf) + 1)) == NULL) {
        serrno = SEINTERNAL;
        return(-1);
      }
      errbuflen = (int) (strlen(prtbuf) + 1);
      if (stage_seterrbuf(dummy,errbuflen) != 0) {
        free(dummy);
        return(-1);
      }
      errbufp = dummy;
    }
  }
  if (*errbufp) {
    if (strlen (prtbuf) < errbuflen) {
      strcpy (errbufp, prtbuf);
    } else {
      strncpy (errbufp, prtbuf, errbuflen - 2);
      errbufp[errbuflen-2] = '\n';
      errbufp[errbuflen-1] = '\0';
    }
  } else {
    fprintf (stderr, "%s", prtbuf);
  }
  errno = save_errno;
  return (0);
}

/* stage_outmsg - send output message to user defined client buffer or to stdout */

int DLL_DECL stage_outmsg(va_alist)
     va_dcl
{
  va_list args;
  char *func;
  char *msg;
  char prtbuf[PRTBUFSZ];
  int save_errno;
  char *outbufp;
  int outbuflen;

  if (stage_getoutbuf(&outbufp,&outbuflen) != 0) {
    return(-1);
  }

  save_errno = errno;
  va_start (args);
  func = va_arg (args, char *);
  msg = va_arg (args, char *);
  if (func)
    sprintf (prtbuf, "%s: ", func);
  else
    *prtbuf = '\0';
  vsprintf (prtbuf + strlen(prtbuf), msg, args);

  /* We check length of the output buffer */
  if (outbufp == NULL) {
    /* Output TSD not yet created */
    if ((outbufp = (char *) calloc(1,strlen(prtbuf) + 1)) == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    outbuflen = (int) (strlen(prtbuf) + 1);
    if (stage_setoutbuf(outbufp,outbuflen) != 0) {
      free(outbufp);
      return(-1);
    }
  } else {
    /* We check current Output TSD length */
    if (outbuflen <= 0) {
      /* Impossible */
      serrno = SEINTERNAL;
      return(-1);
    }
    if (outbuflen < (int) (strlen(prtbuf) + 1)) {
      char *dummy;
      if ((dummy = (char *) calloc(1,strlen(prtbuf) + 1)) == NULL) {
        serrno = SEINTERNAL;
        return(-1);
      }
      outbuflen = (int) (strlen(prtbuf) + 1);
      if (stage_setoutbuf(dummy,outbuflen) != 0) {
        free(dummy);
        return(-1);
      }
      outbufp = dummy;
    }
  }
  if (*outbufp) {
    if (strlen (prtbuf) < outbuflen) {
      strcpy (outbufp, prtbuf);
    } else {
      strncpy (outbufp, prtbuf, outbuflen - 2);
      outbufp[outbuflen-2] = '\n';
      outbufp[outbuflen-1] = '\0';
    }
  } else {
    fprintf (stdout, "%s", prtbuf);
  }
  errno = save_errno;
  return (0);
}
