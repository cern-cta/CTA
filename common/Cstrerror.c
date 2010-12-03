/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA 
 * All rights reserved
 */

/*
 * $Id: Cstrerror.c,v 1.4 2007/12/06 14:24:47 sponcec3 Exp $
 */

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "osdep.h"
#include "Cstrerror.h"
#include "Cglobals.h"

#define INTERNAL_BUFLEN 1023

char *static_message = "Cstrerror internal error";

static int Cstrerror_bufp_key = 0;
static int Cstrerror_buflen_key = 0;
static int Cstrerror_getbuf (char **, int *);
static int Cstrerror_setbuf (char *, int);

static int Cstrerror_getbuf(char **buffer,
                            int *buflen)
{
  char **bufp;
  int *buflenp;

  Cglobals_get (&Cstrerror_bufp_key, (void **)&bufp, sizeof(char *));
  Cglobals_get (&Cstrerror_buflen_key, (void **)&buflenp, sizeof(int));
  if (bufp == NULL || buflenp == NULL) {
    return(-1);
  }
  *buffer = *bufp;
  *buflen = *buflenp;
  return(0);
}

static int Cstrerror_setbuf(char *buffer,
                            int buflen)
{
  char **bufp;
  int *buflenp;

  Cglobals_get (&Cstrerror_bufp_key, (void **)&bufp, sizeof(char *));
  Cglobals_get (&Cstrerror_buflen_key, (void **)&buflenp, sizeof(int));
  if (bufp == NULL || buflenp == NULL) {
    return(-1);
  }
  *bufp = buffer;
  *buflenp = buflen;
  return(0);
}

char *Cstrerror(int code)
{
  char *bufp;
  int buflen = INTERNAL_BUFLEN+1;

#if !linux
  char *message;
#endif

  /* Get thread-specific buffers */
  if (Cstrerror_getbuf(&bufp,&buflen) != 0) {
    errno = code;
    return(static_message);
  }
  if (bufp == NULL) {
    /* Assign a thread-specific buffer - a priori 1023+1 characters will always be enough */
    if ((bufp = (char *) malloc(buflen)) == NULL) {
      errno = code;
      return(static_message);
    }
    if (Cstrerror_setbuf(bufp,buflen) != 0) {
      errno = code;
      return(static_message);
    }
  }

#if linux
  /* Retreive error message in bufp */
  if (strerror_r(code,bufp,(size_t) (buflen-1)) != 0) {
    errno = code;
    return(static_message);
  }
  /* Who knows */
  bufp[buflen] = '\0';
#else
  /* TODO */
  if ((message = strerror(code)) == NULL) {
    /* Should never happen */
    errno = code;
    return(static_message);
  }
  /* Keep a copy anyway */
  strncpy(bufp, message, buflen-1);
  bufp[buflen] = '\0';
#endif

  errno = code;
  return(bufp);
}
