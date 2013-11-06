/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include "Cglobals.h"
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
#include "getconfent.h"

static int Cns_api_key = -1;

int
Cns_apiinit(struct Cns_api_thread_info **thip)
{
  char *p = NULL;
  Cglobals_get (&Cns_api_key,
                (void **) thip, sizeof(struct Cns_api_thread_info));
  if (*thip == NULL) {
    serrno = ENOMEM;
    return (-1);
  }
  if(! (*thip)->initialized) {
    umask ((*thip)->mask = umask (0));
    (*thip)->initialized = 1;
    (*thip)->fd = -1;
    (*thip)->defserver[0] = '\0';
    if ((p = getenv (CNS_HOST_ENV)) || (p = getconfent (CNS_SCE, "HOST", 0))) {
      strncpy((*thip)->defserver, p, sizeof((*thip)->defserver));
    }
  }
  return (0);
}

int *
C__Cns_errno()
{
  struct Cns_api_thread_info *thip;
  Cglobals_get (&Cns_api_key,
                (void **) &thip, sizeof(struct Cns_api_thread_info));
  return (&thip->ns_errno);
}
