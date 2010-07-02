/*
 * $Id: stager_uuid.c,v 1.2 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include "Cglobals.h"   /* Get Cglobals_get prototype */
#include "stager_uuid.h"

static int stager_request_uuid_key = -1;    /* Our static key, integer, init value -1 */
static int stager_subrequest_uuid_key = -1; /* Our static key, integer, init value -1 */

static Cuuid_t stager_request_uuid_static;      /* If Cglobals_get error in order not to crash */
static Cuuid_t stager_subrequest_uuid_static;   /* If Cglobals_get error in order not to crash */

Cuuid_t *C__stager_request_uuid() {
  Cuuid_t *var;
  /* Call Cglobals_get */
  Cglobals_get(&stager_request_uuid_key, (void **) &var, sizeof(Cuuid_t));
  /* If error, var will be NULL */
  if (var == NULL) {
    return(&stager_request_uuid_static);
  }
  return(var);
}

Cuuid_t *C__stager_subrequest_uuid() {
  Cuuid_t *var;
  /* Call Cglobals_get */
  Cglobals_get(&stager_subrequest_uuid_key, (void **) &var, sizeof(Cuuid_t));
  /* If error, var will be NULL */
  if (var == NULL) {
    return(&stager_subrequest_uuid_static);
  }
  return(var);
}
