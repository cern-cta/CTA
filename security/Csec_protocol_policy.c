/*
 * $id$
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

/*
 * Csec_protocol_policy.c 
 */

#ifndef lint
static char sccsid[] = "@(#)Csec_protocol_policy.c CERN IT/ADC/CA Benjamin Couturier";
#endif

#include <string.h>
#include <serrno.h>
#include <errno.h>
#include <Csec.h>
#include <Csec_protocol_policy.h>


/**
 * Returns the the list of protocols usable, as a client.
 */
int Csec_client_get_protocols(Csec_protocol **protocols, int *nbprotocols) {

  char *p, *q, *ctx;
  char *buf;
  int entry = 0;
  Csec_protocol *prots;

  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv ("CSEC_MECH")) || (p = (char *)getconfent ("CSEC", "MECH", 0)))) {
    p = "CSEC_DEFAULT_MECHS";
  }

  buf = (char *)malloc(strlen(p));
  if (NULL == buf) {
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  while ((q = strtok_r(buf, " \t", &ctx)) != NULL) {
    if (strlen(q) > 0) entry++;
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == buf) {
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  while ((q = strtok_r(buf, " \t", &ctx)) != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, PROTID_SIZE);
    }
  }

  free(buf);
  *protocols = prots;
  return 0;
}


int Csec_server_get_client_authorized_protocols(long client_address,
						Csec_protocol **protocols,
						int *nbprotocols) {

  char *p, *q, *ctx;
  char *buf;
  int entry = 0;
  Csec_protocol *prots;

  /* Getting the protocol list from environment variable, configuration file
     or default value */
  if (!((p = (char *)getenv ("CSEC_MECH")) || (p = (char *)getconfent ("CSEC", "MECH", 0)))) {
    p = CSEC_MECH;
  }

  buf = (char *)malloc(strlen(p));
  if (NULL == buf) {
    return -1;
  }

  /* First counting the entries */
  strcpy(buf, p);
  while ((q = strtok_r(buf, " \t", &ctx)) != NULL) {
    if (strlen(q) > 0) entry++;
  }

  /* Allocating the list */
  prots = (Csec_protocol *)malloc(entry * sizeof(Csec_protocol));
  if (NULL == buf) {
    free(buf);
    return -1;
  }
  
  /* Now creating the list of protocols */
  *nbprotocols = entry;
  entry = 0;
  strcpy(buf, p);
  while ((q = strtok_r(buf, " \t", &ctx)) != NULL) {
    if (strlen(q) > 0) {
      strncpy(prots[entry].id, q, PROTID_SIZE);
    }
  }

  free(buf);
  *protocols = prots;
  return 0;


}



