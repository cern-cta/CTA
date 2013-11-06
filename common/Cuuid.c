/*
 * $Id: Cuuid.c,v 1.15 2009/06/03 13:25:22 sponcec3 Exp $
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <errno.h>
#include <uuid/uuid.h>
#include <netinet/in.h>

#include "serrno.h"
#include "Cuuid.h"
#include "marshall.h"
#include "Csnprintf.h"

/**
 * Declaration of a usefull cuuid : the null one.
 * No need to do a memset, it will be set to 0 by default
 */
Cuuid_t nullCuuid;

void Cuuid_create(Cuuid_t *uuid)
{
  uuid_t internal_uuid;
  char uuidstr[CUUID_STRING_LEN + 1];

  /* Generate a UUID using libuuid */
  uuid_generate(internal_uuid);

  /* Convert the UUID to a string */
  uuidstr[CUUID_STRING_LEN] = 0;
  uuid_unparse(internal_uuid, uuidstr);

  /* Convert the string to a CASTOR UUID */
  string2Cuuid(uuid, uuidstr);
}

int Cuuid_compare(Cuuid_t *u1,
                  Cuuid_t *u2)
{
  int i;

#define CHECK(f1, f2) if (f1 != f2) return f1 < f2 ? -1 : 1;
  CHECK(u1->time_low, u2->time_low);
  CHECK(u1->time_mid, u2->time_mid);
  CHECK(u1->time_hi_and_version, u2->time_hi_and_version);
  CHECK(u1->clock_seq_hi_and_reserved, u2->clock_seq_hi_and_reserved);
  CHECK(u1->clock_seq_low, u2->clock_seq_low);
  for (i = 0; i < 6; i++) {
    if (u1->node[i] < u2->node[i])
      return -1;
    if (u1->node[i] > u2->node[i])
      return 1;
  }
  return 0;
}

int Cuuid2string(char *output,
                 size_t maxlen,
                 Cuuid_t *uuid)
{
  if ((output == NULL) || (uuid == NULL)) {
    serrno = EFAULT;
    return(-1);
  }

  Csnprintf(output,
	    maxlen,
	    CUUID_STRING_FMT,
	    uuid->time_low,
	    uuid->time_mid,
	    uuid->time_hi_and_version,
	    uuid->clock_seq_hi_and_reserved,
	    uuid->clock_seq_low,
	    uuid->node[0],
	    uuid->node[1],
	    uuid->node[2],
	    uuid->node[3],
	    uuid->node[4],
	    uuid->node[5]
	    );

  return(0);
}

int string2Cuuid(Cuuid_t *uuid,
                 const char *input)
{
  int items;
  U_LONG dummy[11];

  if ((uuid == NULL) || (input == NULL)) {
    serrno = EFAULT;
    return(-1);
  }

  items = sscanf(input,
		 CUUID_STRING_FMT,
		 &(dummy[0]),
		 &(dummy[1]),
		 &(dummy[2]),
		 &(dummy[3]),
		 &(dummy[4]),
		 &(dummy[5]),
		 &(dummy[6]),
		 &(dummy[7]),
		 &(dummy[8]),
		 &(dummy[9]),
		 &(dummy[10]));
  if (items != 11) {
    /* We did not matched all the items !? */
    serrno = EINVAL;
    return(-1);
  }

  uuid->time_low                  = (U_LONG)  dummy[0];
  uuid->time_mid                  = (U_SHORT) dummy[1];
  uuid->time_hi_and_version       = (U_SHORT) dummy[2];
  uuid->clock_seq_hi_and_reserved = (U_BYTE)  dummy[3];
  uuid->clock_seq_low             = (U_BYTE)  dummy[4];
  uuid->node[0]                   = (U_BYTE)  dummy[5];
  uuid->node[1]                   = (U_BYTE)  dummy[6];
  uuid->node[2]                   = (U_BYTE)  dummy[7];
  uuid->node[3]                   = (U_BYTE)  dummy[8];
  uuid->node[4]                   = (U_BYTE)  dummy[9];
  uuid->node[5]                   = (U_BYTE)  dummy[10];

  return(0);
}

void _marshall_UUID (char **ptr,
                     Cuuid_t *uuid)
{
  int i;
  marshall_LONG(*ptr, uuid->time_low);
  marshall_SHORT(*ptr, uuid->time_mid);
  marshall_SHORT(*ptr, uuid->time_hi_and_version);
  marshall_BYTE(*ptr, uuid->clock_seq_hi_and_reserved);
  marshall_BYTE(*ptr, uuid->clock_seq_low);
  for (i = 0; i < 6; i++) marshall_BYTE(*ptr, uuid->node[i]);
}

void _unmarshall_UUID (char **ptr,
                       Cuuid_t *uuid)
{
  int i;
  unmarshall_LONG(*ptr, uuid->time_low);
  unmarshall_SHORT(*ptr, uuid->time_mid);
  unmarshall_SHORT(*ptr, uuid->time_hi_and_version);
  unmarshall_BYTE(*ptr, uuid->clock_seq_hi_and_reserved);
  unmarshall_BYTE(*ptr, uuid->clock_seq_low);
  for (i = 0; i < 6; i++) unmarshall_BYTE(*ptr, uuid->node[i]);
}
