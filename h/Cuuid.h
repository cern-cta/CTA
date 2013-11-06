/*
 * $Id: Cuuid.h,v 1.10 2009/03/26 11:25:41 itglp Exp $
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 */

#ifndef _CUUID_H
#define _CUUID_H

#include <osdep.h>
#include <sys/types.h>

#define CUUID_STRING_LEN 36
#define CUUID_STRING_FMT "%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x"

typedef struct _Cuuid_t {
	U_LONG  time_low;
	U_SHORT time_mid;
	U_SHORT time_hi_and_version;
	U_BYTE  clock_seq_hi_and_reserved;
	U_BYTE  clock_seq_low;
	U_BYTE  node[6];
} Cuuid_t;

/* A usefull cuuid : the null one */
extern Cuuid_t nullCuuid;

EXTERN_C void Cuuid_create (Cuuid_t *);
EXTERN_C int Cuuid_compare (Cuuid_t *, Cuuid_t *);

EXTERN_C int Cuuid2string (char *, size_t, Cuuid_t *);
EXTERN_C int string2Cuuid (Cuuid_t *, const char *);

EXTERN_C void _marshall_UUID (char**, Cuuid_t *);
EXTERN_C void _unmarshall_UUID (char**, Cuuid_t *);

#define unmarshall_UUID(ptr, uuid) _unmarshall_UUID(&(ptr), &(uuid))
#define marshall_UUID(ptr, uuid) _marshall_UUID(&(ptr), &(uuid))

#endif /* _CUUID_H */
