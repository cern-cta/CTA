/*
 * $Id: Cuuid.h,v 1.6 2004/11/25 09:41:07 bcouturi Exp $
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * Based on: http://www.webdav.org/specs/draft-leach-uuids-guids-01.txt
 *
 * $RCSfile: Cuuid.h,v $ $Revision: 1.6 $ $Date: 2004/11/25 09:41:07 $ CERN IT-PDP/DM Jean-Damien Durand
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

EXTERN_C void DLL_DECL Cuuid_create _PROTO((Cuuid_t *));
EXTERN_C void DLL_DECL Cuuid_create_from_name _PROTO ((Cuuid_t *,
                                                       Cuuid_t, char *));
EXTERN_C int  DLL_DECL Cuuid_compare _PROTO((Cuuid_t *, Cuuid_t *));
EXTERN_C void DLL_DECL _marshall_UUID _PROTO((char**, Cuuid_t *));
EXTERN_C void DLL_DECL _unmarshall_UUID _PROTO((char**, Cuuid_t *));

EXTERN_C int DLL_DECL Cuuid2string _PROTO((char *, size_t, Cuuid_t *));
EXTERN_C int DLL_DECL string2Cuuid _PROTO((Cuuid_t *, char *));

#define  unmarshall_UUID(ptr,uuid)  _unmarshall_UUID(&(ptr), &(uuid))
#define  marshall_UUID(ptr,uuid)  _marshall_UUID(&(ptr), &(uuid))

#endif /* _CUUID_H */
