/*
 * $Id: Cuuid.h,v 1.1 2003/09/09 12:54:39 jdurand Exp $
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * Based on: http://www.webdav.org/specs/draft-leach-uuids-guids-01.txt
 *
 * $RCSfile: Cuuid.h,v $ $Revision: 1.1 $ $Date: 2003/09/09 12:54:39 $ CERN IT-PDP/DM Jean-Damien Durand
 */


#ifndef _CUUID_H
#define _CUUID_H

#include <osdep.h>
#include <sys/types.h>

typedef struct _Cuuid_t {
	U_LONG  time_low;
	U_SHORT time_mid;
	U_SHORT time_hi_and_version;
	U_BYTE  clock_seq_hi_and_reserved;
	U_BYTE  clock_seq_low;
	BYTE    node[6];
} Cuuid_t;

EXTERN_C void DLL_DECL Cuuid_create _PROTO((Cuuid_t *));
EXTERN_C void DLL_DECL Cuuid_create_from_name _PROTO((Cuuid_t *, Cuuid_t, char *));
EXTERN_C int  DLL_DECL Cuuid_compare _PROTO((Cuuid_t *, Cuuid_t *));

#endif /* _CUUID_H */
