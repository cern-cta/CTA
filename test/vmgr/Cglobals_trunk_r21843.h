/*
 * $Id: Cglobals.h,v 1.9 2001/06/29 05:04:23 baud Exp $
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_GLOBALS_H
#define _CASTOR_GLOBALS_H    

#include "osdep_trunk_r21843.h"
#include <stddef.h>                 /* For size_t                    */

EXTERN_C void Cglobals_init (int (*) (int *, void **),
			     int (*) (int *, void *),
			     int (*) (void));
EXTERN_C int Cglobals_get (int *, void **, size_t size);
EXTERN_C void Cglobals_getTid (int *);

#endif /* _CASTOR_GLOBALS_H */
