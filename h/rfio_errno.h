/*
 * $Id: rfio_errno.h,v 1.6 2001/06/17 14:06:24 baud Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */

/* rfio_errno.h   Thread safe rfio_errno  */

#ifndef _RFIO_ERRNO_H_INCLUDED_
#define _RFIO_ERRNO_H_INCLUDED_

#ifndef _OSDEP_H_INCLUDED_
#include <osdep.h>                  /* EXTERN_C */
#endif
#include <stddef.h>                 /* For size_t                    */

/*
 * Multi-thread (MT) environment
 */
EXTERN_C int *C__rfio_errno (void);

/*
 * Thread safe rfio_errno. Note, C__rfio_errno is defined in Cglobals.c rather
 * than rfio/error.c .
 */
#define rfio_errno (*C__rfio_errno())

EXTERN_C char *rfio_errmsg_r (int, int, char*, size_t);
EXTERN_C char *rfio_errmsg (int, int);
EXTERN_C char *rfio_serror_r (char*, size_t);
EXTERN_C char *rfio_serror (void);
EXTERN_C void rfio_perror (char *);

#endif /* _RFIO_ERRNO_H_INCLUDED_ */
