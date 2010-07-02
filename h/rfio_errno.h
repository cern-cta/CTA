/*
 * $Id: rfio_errno.h,v 1.6 2001/06/17 14:06:24 baud Exp $
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: rfio_errno.h,v $ $Revision: 1.6 $ $Date: 2001/06/17 14:06:24 $
 */

/* rfio_errno.h   Thread safe rfio_errno  */

#ifndef _RFIO_ERRNO_H_INCLUDED_
#define _RFIO_ERRNO_H_INCLUDED_

#ifndef _OSDEP_H_INCLUDED_
#include <osdep.h>                  /* EXTERN_C */
#endif
#include <stddef.h>                 /* For size_t                    */

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
/*
 * Multi-thread (MT) environment
 */
EXTERN_C int *C__rfio_errno (void);

/*
 * Thread safe rfio_errno. Note, C__rfio_errno is defined in Cglobals.c rather
 * than rfio/error.c .
 */
#define rfio_errno (*C__rfio_errno())

#else /* _REENTRANT || _THREAD_SAFE ... */
/*
 * non-MT environment
 */
extern  int     rfio_errno;                 /* Global error number          */
#endif /* _REENTRANT || _TREAD_SAFE */

EXTERN_C char *rfio_errmsg_r (int, int, char*, size_t);
EXTERN_C char *rfio_errmsg (int, int);
EXTERN_C char *rfio_serror_r (char*, size_t);
EXTERN_C char *rfio_serror (void);
EXTERN_C void rfio_perror (char *);

#endif /* _RFIO_ERRNO_H_INCLUDED_ */
