/*
 * $Id: shift.h,v 1.4 2001/05/21 11:07:18 baud Exp $
 */

/*
 * @(#)$RCSfile: shift.h,v $ $Revision: 1.4 $ $Date: 2001/05/21 11:07:18 $ CERN IT-PDP/DC Frederic Hemmer
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DC
 * All rights reserved
 */

/* shift.h      SHIFT defines and macros for general user               */

#ifndef _SHIFT_H_INCLUDED_
#define _SHIFT_H_INCLUDED_

#include <shift/patchlevel.h>   /* Current version number               */
#include <shift/osdep.h>        /* Operating System dependencies        */
#include <shift/serrno.h>       /* SHIFT error numbers and codes        */
#ifdef _WIN32
#include <shift/dirent.h>
#include <shift/statbits.h>
#include <shift/syslog.h>
#endif
#include <shift/log.h>          /* Logging facilities                   */
#include <shift/trace.h>        /* Tracing facilities                   */
#include <shift/rfio_constants.h>
#include <shift/rfio_errno.h>
#include <shift/rfio_api.h>
#include <shift/rfio.h>         /* Remote file I/O defines and Macros   */
#include <shift/rfcntl.h>       /* Remote file I/O heter. file control  */

#endif /* _SHIFT_H_INCLUDED_ */
