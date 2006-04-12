/*
 * $Id: times.h,v 1.1 2006/04/12 08:36:48 motiakov Exp $
 */

/*
 * @(#)$RCSfile: times.h,v $ $Revision: 1.1 $ $Date: 2006/04/12 08:36:48 $ CERN IT-ADC Vitaly Motyakov
 */
 
/*
 * Copyright (C) 1993-2001 by CERN/IT/ADC
 * All rights reserved
 */

/************************************ 

wrapper for the times() function
it returns the same values for 
all four times.

*************************************/

#ifndef __win32_times_h
#define __win32_times_h

#include <osdep.h>
#include <time.h>

struct tms {
       clock_t tms_utime;  /* user time */
       clock_t tms_stime;  /* system time */
       clock_t tms_cutime; /* user time of children */
       clock_t tms_cstime; /* system time of children */
};
 
EXTERN_C clock_t  DLL_DECL times _PROTO((struct tms *));

#endif /* __win32_times_h */
