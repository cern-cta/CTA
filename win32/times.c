/*
 * $Id: times.c,v 1.1 2006/04/12 08:36:48 motiakov Exp $
 */

/*
 * Copyright (C) 2006 by CERN/IT/ADC
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: times.c,v $ $Revision: 1.1 $ $Date: 2006/04/12 08:36:48 $ CERN/IT/ADC Vitaly Motyakov";
#endif /* not lint */

#include "times.h"

clock_t
times(buf)
struct tms *buf;
{
        clock_t ret = clock();
        buf->tms_utime = ret;
        buf->tms_stime = ret;
        buf->tms_cutime = ret;
        buf->tms_cstime = ret;
	return (ret);
}
