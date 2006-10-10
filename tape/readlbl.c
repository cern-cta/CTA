/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: readlbl.c,v $ $Revision: 1.12 $ $Date: 2006/10/10 14:16:25 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	readlbl - read one possible label record */
/*	return	-1 and serrno set in case of error
 *		0	if a 80 characters record was read
 *		1	if the record length was different
 *		2	for EOF
 *		3	for blank tape
 */
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(linux)
#include <sys/mtio.h>
#include <sys/utsname.h>
#endif
#include "Ctape.h"
#include "serrno.h"
readlbl(tapefd, path, lblbuf)
int tapefd;
char *path;
char *lblbuf;
{
	int errcat;
	char func[16];
	char *msgaddr;
	int n;
	int rc;
    int c;
#if defined(linux) 
    struct mtget mt_info;
    static struct utsname un;
    int major = 0;
    int minor = 0;
    int patch = 0;
    int nr = 0;
#endif

	ENTRY (readlbl);
	if ((n = read (tapefd, lblbuf, LBLBUFSZ)) < 0) {
#if sgi
		if (errno == ENOSPC) RETURN (3);	/* blank tape */
		if (errno != ENOMEM) {	/* not large block */
#else
#if sun
		if (errno != EINVAL && errno != ENOMEM) {	/* not large block */
#else
#if __alpha && __osf__
		if (errno == ENOSPC) {
			RETURN (3);	/* blank tape */
		} else {
#else
#if _IBMR2
		if (errno != ENOMEM) {	/* not large block */
#else
#if linux
		if (errno != ENOMEM) {	/* not large block */
#else
		{
#endif
#endif
#endif
#endif
#endif
			if (errno == ENOSPC) {
				msgaddr = "";
				serrno = ETPARIT;
			} else if (errno == EIO) {
				errcat = gettperror (tapefd, path, &msgaddr);
#if defined(sun) || defined(RS6000PCTA) || defined(ADSTAR) || defined(hpux) || defined(linux)
				if (errcat == ETBLANK) RETURN (3);
#endif
				serrno = (errcat > 0) ? errcat : EIO;
			} else {
				msgaddr = strerror(errno);
				serrno = errno;
#if defined(_IBMR2)
				if (errno == EMEDIA) serrno = ETPARIT;
#endif
			}
			usrmsg (func, TP042, path, "read", msgaddr);
			RETURN (-1);
		}
	}
	if (n == 0) {
#if defined(linux) 
        /* try first to determine blank tape via st macro for 2.6 kernels */
        uname(&un);
        nr = sscanf(un.release, "%d.%d.%d", &major, &minor, &patch);
        if (nr == 3 && major == 2) {
          if (minor >= 6) {
            if (ioctl (tapefd, MTIOCGET, &mt_info) >= 0) {
              if (GMT_EOD(mt_info.mt_gstat))
                 RETURN(3);	/* blank tape - end of data */
            }
          }
        } 
#endif

#if defined(sun) || defined(linux)
		if (gettperror (tapefd, path, &msgaddr) == ETBLANK)
			RETURN (3);	/* blank tape */
#endif
		RETURN (2);	/* tapemark */
	}
	if (n != 80) RETURN (1);
	lblbuf[80] = '\0';
	RETURN (0);
}
