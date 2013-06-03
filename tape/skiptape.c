/*
 * $Id: skiptape.c,v 1.13 2008/10/28 08:04:11 wiebalck Exp $
 */

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <linux/version.h>
#include <sys/utsname.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

extern int mt_rescnt;

int skiptpfff(const int tapefd, const char *const path, const int n) {

	/* skip n files forward */

	int count;
	int errcat;
	char func[16];
	char *msgaddr;
	struct mtop mtop;
	int tobeskipped;
        static struct utsname un;
        int major = 0; 
        int minor = 0; 
        int patch = 0; 
        int nr = 0;

	ENTRY (skiptpfff);
	mtop.mt_op = MTFSF;
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
			if (errno == EIO) {
				errcat = gettperror (tapefd, path, &msgaddr);
                                uname(&un);
                                nr = sscanf(un.release, "%d.%d.%d", &major, &minor, &patch);
                                if (nr == 3 && major == 2) {
                                        if ((minor >= 6) && ((errcat == ETBLANK) || (errcat == ETNOSNS))) {
                                                RETURN (tobeskipped - count + mt_rescnt);
                                        } else if ((minor < 6) && (errcat == ETBLANK)) {
                                                RETURN (tobeskipped - count + mt_rescnt);
                                        }
                                }
			} else
				msgaddr = strerror(errno);
			serrno = errno;
			usrmsg (func, TP042, path, "ioctl", msgaddr);
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}

int skiptpff(const int tapefd, const char *const path, const int n) {

	/* skip n files forward */

	int count;
	char func[16];
	struct mtop mtop;
	int tobeskipped;

	ENTRY (skiptpff);
	mtop.mt_op = MTFSF;
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
			serrno = rpttperror (func, tapefd, path, "ioctl");
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}

int skiptpfb(const int tapefd, const char *const path, const int n) {

        /* skip n files backward */

	int count;
	char func[16];
	struct mtop mtop;
	int tobeskipped;

	ENTRY (skiptpfb);
	mtop.mt_op = MTBSF;
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
			serrno = rpttperror (func, tapefd, path, "ioctl");
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}

int skiptprf(const int tapefd, const char *const path, const int n, const int silent) {

	/* skip n blocks forward */

	int count;
	char func[16];
	struct mtop mtop;
	int tobeskipped;

	ENTRY (skiptprf);
	mtop.mt_op = MTFSR;
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
                        if (!silent) {
                                serrno = rpttperror (func, tapefd, path, "ioctl");
                        }
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}

int skiptprb(const int tapefd, const char *const path, const int n) {

        /* skip n blocks backward */

	int count;
	char func[16];
	struct mtop mtop;
	int tobeskipped;

	ENTRY (skiptprb);
	mtop.mt_op = MTBSR;
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
			serrno = rpttperror (func, tapefd, path, "ioctl");
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}

int skip2eod(const int tapefd, const char *const path) {
        
        /* Skip to the end of data. */

        char func[16];
	struct mtop mtop;

	ENTRY (skip2eod);

	mtop.mt_op = MTEOM;
	mtop.mt_count = 1;
	if (ioctl(tapefd, MTIOCTOP, &mtop) < 0) {
		serrno = rpttperror (func, tapefd, path, "ioctl"); 
		RETURN (-1);
	}
	RETURN (0);
}
