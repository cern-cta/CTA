/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: skiptape.c,v $ $Revision: 1.8 $ $Date: 2002/04/09 05:07:54 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/tape.h>
#define mtop stop
#define mt_op st_op
#define mt_count st_count
#define MTIOCTOP STIOCTOP
#define MTFSF STFSF
#define MTBSF STRSF
#define MTFSR STFSR
#define MTBSR STRSR
#else
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#endif
#include "Ctape.h"
#include "serrno.h"
#if (defined(__alpha) && defined(__osf__)) || defined(ADSTAR) || defined(IRIX64) || defined(linux)
extern int mt_rescnt;
extern char *sys_errlist[];
skiptpfff(tapefd, path, n)
int tapefd;
char *path;
int n;
{
	int count;
	int errcat;
	char func[16];
	char *msgaddr;
	struct mtop mtop;
	int tobeskipped;

	ENTRY (skiptpfff);
	mtop.mt_op = MTFSF;	/* skip n files forward */
	tobeskipped = n;
	while (tobeskipped > 0) {
		count = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
		mtop.mt_count = count;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
#if defined(IRIX64)
			if ((errno == ENOSPC) || (errno == EIO)) {
#else
			if (errno == EIO) {
#endif
				errcat = gettperror (tapefd, path, &msgaddr);
#if defined(__alpha) && defined(__osf__) && ! defined(DUXV4)
				if (errcat == ETNOSNS)
#else
				if (errcat == ETBLANK)
#endif
					RETURN (tobeskipped - count + mt_rescnt);
			} else
				msgaddr = sys_errlist[errno];
			serrno = errno;
			usrmsg (func, TP042, path, "ioctl", msgaddr);
			RETURN (-1);
		}
		tobeskipped -= count;
	}
	RETURN (0);
}
#endif
skiptpff(tapefd, path, n)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
int n;
{
	int count;
	char func[16];
#if !defined(_WIN32)
	struct mtop mtop;
	int tobeskipped;
#endif

	ENTRY (skiptpff);
#if !defined(_WIN32)
	mtop.mt_op = MTFSF;	/* skip n files forward */
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
#else
	if (SetTapePosition (tapefd, TAPE_SPACE_FILEMARKS, 0, n, 0, (BOOL)1)) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
#endif
	RETURN (0);
}
skiptpfb(tapefd, path, n)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
int n;
{
	int count;
	char func[16];
#if !defined(_WIN32)
	struct mtop mtop;
#endif
	int tobeskipped;

	ENTRY (skiptpfb);
#if !defined(_WIN32)
	mtop.mt_op = MTBSF;	/* skip n files backward */
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
#else
	if (SetTapePosition (tapefd, TAPE_SPACE_FILEMARKS, 0, -n, 0, (BOOL)1)) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
#endif
	RETURN (0);
}
skiptprf(tapefd, path, n)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
int n;
{
	int count;
	char func[16];
#if !defined(_WIN32)
	struct mtop mtop;
#endif
	int tobeskipped;

	ENTRY (skiptprf);
#if !defined(_WIN32)
	mtop.mt_op = MTFSR;	/* skip n blocks forward */
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
#else
	if (SetTapePosition (tapefd, TAPE_SPACE_RELATIVE_BLOCKS, 0, n, 0, (BOOL)1)) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
#endif
	RETURN (0);
}
skiptprb(tapefd, path, n)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
int n;
{
	int count;
	char func[16];
#if !defined(_WIN32)
	struct mtop mtop;
#endif
	int tobeskipped;

	ENTRY (skiptprb);
#if !defined(_WIN32)
	mtop.mt_op = MTBSR;	/* skip n blocks backward */
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
#else
	if (SetTapePosition (tapefd, TAPE_SPACE_RELATIVE_BLOCKS, 0, -n, 0, (BOOL)1)) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
#endif
	RETURN (0);
}
