/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_errmsg.c,v $ $Revision: 1.1 $ $Date: 2004/10/24 22:13:24 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <stdlib.h>
#include "serrno.h"
#include "Cglobals.h"
#include "stagerConstants.h"
#include "Csnprintf.h"

static int errbufp_key = 0;
static int errbuflen_key = 0;
static int outbufp_key = 0;
static int outbuflen_key = 0;
static int logfunction_key = 0;

/*	stager_seterrbuf - set receiving buffer for error messages */

int DLL_DECL stager_seterrbuf(buffer, buflen)
		 char *buffer;
		 int buflen;
{
	char **errbufp;
	int *errbuflenp;

	Cglobals_get (&errbufp_key, (void **)&errbufp, sizeof(char *));
	Cglobals_get (&errbuflen_key, (void **)&errbuflenp, sizeof(int));
	if (errbufp == NULL || errbuflenp == NULL) {
		return(-1);
	}
	*errbufp = buffer;
	*errbuflenp = buflen;
	return(0);
}

/*	stager_setoutbuf - set receiving buffer for output messages */

int DLL_DECL stager_setoutbuf(buffer, buflen)
		 char *buffer;
		 int buflen;
{
	char **outbufp;
	int *outbuflenp;

	Cglobals_get (&outbufp_key, (void **)&outbufp, sizeof(char *));
	Cglobals_get (&outbuflen_key, (void **)&outbuflenp, sizeof(int));
	if (outbufp == NULL || outbuflenp == NULL) {
		return(-1);
	}
	*outbufp = buffer;
	*outbuflenp = buflen;
	return(0);
}

/*	stager_geterrbuf - get receiving buffer for error messages */

int DLL_DECL stager_geterrbuf(buffer, buflen)
		 char **buffer;
		 int *buflen;
{
	char **errbufp;
	int *errbuflenp;

	Cglobals_get (&errbufp_key, (void **)&errbufp, sizeof(char *));
	Cglobals_get (&errbuflen_key, (void **)&errbuflenp, sizeof(int));
	if (errbufp == NULL || errbuflenp == NULL) {
		return(-1);
	}
	*buffer = *errbufp;
	*buflen = *errbuflenp;
	return(0);
}

/*	stager_getoutbuf - get receiving buffer for output messages */

int DLL_DECL stager_getoutbuf(buffer, buflen)
		 char **buffer;
		 int *buflen;
{
	char **outbufp;
	int *outbuflenp;

	Cglobals_get (&outbufp_key, (void **)&outbufp, sizeof(char *));
	Cglobals_get (&outbuflen_key, (void **)&outbuflenp, sizeof(int));
	if (outbufp == NULL || outbuflenp == NULL) {
		return(-1);
	}
	*buffer = *outbufp;
	*buflen = *outbuflenp;
	return(0);
}

/*	stager_setlog - set receiving callback routine for error and output messages */

int DLL_DECL stager_setlog(logfunction)
		 void (*logfunction) _PROTO((int, char *));
{
	void *logfunctionp;

	Cglobals_get (&logfunction_key, &logfunctionp, sizeof(void (*) _PROTO((int, char *))));
	if (logfunctionp == NULL) {
		return(-1);
	}
	* (void (**) _PROTO((int, char *))) logfunctionp = logfunction;
	return(0);
}

/*	stager_getlog - get receiving callback routine for error and output messages */

int DLL_DECL stager_getlog(logfunction)
		 void (**logfunction) _PROTO((int, char *));
{
	void *logfunctionp;

	Cglobals_get (&logfunction_key, &logfunctionp, sizeof(void (*) _PROTO((int, char *))));
	if (logfunctionp == NULL) {
		return(-1);
	}
	*logfunction = * (void (**) _PROTO((int, char *))) logfunctionp;
	return(0);
}

/* stager_errmsg - send error message to user defined client buffer or to stderr */

int DLL_DECL stager_errmsg(char *func, char *msg, ...) {
	va_list args;
	char prtbuf[STAGER_PRTBUFSZ];
	int save_errno;
	char *errbufp;
	int errbuflen;
	void (*logfunction) _PROTO((int, char *));

	if (stager_geterrbuf(&errbufp,&errbuflen) != 0 || stager_getlog(&logfunction) != 0) {
		return(-1);
	}

	save_errno = errno;
	va_start(args, msg);
	if (func) {
		Csnprintf (prtbuf, STAGER_PRTBUFSZ, "%s: ", func);
		prtbuf[STAGER_PRTBUFSZ-1] = '\0';
	} else {
		*prtbuf = '\0';
    }
	if ((strlen(prtbuf) + 1) < STAGER_PRTBUFSZ) {
		Cvsnprintf (prtbuf + strlen(prtbuf), STAGER_PRTBUFSZ - strlen(prtbuf), msg, args);
		prtbuf[STAGER_PRTBUFSZ-1] = '\0';
	}

	if (logfunction != NULL) {
		logfunction(STAGER_MSG_ERR,prtbuf);
	} else {
		if (errbufp != NULL && errbuflen > 0) {
			if (strlen (prtbuf) < errbuflen) {
				strcpy (errbufp, prtbuf);
			} else {
				strncpy (errbufp, prtbuf, errbuflen - 1);
				errbufp[errbuflen-1] = '\0';
			}
		} else {
			fprintf (stderr, "%s", prtbuf);
		}
	}

	va_end (args);
	errno = save_errno;
	return (0);
}

/* stager_outmsg - send output message to user defined client buffer or to stdout */

int DLL_DECL stager_outmsg(char *func, char *msg, ...) {
	va_list args;
	char prtbuf[STAGER_PRTBUFSZ];
	int save_errno;
	char *outbufp;
	int outbuflen;
	void (*logfunction) _PROTO((int, char *));

	if (stager_getoutbuf(&outbufp,&outbuflen) != 0 || stager_getlog(&logfunction) != 0) {
		return(-1);
	}

	save_errno = errno;
	va_start(args,msg);

	if (func) {
		Csnprintf (prtbuf, STAGER_PRTBUFSZ, "%s: ", func);
		prtbuf[STAGER_PRTBUFSZ-1] = '\0';
	} else {
		*prtbuf = '\0';
    }
	if ((strlen(prtbuf) + 1) < STAGER_PRTBUFSZ) {
		Cvsnprintf (prtbuf + strlen(prtbuf), STAGER_PRTBUFSZ - strlen(prtbuf), msg, args);
		prtbuf[STAGER_PRTBUFSZ-1] = '\0';
	}

	if (logfunction != NULL) {
		logfunction(STAGER_MSG_OUT,prtbuf);
	} else {
		if (outbufp != NULL && outbuflen > 0) {
			if (strlen (prtbuf) < outbuflen) {
				strcpy (outbufp, prtbuf);
			} else {
				strncpy (outbufp, prtbuf, outbuflen - 1);
				outbufp[outbuflen-1] = '\0';
			}
		} else {
			fprintf (stdout, "%s", prtbuf);
		}
	}
	va_end (args);
	errno = save_errno;
	return (0);
}



