/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_errmsg.c,v $ $Revision: 1.8 $ $Date: 2001/01/31 19:00:01 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <stdarg.h>         /* _WIN32's UNIX compatibility */
#endif
#include <varargs.h>
#include <sys/types.h>
#include <stdlib.h>
#include "serrno.h"
#include "stage.h"
#include "stage_api.h"
#include "Cglobals.h"

static int errbufp_key = 0;
static int errbuflen_key = 0;
static int outbufp_key = 0;
static int outbuflen_key = 0;
static int logfunction_key = 0;
static int callbackfunction_key = 0;

/*	stage_seterrbuf - set receiving buffer for error messages */

int DLL_DECL stage_seterrbuf(buffer, buflen)
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

/*	stage_setoutbuf - set receiving buffer for output messages */

int DLL_DECL stage_setoutbuf(buffer, buflen)
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

/*	stage_setcallback - set callback function */

int DLL_DECL stage_setcallback(callbackfunction)
		 void (*callbackfunction) _PROTO((struct stgcat_entry *, struct stgpath_entry *));
{
	void *callbackfunctionp;

	Cglobals_get (&callbackfunction_key, &callbackfunctionp, sizeof(void (*) _PROTO((struct stgcat_entry *, struct stgpath_entry *))));
	if (callbackfunctionp == NULL) {
		return(-1);
	}
	* (void (**) _PROTO((struct stgcat_entry *, struct stgpath_entry *))) callbackfunctionp = callbackfunction;
	return(0);
}

/*	stage_geterrbuf - get receiving buffer for error messages */

int DLL_DECL stage_geterrbuf(buffer, buflen)
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

/*	stage_getoutbuf - get receiving buffer for output messages */

int DLL_DECL stage_getoutbuf(buffer, buflen)
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

/*	stage_getcallback - get callback function */

int DLL_DECL stage_getcallback(callbackfunction)
		 void (**callbackfunction) _PROTO((struct stgcat_entry *, struct stgpath_entry *));
{
	void *callbackfunctionp;

	Cglobals_get (&callbackfunction_key, &callbackfunctionp, sizeof(void (*) _PROTO((struct stgcat_entry *, struct stgpath_entry *))));
	if (callbackfunctionp == NULL) {
		return(-1);
	}
	*callbackfunction = * (void (**) _PROTO((struct stgcat_entry *, struct stgpath_entry *))) callbackfunctionp;
	return(0);
}

/*	stage_setlog - set receiving callback routine for error and output messages */

int DLL_DECL stage_setlog(logfunction)
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

/*	stage_getlog - get receiving callback routine for error and output messages */

int DLL_DECL stage_getlog(logfunction)
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

/* stage_errmsg - send error message to user defined client buffer or to stderr */

int DLL_DECL stage_errmsg(va_alist)
		 va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	char *errbufp;
	int errbuflen;
	void (*logfunction) _PROTO((int, char *));

	if (stage_geterrbuf(&errbufp,&errbuflen) != 0 || stage_getlog(&logfunction) != 0) {
		return(-1);
	}

	save_errno = errno;
	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);

	if (logfunction != NULL) {
		logfunction(MSG_ERR,prtbuf);
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

	errno = save_errno;
	return (0);
}

/* stage_outmsg - send output message to user defined client buffer or to stdout */

int DLL_DECL stage_outmsg(va_alist)
		 va_dcl
{
	va_list args;
	char *func;
	char *msg;
	char prtbuf[PRTBUFSZ];
	int save_errno;
	char *outbufp;
	int outbuflen;
	void (*logfunction) _PROTO((int, char *));

	if (stage_getoutbuf(&outbufp,&outbuflen) != 0 || stage_getlog(&logfunction) != 0) {
		return(-1);
	}

	save_errno = errno;
	va_start (args);
	func = va_arg (args, char *);
	msg = va_arg (args, char *);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);

	if (logfunction != NULL) {
		logfunction(MSG_OUT,prtbuf);
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
	errno = save_errno;
	return (0);
}

/* stage_callback - call callback function */

int DLL_DECL stage_callback(stcp,stpp)
		struct stgcat_entry *stcp;
		struct stgpath_entry *stpp;
{
	void (*callbackfunction) _PROTO((struct stgcat_entry *, struct stgpath_entry *));

	if (stage_getcallback(&callbackfunction) != 0) {
		return(-1);
	}

	if (callbackfunction != NULL)
		callbackfunction(stcp,stpp);

	return (0);
}


