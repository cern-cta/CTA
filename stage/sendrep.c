/*
 * $Id: sendrep.c,v 1.22 2002/04/30 13:01:05 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.22 $ $Date: 2002/04/30 13:01:05 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include <varargs.h>
#include "marshall.h"
#include "net.h"
#include "osdep.h"
#include "stage_api.h"
#ifndef linux 
extern char *sys_errlist[];
#endif
int iserrmsg _PROTO((char *));
int sendrep _PROTO(());
extern int stglogit _PROTO(());

int sendrep(va_alist) va_dcl
{
#define PROTOBUG 2 /* BUG IN THE PROTOCOL when t_or_d == 'm' , for STGMAGIC <= 2 : need more space */
	va_list args;
	char *file1, *file2;
	struct stgcat_entry *stcp;
	char api_stcp_out[3 * LONGSIZE + PROTOBUG * sizeof(struct stgcat_entry)]; /* We overestimate a bit this buffer length (gaps + strings) */
	char stcp_marshalled[PROTOBUG * sizeof(struct stgcat_entry)];
	int  api_stcp_out_status = 0;
	char api_stpp_out[3 * LONGSIZE + sizeof(struct stgpath_entry)];
	char *func_sendrep = "sendrep";
	char *func_stgdaemon = "stgdaemon";
	char *func;
	char *msg;
	char *p;
	char prtbuf[PRTBUFSZ];
	char *q;
	char *rbp, *sav_rbp_magic;
	int rc;
	int rep_type;
	int req_type;
	char repbuf[REPBUFSZ];
	int repsize;
	int rpfd;
	static char savebuf[256];
	static int saveflag = 0;
	u_signed64 uniqueid;
	int magic;

	va_start (args);
	rpfd = va_arg (args, int);

	if (rpfd > 0) {
		func = func_sendrep;
	} else {
		func = func_stgdaemon;
	}

	rep_type = va_arg (args, int);
	rbp = (rep_type == API_STCP_OUT ? api_stcp_out : (rep_type == API_STPP_OUT ? api_stpp_out : repbuf));
	sav_rbp_magic = rbp;
	marshall_LONG (rbp, STGMAGIC);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_OUT:
	case MSG_ERR:
	case RTCOPY_OUT:
		msg = va_arg (args, char *);
#if (defined(__osf__) && defined(__alpha))
		vsprintf (prtbuf, msg, args);
#else
		vsnprintf (prtbuf, PRTBUFSZ, msg, args);
#endif
		prtbuf[PRTBUFSZ-1] = '\0';
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		if (rep_type == MSG_ERR) stglogit (func, "%s", prtbuf);
		if (rep_type == RTCOPY_OUT) {
			p = prtbuf;
			if (saveflag) {
				if ((q = strchr (p, '\n')) == NULL) {	/* line is still incomplete */
					strcat (savebuf, p);
					goto sndmsg;
				}
				*q = '\0';
				strcat (savebuf, p);
				switch (iserrmsg (savebuf)) {
				case 0:
					break;
				case 1:	/* tpread/tpwrite error */
					stglogit (func, "%s\n", savebuf+16);
					break;
				case 2:	/* cptpdsk/cpdsktp error */
					stglogit (func, "%s\n", savebuf);
				}
				saveflag = 0;
				p = q + 1;
			}
			while ((q = strchr (p, '\n')) != NULL) {
				*q = '\0';
				switch (iserrmsg (p)) {
				case 0:
					break;
				case 1:	/* tpread/tpwrite error */
					stglogit (func, "%s\n", p+16);
					break;
				case 2:	/* cptpdsk/cpdsktp error */
					stglogit (func, "%s\n", p);
				}
				p = q + 1;
			}
			if (strlen (p)) {	/* save incomplete line */
				strcpy (savebuf, p);
				saveflag = 1;
			}
		}
		break;
	case STAGERC:
		req_type = va_arg (args, int);
		magic = va_arg (args, int);
		rc = va_arg (args, int);
		if (magic <= STGMAGIC2) {
			/* We know that this client do not fully support error codes */
			rc = rc_castor2shift(rc);
		} else if ((magic == STGMAGIC3) && (rc == SENAMETOOLONG)) {
			/* Known pb with clients using STGMAGIC3 */
			rc = EINVAL;
		}
		marshall_LONG (sav_rbp_magic, magic);
		marshall_LONG (rbp, rc);
		if (req_type != STAGEQRY) {
			if (magic <= STGMAGIC2) {
				stglogit (func, STG99, rc);
			} else {
				stglogit (func, STG199, rc);
			}
		}
		break;
	case SYMLINK:
		file1 = va_arg (args, char *);
		file2 = va_arg (args, char *);
		marshall_LONG (rbp, strlen (file1) + strlen (file2) + 2);
		marshall_STRING (rbp, file1);
		marshall_STRING (rbp, file2);
		break;
	case RMSYMLINK:
		file1 = va_arg (args, char *);
		marshall_LONG (rbp, strlen (file1) + 1);
		marshall_STRING (rbp, file1);
		break;
	case API_STCP_OUT:
		/* There is another argument on the stack */
		stcp = va_arg (args, struct stgcat_entry *);
		magic = va_arg (args, int);
		api_stcp_out_status = 0;
		marshall_LONG (sav_rbp_magic, magic);
		p = stcp_marshalled;
		marshall_STAGE_CAT (magic, STAGE_OUTPUT_MODE, api_stcp_out_status, p, stcp);
		marshall_LONG(rbp, p - stcp_marshalled);
		marshall_OPAQUE(rbp, stcp_marshalled, p - stcp_marshalled);
 		break;
	case UNIQUEID:
		uniqueid = va_arg (args, u_signed64);
		marshall_LONG (rbp, HYPERSIZE);
		marshall_HYPER (rbp, uniqueid);
		break;
	default:
		va_end (args);
		stglogit (func, "STG02 - unknown rep_type (%d)\n", rep_type);
		if (rpfd >= 0) close (rpfd);
		return (-1);
	}
 sndmsg:
	va_end (args);
	repsize = rbp - (rep_type == API_STCP_OUT ? api_stcp_out : (rep_type == API_STPP_OUT ? api_stpp_out : repbuf));
	if (rpfd >= 0) {
      /* At the startup, rpfd is < 0 */
		if (netwrite_timeout (rpfd, (rep_type == API_STCP_OUT ? api_stcp_out : (rep_type == API_STPP_OUT ? api_stpp_out : repbuf)), repsize, STGTIMEOUT) != repsize) {
			stglogit (func, STG02, "", "write", sys_errlist[errno]);
			if (rep_type == STAGERC)
				close (rpfd);
			return (-1);
		}
		if (rep_type == STAGERC)
			close (rpfd);
	}
	return (0);
}

int iserrmsg(p)
		 char *p;
{
	char *q;

	if (*p == '\0') return (0);
	if (strncmp (p, " CP", 3) == 0) {
		if (*(p+9) == '!') {
			return (2);
		} else {
			return (0);
		}
	}
	if (strncmp (p+16, "tpread", 6) &&
		strncmp (p+16, "tpwrite", 7)) return (2);
	if ((q = strchr (p, ']')) == NULL) return (0);
	if (*(q+3) == '!') {
		return (1);
	} else {
		return (0);
	}
}


