/*
 * send2stgd_api.c,v 1.19 2001/05/31 13:52:27 jdurand Exp
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)send2stgd_api.c,v 1.19 2001/05/31 13:52:27 CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <sys/stat.h>
#include "marshall.h"
#include "rfio_api.h"
#include "net.h"
#include "serrno.h"
#include "osdep.h"
#include "Cnetdb.h"
#include "Cglobals.h"
#include "Castor_limits.h"
#include "stage_struct.h"
#include "stage_messages.h"
#include "stage_protocol.h"
#include "socket_timeout.h"
#include "u64subr.h"
#include "stage_api.h"

int dosymlink _PROTO((char *, char *));
void dounlink _PROTO((char *));
int rc_shift2castor _PROTO((int, int));
int send2stgd_sort_stcp _PROTO((int, u_signed64, int, struct stgcat_entry *, int *, struct stgcat_entry **));
int send2stgd_sort_by_fseq _PROTO((CONST void *, CONST void *));
int send2stgd_api_cmp _PROTO((struct stgcat_entry *, struct stgcat_entry *));
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));

/* This macro cleans everything that have to be cleaned, in particular internally memory allocated on the heap */
#define SEND2STGD_API_CLEAN() { \
  if ((_stcp_output != NULL) && (stcp_output == NULL)) { free(_stcp_output) ; _stcp_output = NULL;} \
  if ((_stpp_output != NULL) && (stpp_output == NULL)) { free(_stpp_output) ; _stpp_output = NULL;} \
}

/* This macro makes sure that uniqueid stored in thread-specific */
/* variable is reset before the return so that from now on, any  */
/* stage_kill() hanler call will have NO effect.                 */
#define SEND2STGD_API_RETURN(c) {                 \
	if (stage_setuniqueid((u_signed64) 0) != 0) { \
		stage_errmsg(func, STG02, func,           \
					"stage_setuniqueid",          \
					sstrerror(serrno));           \
        SEND2STGD_API_CLEAN();                    \
		return (-1);                              \
	}                                             \
    SEND2STGD_API_CLEAN();                        \
    if ((c) == 0) serrno = orig_serrno;           \
    return(c);                                    \
}

/* This macro cleans everything that can be cleaned because if it is called there is a error */
/* Note that if _stcp_output != NULL or _stpp_output != NULL it clears and reset it so that */
/* we are sure that the call to macro SEND2STGD_API_CLEAN() will not clear something yet done */
/* Beware !!!! stcp_output is a ** that can contain _stcp_output, e.g. it is possible that */
/* *stcp_output == _stcp_output. Same thing for stpp_output, it is possible that */
/* *stpp_output == _stpp_output ... */

#define SEND2STGD_API_ERROR(c) { \
	if (nstcp_output != NULL) *nstcp_output = 0; \
	if (stcp_output != NULL) { if (*stcp_output != NULL) free(*stcp_output) ; if (*stcp_output == _stcp_output) _stcp_output = NULL; *stcp_output = NULL;} \
	if (nstpp_output != NULL) *nstpp_output = 0; \
	if (stpp_output != NULL) { if (*stpp_output != NULL) free(*stpp_output) ; if (*stpp_output == _stpp_output) _stpp_output = NULL; *stpp_output = NULL;} \
	if (_stcp_output != NULL) { free(_stcp_output) ; _stcp_output = NULL;} \
	if (_stpp_output != NULL) { free(_stpp_output) ; _stpp_output = NULL;} \
	SEND2STGD_API_RETURN(c); \
}

static int uniqueidp_key = 0;
static int laststghostp_key = 0;

int DLL_DECL stage_setuniqueid(uniqueid)
	u_signed64 uniqueid;
{
	u_signed64 *uniqueidp;

	Cglobals_get (&uniqueidp_key, (void **)&uniqueidp, sizeof(u_signed64));
	if (uniqueidp == NULL) {
		return(-1);
	}
	*uniqueidp = uniqueid;
	return(0);
}

int DLL_DECL stage_getuniqueid(uniqueid)
	u_signed64 *uniqueid;
{
	u_signed64 *uniqueidp;

	Cglobals_get (&uniqueidp_key, (void **)&uniqueidp, sizeof(u_signed64));
	if (uniqueidp == NULL) {
		return(-1);
	}
	*uniqueid = *uniqueidp;
	return(0);
}


int DLL_DECL stage_setlaststghost(laststghost)
	char *laststghost;
{
	char *laststghostp;

	Cglobals_get (&laststghostp_key, (void **)&laststghostp, CA_MAXHOSTNAMELEN+1);
	if (laststghostp == NULL) {
		return(-1);
	}
	if (laststghost != NULL) strncpy(laststghostp,laststghost,CA_MAXHOSTNAMELEN);
	return(0);
}

int DLL_DECL stage_getlaststghost(laststghost)
	char **laststghost;
{
	char *laststghostp;

	Cglobals_get (&laststghostp_key, (void **)&laststghostp, CA_MAXHOSTNAMELEN+1);
	if (laststghostp == NULL) {
		return(-1);
	}
	*laststghost = laststghostp;
	return(0);
}


/* This routine takes really action only with STAGERs up to version 1.3.5.0 */
/* Since version 1.3.5.1, the stager always returns only an serrno. The only */
/* conversion needed is for the command-line clients, using rc_castor2shift() */
/* We use the magic number send back by the daemon to detect if we have to do */
/* conversion of not */
int rc_shift2castor(magic,rc)
	int magic;
	int rc;
{
	if (magic > STGMAGIC) {
		/* The daemon returned an serrno, not a SHIFT error code */
		return(rc);
	}

    /* Input  is a SHIFT  return code */
	/* Output is a CASTOR return code */
	switch (rc) {
	case ETHELDERR:
		return(ETHELD);
	case BLKSKPD:
		return(ERTBLKSKPD);
	case TPE_LSZ:
		return(ERTTPE_LSZ);
	case MNYPARI:
		return(ERTMNYPARY);
	case LIMBYSZ:
		return(ERTLIMBYSZ);
	case USERR:
		return(EINVAL);
	case SYERR:
		return(SESYSERR);
	case SHIFT_ESTNACT:
		return(ESTNACT);
	case SHIFT_ECUPVNACT:
		return(ECUPVNACT);
	case CHECKSUMERR:
		return(SECHECKSUM);
	case REQKILD:
		return(ESTKILLED);
	case CLEARED:
		return(ESTCLEARED);
	case CONFERR:
		return(ESTCONF);
	case LNKNSUP:
		return(ESTLNKNSUP);
	default:
		return(rc);
	}
}

int DLL_DECL send2stgd(host, req_type, flags, reqp, reql, want_reply, user_repbuf, user_repbuf_len, nstcp_input, stcp_input, nstcp_output, stcp_output, nstpp_output, stpp_output)
	char *host;
	int req_type;
	u_signed64 flags;
	char *reqp;
	int reql;
	int want_reply;
	char *user_repbuf;
	int user_repbuf_len;
	int nstcp_input;
	struct stgcat_entry *stcp_input;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
	int *nstpp_output;
	struct stgpath_entry **stpp_output;
{
	int c;
	char file2[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];
	char func[16];
	char *getconfent();
	char *getenv();
	struct hostent *hp;
	int link_rc;
	int magic_server;
	int n;
	char *p;
	char prtbuf[PRTBUFSZ];
	int rep_type;
	char repbuf[REPBUFSZ];
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp = NULL;
	int stg_s;
	char stghost[CA_MAXHOSTNAMELEN+1];
	char *stagehost = STAGEHOST;
	int stg_service = 0;
	int stage_timeout;
	int connect_timeout;
	int connect_rc;
	int _nstcp_output = 0;
	struct stgcat_entry *_stcp_output = NULL;
	int _nstpp_output = 0;
	struct stgpath_entry *_stpp_output = NULL;
	u_signed64 uniqueid = 0;
	u_signed64 current_uniqueid = 0;
	int save_serrno, orig_serrno;
	int magic_client = stage_stgmagic();
	int on = 1;                /* For socket options               */

	strcpy (func, "send2stgd");
	orig_serrno = serrno;

    /* We initialize output values */
	if (nstcp_output != NULL) *nstcp_output = 0;
	if (stcp_output != NULL) *stcp_output = NULL;
	if (nstpp_output != NULL) *nstpp_output = 0;
	if (stpp_output != NULL) *stpp_output = NULL;

	/* We remind current uniqueid value */
	if (stage_getuniqueid(&current_uniqueid) != 0) {
		stage_errmsg(func, STG02, func, "stage_getuniqueid", sstrerror(serrno));
		return (-1);
	}

    /* We always reset our uniqueid value */
    if (stage_setuniqueid(uniqueid) != 0) {
		stage_errmsg(func, STG02, func, "stage_setuniqueid", sstrerror(serrno));
		return (-1);
    }

	link_rc = 0;
	if ((p = getenv ("STAGE_PORT")) == NULL &&
		(p = getconfent("STG", "PORT",0)) == NULL) {
		if ((sp = Cgetservbyname (STAGE_NAME, STAGE_PROTO)) == NULL) {
			if ((stg_service = STAGE_PORT) <= 0) {
				stage_errmsg (func, STG09, STAGE_NAME, "service from environment or configuration is <= 0");
				serrno = SENOSSERV;
				SEND2STGD_API_ERROR(-1);
			}
		}
	} else {
		if ((stg_service = atoi(p)) <= 0) {
			stage_errmsg (func, STG09, STAGE_NAME, "service from environment or configuration is <= 0");
			serrno = SENOSSERV;
			SEND2STGD_API_ERROR(-1);
		}
	}

	if (host == NULL) {
		if ((p = getenv ("STAGE_HOST")) == NULL &&
			(p = getconfent("STG", "HOST",0)) == NULL) {
			strcpy (stghost, stagehost);
		} else {
			strcpy (stghost, p);
		}
	} else {
		strcpy (stghost, host);
	}

	if ((p = getenv ("STAGE_TIMEOUT")) == NULL &&
		(p = getconfent("STG", "TIMEOUT",0)) == NULL) {
		stage_timeout = -1;
	} else {
		stage_timeout = atoi(p);
	}

	if ((p = getenv ("STAGE_CONNTIMEOUT")) == NULL &&
		(p = getconfent("STG", "CONNTIMEOUT",0)) == NULL) {
		connect_timeout = -1;
	} else {
		connect_timeout = atoi(p);
	}

	if ((hp = Cgethostbyname(stghost)) == NULL) {
		stage_errmsg (func, STG09, "Host unknown:", stghost);
		serrno = SENOSHOST;
		SEND2STGD_API_ERROR(-1);
	}
	sin.sin_family = AF_INET;
	sin.sin_port = (stg_service > 0 ? htons((u_short) stg_service) : sp->s_port);
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((stg_s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		stage_errmsg (func, STG02, "", "socket", neterror());
		serrno = SECOMERR;
		SEND2STGD_API_ERROR(-1);
	}

#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
	/* Set socket option */
	setsockopt(stg_s,SOL_SOCKET,SO_KEEPALIVE,(char *) &on,sizeof(on));
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	serrno = 0;
	if (connect_timeout > 0) {
		connect_rc = netconnect_timeout (stg_s, (struct sockaddr *) &sin, sizeof(sin), connect_timeout);
	} else {
		connect_rc = connect (stg_s, (struct sockaddr *) &sin, sizeof(sin));
	}
	if (connect_rc < 0) {
		if (
#if defined(_WIN32)
			WSAGetLastError() == WSAECONNREFUSED
#else
			errno == ECONNREFUSED
#endif
			) {
			stage_errmsg (func, STG00, stghost, neterror());
			(void) netclose (stg_s);
			serrno = ESTNACT;
			SEND2STGD_API_ERROR(-1);
		} else {
			stage_errmsg (func, STG02, stghost, "connect", neterror());
			(void) netclose (stg_s);
			serrno = SECOMERR;
			SEND2STGD_API_ERROR(-1);
		}
	}

	if ((n = netwrite_timeout (stg_s, reqp, reql, STGTIMEOUT)) != reql) {
		save_serrno = serrno;
		if (n == 0)
			stage_errmsg (func, STG02, "", "send", sys_serrlist[SERRNO]);
		else
			stage_errmsg (func, STG02, "", "send", neterror());
		(void) netclose (stg_s);
		serrno = save_serrno;
		SEND2STGD_API_ERROR(-1);
	}
	if (! want_reply) {
		(void) netclose (stg_s);
		SEND2STGD_API_RETURN(0);
	}
	
    /* We restore our uniqueid value */
    if (stage_setuniqueid(current_uniqueid) != 0) {
		stage_errmsg(func, STG02, func, "stage_setuniqueid", sstrerror(serrno));
		(void) netclose (stg_s);
		SEND2STGD_API_ERROR(-1);
    }

    /* Connect and netwrite suceeded : Let's remind the latest stghost contacted in this thread */
    stage_setlaststghost(stghost);

	while (1) {

		if ((n = netread_timeout(stg_s, repbuf, 3 * LONGSIZE, stage_timeout)) != (3 * LONGSIZE)) {
			save_serrno = serrno;
			if (! (
#if !defined(_WIN32)
				(serrno == SECONNDROP || errno == ECONNRESET)
#else
				(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
				)) {
				if (n == 0) {
					stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
				} else {
					stage_errmsg (func, STG02, "", "recv", neterror());
				}
			}
			(void) netclose (stg_s);
			serrno = save_serrno;
			SEND2STGD_API_ERROR(-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic_server) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;

		if (rep_type == STAGERC) {
			(void) netclose (stg_s);
			if (c) {
				serrno = rc_shift2castor(magic_server,c);
				c = -1;
			}
			break;
		} else if (rep_type == API_STCP_OUT) {
			int noutput_data;
			char *msgbuf_out;
			int unmarshall_status;

			p = repbuf;
			if (c <= 0) {
				serrno = SEINTERNAL;
				SEND2STGD_API_ERROR(-1);
			}
			if ((msgbuf_out = (char *) malloc((size_t) c)) == NULL) {
				serrno = SEINTERNAL;
				SEND2STGD_API_ERROR(-1);
			}
			if ((noutput_data = netread_timeout(stg_s, msgbuf_out, c, stage_timeout)) != c) {
				save_serrno = serrno;
				if (noutput_data == 0)
					stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
				else
					stage_errmsg (func, STG02, "", "recv", neterror());
				(void) netclose (stg_s);
				serrno = save_serrno;
				free(msgbuf_out);
				SEND2STGD_API_ERROR(-1);
			}
			if (_nstcp_output++ == 0) {
				if ((_stcp_output = (struct stgcat_entry *) calloc(1,sizeof(struct stgcat_entry))) == NULL) {
					(void) netclose (stg_s);
					serrno = SEINTERNAL;
					free(msgbuf_out);
					SEND2STGD_API_ERROR(-1);
				}
			} else {
				struct stgcat_entry *dummy;
				if ((dummy = (struct stgcat_entry *) realloc(_stcp_output,_nstcp_output * sizeof(struct stgcat_entry))) == NULL) {
					(void) netclose (stg_s);
					serrno = SEINTERNAL;
					free(msgbuf_out);
					SEND2STGD_API_ERROR(-1);
				}
				_stcp_output = dummy;
				memset(&(_stcp_output[_nstcp_output - 1]),0,sizeof(struct stgcat_entry));
			}
			p = msgbuf_out;
			unmarshall_status = 0;
			unmarshall_STAGE_CAT(magic_server,magic_client,STAGE_OUTPUT_MODE,unmarshall_status,p,&(_stcp_output[_nstcp_output - 1]));
			free(msgbuf_out);
			if (unmarshall_status == 0) stage_callback(&(_stcp_output[_nstcp_output - 1]),NULL);
			if (stcp_output != NULL) *stcp_output = _stcp_output;
			if (nstcp_output != NULL) *nstcp_output = _nstcp_output;
			continue;
		} else if (rep_type == API_STPP_OUT) {
			int noutput_data;
			char *msgbuf_out;
			int unmarshall_status;

			p = repbuf;
			if (c <= 0) {
				serrno = SEINTERNAL;
				SEND2STGD_API_ERROR(-1);
			}
			if ((msgbuf_out = (char *) malloc((size_t) c)) == NULL) {
				serrno = SEINTERNAL;
				SEND2STGD_API_ERROR(-1);
			}
			if ((noutput_data = netread_timeout(stg_s, msgbuf_out, c, stage_timeout)) != c) {
				save_serrno = serrno;
				if (noutput_data == 0)
					stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
				else
					stage_errmsg (func, STG02, "", "recv", neterror());
				(void) netclose (stg_s);
				serrno = save_serrno;
				free(msgbuf_out);
				SEND2STGD_API_ERROR(-1);
			}
			if (_nstpp_output++ == 0) {
				if ((_stpp_output = (struct stgpath_entry *) calloc(1,sizeof(struct stgpath_entry))) == NULL) {
					(void) netclose (stg_s);
					serrno = SEINTERNAL;
					free(msgbuf_out);
					SEND2STGD_API_ERROR(-1);
				}
			} else {
				struct stgpath_entry *dummy;
				if ((dummy = (struct stgpath_entry *) realloc(_stpp_output,_nstpp_output * sizeof(struct stgpath_entry))) == NULL) {
					(void) netclose (stg_s);
					serrno = SEINTERNAL;
					free(msgbuf_out);
					SEND2STGD_API_ERROR(-1);
				}
                _stpp_output = dummy;
                memset(&(_stpp_output[_nstpp_output - 1]),0,sizeof(struct stgpath_entry));
			}
			p = msgbuf_out;
			unmarshall_status = 0;
			unmarshall_STAGE_PATH(magic_server,magic_client,STAGE_OUTPUT_MODE,unmarshall_status,p,&(_stpp_output[_nstpp_output - 1]));
			free(msgbuf_out);
			if (unmarshall_status == 0) stage_callback(NULL,&(_stpp_output[_nstpp_output - 1]));
			if (stpp_output != NULL) *stpp_output = _stpp_output;
			if (nstpp_output != NULL) *nstpp_output = _nstpp_output;
			continue;
		} else if (rep_type == UNIQUEID) {
			char uniqueidbuf[HYPERSIZE];
			int  nread;

			if ((nread = netread_timeout(stg_s, uniqueidbuf, HYPERSIZE, stage_timeout)) != HYPERSIZE) {
				save_serrno = serrno;
				if (nread == 0)
					stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
				else
					stage_errmsg (func, STG02, "", "recv", neterror());
				(void) netclose (stg_s);
				serrno = save_serrno;
				SEND2STGD_API_ERROR(-1);
			}
			p = uniqueidbuf;
			unmarshall_HYPER(p, uniqueid);
			if (stage_setuniqueid(uniqueid) != 0) {
				(void) netclose (stg_s);
				serrno = SEINTERNAL;
				SEND2STGD_API_ERROR(-1);
			}
			continue;
		}
		if ((n = netread_timeout(stg_s, repbuf, c, stage_timeout)) != c) {
			save_serrno = serrno;
			if (n == 0)
				stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				stage_errmsg (func, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			serrno = save_serrno;
			SEND2STGD_API_ERROR(-1);
		}
		p = repbuf;
		unmarshall_STRINGN (p, prtbuf, REPBUFSZ);
		switch (rep_type) {
		case MSG_OUT:
		case RTCOPY_OUT:
			stage_outmsg (NULL, "%s", prtbuf);
			break;
		case MSG_ERR:
			stage_errmsg (NULL, "%s", prtbuf);
			break;
		case SYMLINK:
			unmarshall_STRINGN (p, file2, CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2);
			if ((c = dosymlink (prtbuf, file2)))
				link_rc = c;
			break;
		case RMSYMLINK:
			dounlink (prtbuf);
			break;
		default:
			break;
		}
	}
	if (send2stgd_sort_stcp(req_type,flags,nstcp_input,stcp_input,nstcp_output,stcp_output) != 0) {
		SEND2STGD_API_ERROR(-1);
    }

	SEND2STGD_API_RETURN(c ? c : link_rc);
}

int dosymlink (file1, file2)
	char *file1;
	char *file2;
{
	char *filename;
	char func[16];
	char *host;
	int remote;
	
	strcpy (func, "send2stgd");
	remote = rfio_parseln (file2, &host, &filename, NORDLINKS);
	PRE_RFIO;
	if (rfio_symlink (file1, file2) &&
		((!remote && errno != EEXIST) || (remote && rfio_errno != EEXIST))) {
		stage_errmsg (func, STG02, file1, "symlink", rfio_serror());
		if (serrno == SEOPNOTSUP) {
			serrno = ESTLNKNSUP;
			return (-1);
		}
		if ((remote &&
			 (rfio_errno == EACCES || rfio_errno == ENOENT)) ||
			(remote == 0 && (errno == EACCES || errno == ENOENT))) {
			serrno = ESTLNKNCR;
			return (-1);
		} else {
			serrno = SESYSERR;
			return (-1);
		}
	}
	return (0);
}

void dounlink (path)
	char *path;
{
	char *filename;
	char func[16];
	char *host;
#if !defined(_WIN32)
	struct stat64 st;
#endif
	int remote;
	
	strcpy (func, "send2stgd");
	remote = rfio_parseln (path, &host, &filename, NORDLINKS);
	PRE_RFIO;
	if (rfio_unlink (path)) {
		if ((remote && rfio_errno == ENOENT) ||
			(remote == 0 && errno == ENOENT)) return;
#if !defined(_WIN32)
		if (getuid() || (remote && rfio_errno != EACCES) ||
			(remote == 0 && errno != EACCES) ||
			strncmp (filename, "/afs/", 5) == 0) {
#endif
			stage_errmsg (func, STG02, path, "unlink", rfio_serror());
			return;
		}
#if !defined(_WIN32)
		PRE_RFIO;
		if (rfio_lstat64 (path, &st) != 0) {
			stage_errmsg (func, STG02, path, "unlink(lstat64)", rfio_serror());
			return;
		}
		setgid (st.st_gid);
		setuid (st.st_uid);
		PRE_RFIO;
		if (rfio_unlink (path)) {
			stage_errmsg (func, STG02, path, "unlink", rfio_serror());
			return;
		}
	}
#endif
}

int send2stgd_sort_stcp(req_type,flags,nstcp_input,stcp_input,nstcp_output,stcp_output)
	int req_type;
	u_signed64 flags;
	int nstcp_input;
	struct stgcat_entry *stcp_input;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
{
	int *seen;
	int i, j, k, nseen;
	int status;
	int nstcp_output_duplicate_found;
	struct stgcat_entry *new_stcp_output;

	/* No input/output structures ? */
	if (stcp_input == NULL || nstcp_input <= 0 ||
		stcp_output == NULL || nstcp_output == NULL || *nstcp_output <= 0) return(0);

	/* No sort needed ? */
	/*
	  STAGE_QRY and STAGE_UPDC are not yet supported
	*/
	/*
	  if (req_type == STAGE_QRY ||
      req_type == STAGE_UPDC) {
	  return(0);
	  }
	*/

	if ((req_type == STAGE_IN) && ((flags & STAGE_COFF) == STAGE_COFF)) {
		/* In the case "stagein -c off", the output is known to be sorted by fseq */
		qsort((void *) *stcp_output, *nstcp_output, sizeof(struct stgcat_entry), &send2stgd_sort_by_fseq);
		return(0);
	}

	/* In all other cases we order by making correspondance with initial request, except for requests than can  */
	/* receive in output more entries than in input, in particular: STAGE_QRY */
	if (req_type == STAGE_QRY) {
		return(0);
	}

	if ((seen = (int *) malloc(*nstcp_output * sizeof(int))) == NULL) {
		serrno = SEINTERNAL;
		return(-1);
	}
	nseen = 0;
	for (i = 0; i < *nstcp_output; i++) {
		seen[i] = -1;
		status = -1;
		for (j = 0; j < nstcp_input; j++) {
			if (send2stgd_api_cmp(&(stcp_input[j]),&((*stcp_output)[i])) == 0) {
				/* We believe the output No i corresponds to input No j */
				status = 0;
				seen[i] = j;
				nseen++;
				break;
			}
		}
		if (status != 0) {
			/* Not found !?? */
			serrno = SEINTERNAL;
			free(seen);
			return(-1);
		}
	}

	/* All output structures has been recognized ? */
	if (nseen != *nstcp_output) {
		free(seen);
		return(-1);
	}

	/* It is possible that we identified output No i to a wrong input No j because, for example */
	/* in case of CASTOR files, input did contain ONLY the HSM name */
	/* So we look again to output structures only and resolve the cases when we said that */
	/* too seen[] values were identical */
	nstcp_output_duplicate_found = 1;      /* This statement just forces the while() to start... */
	while (nstcp_output_duplicate_found != 0) {
		nstcp_output_duplicate_found = 0;
		for (i = 0; i < *nstcp_output; i++) {
			for (j = i + 1; j < *nstcp_output; j++) {
				if (seen[j] == seen[i]) {
					/* Output structures are claimed to refer to same input structure No seen[i] */
					if (send2stgd_api_cmp(&((*stcp_output)[i]),&((*stcp_output)[j])) != 0) {
						/* But this is not true */
						/* Since i != j per construction it is possible to find one of them */
						/* that we can overwrite */
						if (seen[i] != i) {
							seen[i] = i;
						} else {
							seen[j] = j;
						}
						nstcp_output_duplicate_found = 1;
					}
				}
			}
			if (nstcp_output_duplicate_found != 0) {
				/* We found a wrong duplicate */
				/* Since we modified one entry we have to reverify since the beginning */
				break;
			}
		}
	}


	/* We possibliy shrunk output structures to handle the special case when there was an */
	/* internal retry by the stgdaemon - that assigned another catalog entry but for the */
	/* same input entry - typically in case of ENOSPC entries successful after a retry */
	/* for example */
	/* This is visible from the API by the fact that multiple seen[] can reference the same */
	/* input index, for example seen[i1] == seen[i2] == j */
	/* Per construction, if two seen[] values are equal the latest one is always the correct */
	/* one */
	nstcp_output_duplicate_found = 1;      /* This statement just forces the while() to start... */
	while (nstcp_output_duplicate_found != 0) {
		nstcp_output_duplicate_found = 0;
		for (i = 0; i < *nstcp_output; i++) {
			for (j = i + 1; j < *nstcp_output; j++) {
				if (seen[j] == seen[i]) {
					/* Here is a case when entry No j (j > i per construction) refers to the same input */
					/* This means that (*stcp_output)[i] is to be replaced by (*stcp_output)[j] and the */
					/* total number of output entries decremented */
					/* Finally all output entries with index > j have to be shifted as well */
					(*stcp_output)[i] = (*stcp_output)[j];
					for (k = j + 1; k < *nstcp_output; k++) {
						(*stcp_output)[k - 1] = (*stcp_output)[k];
						seen[k - 1] = seen[k];
					}
					nstcp_output_duplicate_found = 1;
					*nstcp_output = *nstcp_output - 1;
					break;
				}
			}
			if (nstcp_output_duplicate_found != 0) {
				/* We found a duplicate for Entry No i and it had already been overwriten */
				/* With this break we force a new check on all the entries so that all */
				/* duplicates must have been removed */
				break;
			}
		}
	}

	/* We reorder output structures to match input structures original order */

	/* [Bug #1854] stage_in_tape does not reorder correctly structures in output */
	/* It is an error to do permutations. We must only do what we prepared the code */
	/* for: refill an array with the correct indices - that's all */
	if ((new_stcp_output = (struct stgcat_entry *) malloc(*nstcp_output * sizeof(struct stgcat_entry))) == NULL) {
		serrno = ENOMEM;
		free(seen);
		return(-1);
	}

	for (i = 0; i < *nstcp_output; i++) {
		for (j = 0; j < *nstcp_output; j++) {
			/* We search which entry should be at position i */
			if (seen[j] == i) {
				new_stcp_output[i] = (*stcp_output)[j];
				/* Please note that we made sure (see upper) that all entries are in seen[] array */
				break;
			}
		}
	}
	free(*stcp_output);
	*stcp_output = new_stcp_output;
	free(seen);

	/* OK */
	return(0);
}

int send2stgd_sort_by_fseq(p1,p2)
	CONST void *p1;
	CONST void *p2;
{
	struct stgcat_entry *stcp1 = (struct stgcat_entry *) p1;
	struct stgcat_entry *stcp2 = (struct stgcat_entry *) p2;
	int fseq1, fseq2;

	fseq1 = atoi(stcp1->u1.t.fseq);
	fseq2 = atoi(stcp2->u1.t.fseq);

	if (fseq1 < fseq2) {
		return(-1);
	} else if (fseq1 > fseq2) {
		return(1);
	} else {
		return(0);
	}
}

int send2stgd_api_cmp(stcp1,stcp2)
	struct stgcat_entry *stcp1;
	struct stgcat_entry *stcp2;
{
	int i;
	
	/* stcp1 is the reference and we check if it matches stcp2 */
	if (stcp1->blksize     != 0    && stcp1->blksize  != stcp2->blksize)  {
		return(-1);
	}
	if (stcp1->charconv    != '\0' && stcp1->charconv != stcp2->charconv) {
		return(-1);
	}
	if (stcp1->keep        != '\0' && stcp1->keep     != stcp2->keep) {
		return(-1);
	}
	if (stcp1->lrecl       != 0    && stcp1->lrecl    != stcp2->lrecl)  {
		return(-1);
	}
	if (stcp1->nread       != 0    && stcp1->nread    != stcp2->nread)  {
		return(-1);
	}
	/*
	if (stcp1->poolname[0] != '\0' && strcmp(stcp1->poolname,stcp2->poolname) != 0) {
		return(-1);
	}
	*/
	if (stcp1->recfm[0]    != '\0' && strcmp(stcp1->recfm,stcp2->recfm) != 0) {
		return(-1);
	}
	/*
	  if (stcp1->size        != 0    && stcp1->size     != stcp2->size)  {
	  return(-1);
	  }
	*/
	if (stcp1->t_or_d == '\0') {
		stage_errmsg(NULL, "### stcp1->t_or_d is empty !?\n");
		return(-1);
	}
	if (stcp1->t_or_d   != stcp2->t_or_d) {
		/* This is accepted only if input was 'h' or 'm' and output was 'm' or 'h' */
		if ((stcp1->t_or_d == 'h' && stcp2->t_or_d != 'm') ||
			(stcp1->t_or_d == 'm' && stcp2->t_or_d != 'h')) {
			return(-1);
		}
	}

	switch (stcp2->t_or_d) { /* We use stcp2 because it might be 'm' or 'h' when input is 'm' */
	case 't':
		if (stcp1->u1.t.den[0]      != '\0' && strcmp(stcp1->u1.t.den,stcp2->u1.t.den) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.dgn[0]      != '\0' && strcmp(stcp1->u1.t.dgn,stcp2->u1.t.dgn) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.fid[0]      != '\0' && strcmp(stcp1->u1.t.fid,stcp2->u1.t.fid) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.filstat     != '\0' && stcp1->u1.t.filstat != stcp2->u1.t.filstat) {
			return(-1);
		}
		if (stcp1->u1.t.fseq[0]     != '\0' && strcmp(stcp1->u1.t.fseq,stcp2->u1.t.fseq) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.lbl[0]      != '\0' && strcmp(stcp1->u1.t.lbl,stcp2->u1.t.lbl) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.retentd     != 0    && stcp1->u1.t.retentd != stcp2->u1.t.retentd) {
			return(-1);
		}
		if (stcp1->u1.t.tapesrvr[0] != '\0' && strcmp(stcp1->u1.t.tapesrvr,stcp2->u1.t.tapesrvr) != 0) {
			return(-1);
		}
		if (stcp1->u1.t.E_Tflags    != 0    && stcp1->u1.t.E_Tflags != stcp2->u1.t.E_Tflags) {
			return(-1);
		}
		for (i = 0; i < MAXVSN; i++) {
			if (stcp1->u1.t.vid[i][0] != '\0' && strcmp(stcp1->u1.t.vid[i],stcp2->u1.t.vid[i]) != 0) {
				return(-1);
			}
			if (stcp1->u1.t.vsn[i][0] != '\0' && strcmp(stcp1->u1.t.vsn[i],stcp2->u1.t.vsn[i]) != 0) {
				return(-1);
			}
		}
		break;
	case 'd':
		if (stcp1->u1.d.xfile[0] != '\0' && strcmp(stcp1->u1.d.xfile,stcp2->u1.d.xfile) != 0) {
			return(-1);
		}
		if (stcp1->u1.d.Xparm[0] != '\0' && strcmp(stcp1->u1.d.Xparm,stcp2->u1.d.Xparm) != 0) {
			return(-1);
		}
		break;
	case 'a':
		if (stcp1->u1.d.xfile[0] != '\0' && strcmp(stcp1->u1.d.xfile,stcp2->u1.d.xfile) != 0) {
			return(-1);
		}
		break;
	case 'm':
		if (stcp1->u1.m.xfile[0] != '\0' && strcmp(stcp1->u1.m.xfile,stcp2->u1.m.xfile) != 0) {
			return(-1);
		}
		break;
	case 'h':
		/* HSM name is volatile */
		/*
		if (stcp1->u1.h.xfile[0]       != '\0' && strcmp(stcp1->u1.h.xfile,stcp2->u1.h.xfile) != 0) {
			return(-1);
		}
		*/
		if (stcp1->u1.h.server[0]      != '\0' && strcmp(stcp1->u1.h.server,stcp2->u1.h.server) != 0) {
			return(-1);
		}
		if (stcp1->u1.h.fileid         != 0    && stcp1->u1.h.fileid != stcp2->u1.h.fileid) {
			return(-1);
		}
		if (stcp1->u1.h.fileclass      != 0    && stcp1->u1.h.fileclass != stcp2->u1.h.fileid) {
			return(-1);
		}
		if (stcp1->u1.h.tppool[0]      != '\0' && strcmp(stcp1->u1.h.tppool,stcp2->u1.h.tppool) != 0) return(-1);
		/* These are 'volative' values */
		/*
		  if (stcp1->u1.h.retenp_on_disk != 0     && stcp1->u1.h.retenp_on_disk != stcp2->u1.h.retenp_on_disk) return(-1);
		  if (stcp1->u1.h.mintime_beforemigr != 0 && stcp1->u1.h.mintime_beforemigr != stcp2->u1.h.mintime_beforemigr) return(-1);
		*/
		break;
	}

	/* OK */
	return(0);
}

