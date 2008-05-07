/*
 * Copyright (C) 1993-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#include "Cnetdb.h"
#include "Cns.h"
#include "Cns_api.h"
#ifdef CSEC
#include "Csec_api.h"
#endif
#include "marshall.h"
#include "net.h"
#include "serrno.h"

/* send2nsd - send a request to the name server and wait for the reply */

int send2nsdx(socketp, host, reqp, reql, user_repbuf, user_repbuf_len, repbuf2, nbstruct)
int *socketp;
char *host;
char *reqp;
int reql;
char *user_repbuf;
int user_repbuf_len;
void **repbuf2;
int *nbstruct;
{
	int actual_replen = 0;
	int alloced = 0;
	int c;
	char Cnshost[CA_MAXHOSTNAMELEN+1];
#ifdef CSEC
	Csec_context_t ctx;
#endif
	int errflag = 0;
	char func[16];
	char *getconfent();
	char *getenv();
	struct hostent *hp;
	struct Cns_linkinfo *li;
	struct Cns_filereplica *lp;
	int magic;
	int n;
	int nbretry;
	char *p;
	char prtbuf[PRTBUFSZ];
	char *q;
	struct Cns_rep_info *ri;
	int rep_type;
	char repbuf[REPBUFSZ];
	int repbuf2sz;
	int retrycnt = 0;
	int retryint;
	int s;
	char se[CA_MAXHOSTNAMELEN+1];
	char sfn[CA_MAXSFNLEN+1];
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp;
	struct Cns_api_thread_info *thip = NULL;
	int timeout;

	strcpy (func, "send2nsd");
	if (socketp && *socketp >= 0) {	/* connection opened by Cns_list... */
		s = *socketp;
	} else if (socketp == NULL && Cns_apiinit (&thip) == 0 && thip->fd >= 0) {
		s = thip->fd;		/* connection opened by Cns_starttrans */
	} else {			/* connection not yet opened */
		sin.sin_family = AF_INET;
		sin.sin_port = 0;
		if (host && *host)
			strcpy (Cnshost, host);
		else if ((p = getenv (CNS_HOST_ENV)) || (p = getconfent (CNS_SCE, "HOST", 0)))
			strcpy (Cnshost, p);
		else {
#if defined(CNS_HOST)
			strcpy (Cnshost, CNS_HOST);
#else
			gethostname (Cnshost, sizeof(Cnshost));
#endif
			serrno = 0;
		}
		if ((p = strchr (Cnshost, ':'))) {
			*p = '\0';
			sin.sin_port = htons (atoi (p + 1));
		}
		if ((hp = Cgethostbyname (Cnshost)) == NULL) {
			Cns_errmsg (func, NS009, "Host unknown:", Cnshost);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
		if (! sin.sin_port) {
			if ((p = getenv (CNS_PORT_ENV)) || (p = getconfent (CNS_SCE, "PORT", 0))) {
				sin.sin_port = htons ((unsigned short)atoi (p));
			} else if ((sp = Cgetservbyname (CNS_SVC, "tcp"))) {
				sin.sin_port = sp->s_port;
				serrno = 0;
			} else {
				sin.sin_port = htons ((unsigned short)CNS_PORT);
				serrno = 0;
			}
		}

		/* get retry environment variables */
		if ((p = getenv (CNS_CONNTIMEOUT_ENV)) == NULL) {
			timeout = DEFAULT_CONNTIMEOUT;
		} else {
			timeout = atoi (p);
		}

		nbretry = DEFAULT_RETRYCNT;
		if ((p = getenv (CNS_CONRETRY_ENV)) || 
		    (p = getconfent (CNS_SCE, "CONRETRY", 0))) {
			nbretry = atoi(p);
		}

		retryint = RETRYI;
		if ((p = getenv (CNS_CONRETRYINT_ENV)) ||
		    (p = getconfent (CNS_SCE, "CONRETRYINT", 0))) {
			retryint = atoi(p);
		} 

		/* retry as much as the user wishes */
	        while (retrycnt <= nbretry) {

			if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
				Cns_errmsg (func, NS002, "socket", neterror());
				serrno = SECOMERR;
				return (-1);
			}

			if (netconnect_timeout (s, (struct sockaddr *)&sin,
						sizeof(struct sockaddr_in), timeout) < 0) {

				if (serrno == SETIMEDOUT) {
					if (retrycnt == nbretry) {
						Cns_errmsg (func, NS002, "connect", neterror());
						(void) netclose(s);
						return (-1);
					}
				} else {

#if defined(_WIN32)
					if (WSAGetLastError() == WSAECONNREFUSED) {
#else
					if (serrno == ECONNREFUSED) {
#endif
						if (retrycnt == nbretry) {
							Cns_errmsg (func, NS000, Cnshost);
							(void) netclose (s);
							serrno = ENSNACT;
							return (-1);
						}
					} else {
						if (retrycnt == nbretry) {
							Cns_errmsg (func, NS002, "connect", neterror());
							(void) netclose (s);
							serrno = SECOMERR;
							return (-1);
						}
					}
				}
				(void) netclose (s);
			} else {
#ifdef CSEC
				Csec_client_initContext (&ctx, CSEC_SERVICE_TYPE_HOST, NULL);
				if (Cns_apiinit (&thip) == 0 && thip->use_authorization_id &&
				    *thip->Csec_mech && *thip->Csec_auth_id)
					Csec_client_setAuthorizationId (&ctx, thip->Csec_mech,
									thip->Csec_auth_id);
				if (Csec_client_establishContext (&ctx, s) == 0)
					break;
				
				switch (serrno) {
					case SECOMERR:
						Cns_errmsg (func, NS002, "send", "Communication error");
						(void) netclose (s);
						Csec_clearContext (&ctx);
						return (-1);
					case SETIMEDOUT:
						if (retrycnt == nbretry) {
							Cns_errmsg (func, NS002, "send", "Operation timed out");
							(void) netclose (s);
							Csec_clearContext (&ctx);
							return (-1);
						}
						break;
					default:
						Cns_errmsg (func, NS002, "send", "No valid credential found");
						(void) netclose (s);
						Csec_clearContext (&ctx);
						return (-1);
				}
				
				(void) netclose (s);
				Csec_clearContext (&ctx);			
#else
				break;
#endif
			}
			sleep (retryint);
			retrycnt++;
		}

#ifdef CSEC
		Csec_clearContext (&ctx);
#endif
		if (socketp)
			*socketp = s;
	}

	/* send request to name server */
	serrno = 0;
	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			Cns_errmsg (func, NS002, "send", sys_serrlist[SERRNO]);
		else
			Cns_errmsg (func, NS002, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
			else
				Cns_errmsg (func, NS002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == CNS_RC) {
			(void) netclose (s);
			if (thip && s == thip->fd)
				thip->fd = -1;
		}
		if (rep_type == CNS_RC || rep_type == CNS_IRC) {
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if (c > REPBUFSZ) {
			Cns_errmsg (func, "reply too large\n");
			serrno = SEINTERNAL;
			return (-1);
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
			else
				Cns_errmsg (func, NS002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			Cns_errmsg (NULL, "%s", prtbuf);
		} else if (rep_type == MSG_LINKS) {
			if (errflag) continue;
			if (alloced == 0) {
				repbuf2sz = 4096;
				if ((*repbuf2 = malloc (repbuf2sz)) == NULL) {
					errflag++;
					continue;
				}
				alloced = 1;
				*nbstruct = 0;
				li = (struct Cns_linkinfo *) *repbuf2;
			}
			while (p < repbuf + c) {
				if ((char *)li - (char *)(*repbuf2) +
				    sizeof(struct Cns_linkinfo) > repbuf2sz) {
					repbuf2sz += 4096;
					if ((q = realloc (*repbuf2, repbuf2sz)) == NULL) {
						errflag++;
						break;
					}
					*repbuf2 = q;
					li = ((struct Cns_linkinfo *) *repbuf2) + *nbstruct;
				}
				unmarshall_STRING (p, li->path);
				(*nbstruct)++;
				li++;
			}
		} else if (rep_type == MSG_REPLIC) {
			if (errflag) continue;
			if (alloced == 0) {
				repbuf2sz = 4096;
				if ((*repbuf2 = malloc (repbuf2sz)) == NULL) {
					errflag++;
					continue;
				}
				alloced = 1;
				*nbstruct = 0;
				lp = (struct Cns_filereplica *) *repbuf2;
			}
			while (p < repbuf + c) {
				if ((char *)lp - (char *)(*repbuf2) +
				    sizeof(struct Cns_filereplica) > repbuf2sz) {
					repbuf2sz += 4096;
					if ((q = realloc (*repbuf2, repbuf2sz)) == NULL) {
						errflag++;
						break;
					}
					*repbuf2 = q;
					lp = ((struct Cns_filereplica *) *repbuf2) + *nbstruct;
				}
				unmarshall_HYPER (p, lp->fileid);
				unmarshall_HYPER (p, lp->nbaccesses);
				unmarshall_TIME_T (p, lp->atime);
				unmarshall_TIME_T (p, lp->ptime);
				unmarshall_BYTE (p, lp->status);
				unmarshall_BYTE (p, lp->f_type);
				unmarshall_STRING (p, lp->poolname);
				unmarshall_STRING (p, lp->host);
				unmarshall_STRING (p, lp->fs);
				unmarshall_STRING (p, lp->sfn);
				(*nbstruct)++;
				lp++;
			}
		} else if (rep_type == MSG_REPLICP) {
			if (errflag) continue;
			if (alloced == 0) {
				repbuf2sz = 4096;
				if ((*repbuf2 = malloc (repbuf2sz)) == NULL) {
					errflag++;
					continue;
				}
				alloced = 1;
				*nbstruct = 0;
				ri = (struct Cns_rep_info *) *repbuf2;
			}
			while (p < repbuf + c) {
				if ((char *)ri - (char *)(*repbuf2) +
				    sizeof(struct Cns_rep_info) > repbuf2sz) {
					repbuf2sz += 4096;
					if ((q = realloc (*repbuf2, repbuf2sz)) == NULL) {
						errflag++;
						break;
					}
					*repbuf2 = q;
					ri = ((struct Cns_rep_info *) *repbuf2) + *nbstruct;
				}
				unmarshall_HYPER (p, ri->fileid);
				unmarshall_BYTE (p, ri->status);
				unmarshall_STRING (p, se);
				if ((ri->host = strdup (se)) == NULL) {
					errflag++;
					break;
				}
				unmarshall_STRING (p, sfn);
				if ((ri->sfn = strdup (sfn)) == NULL) {
					errflag++;
					break;
				}
				(*nbstruct)++;
				ri++;
			}
		} else if (user_repbuf) {
			if (actual_replen + c <= user_repbuf_len)
				n = c;
			else
				n = user_repbuf_len - actual_replen;
			if (n) {
				memcpy (user_repbuf + actual_replen, repbuf, n);
				actual_replen += n;
			}
		}
	}
	return (c);
}

int send2nsd(socketp, host, reqp, reql, user_repbuf, user_repbuf_len)
int *socketp;
char *host;
char *reqp;
int reql;
char *user_repbuf;
int user_repbuf_len;
{
	return (send2nsdx (socketp, host, reqp, reql, user_repbuf, user_repbuf_len,
	    NULL, NULL));
}
