/*
 * $Id: vmgr_main.c,v 1.6 2005/12/12 10:25:53 lopic3 Exp $
 *
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: vmgr_main.c,v $ $Revision: 1.6 $ $Date: 2005/12/12 10:25:53 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/time.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif
#include "Cinit.h"
#include "Cnetdb.h"
#include "Cpool_api.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_server.h"
#ifdef CSEC
#include "Csec_api.h"
#endif

int being_shutdown;
char db_pwd[33];
char db_srvr[33];
char db_user[33];
char func[16];
int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;
struct vmgr_srv_thread_info vmgr_srv_thread_info[VMGR_NBTHREADS];

vmgr_main(main_args)
struct main_args *main_args;
{
	int c;
	FILE *fopen(), *cf;
	char cfbuf[80];
	struct vmgr_dbfd dbfd;
	void *doit(void *);
	char domainname[CA_MAXHOSTNAMELEN+1];
	struct sockaddr_in from;
	int fromlen = sizeof(from);
	char *getconfent();
	int i;
	int ipool;
	int on = 1;	/* for REUSEADDR */
	char *p;
	char *p_p, *p_s, *p_u;
	fd_set readfd, readmask;
	int rqfd;
	int s;
	struct sockaddr_in sin;
	struct servent *sp;
	int thread_index;
	struct timeval timeval;
	char vmgrconfigfile[CA_MAXPATHLEN+1];

	jid = getpid();
	strcpy (func, "vmgr_serv");
	vmgrlogit (func, "started\n");
	gethostname (localhost, CA_MAXHOSTNAMELEN+1);
	if (strchr (localhost, '.') == NULL) {
		if (Cdomainname (domainname, sizeof(domainname)) < 0) {
			vmgrlogit (func, "Unable to get domainname\n");
			exit (SYERR);
		}
		strcat (localhost, ".");
		strcat (localhost, domainname);
	}

	/* get login info from the volume manager config file */

	if (strncmp (VMGRCONFIG, "%SystemRoot%\\", 13) == 0 &&
	   (p = getenv ("SystemRoot")))
		sprintf (vmgrconfigfile, "%s%s", p, strchr (VMGRCONFIG, '\\'));
	else
		strcpy (vmgrconfigfile, VMGRCONFIG);
	if ((cf = fopen (vmgrconfigfile, "r")) == NULL) {
		vmgrlogit (func, VMG23, vmgrconfigfile);
		return (CONFERR);
	}
	if (fgets (cfbuf, sizeof(cfbuf), cf) &&
	    strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n")) &&
	    (p_p = strtok (NULL, "@\n")) && (p_s = strtok (NULL, "\n"))) {
		strcpy (db_user, p_u);
		strcpy (db_pwd, p_p);
		strcpy (db_srvr, p_s);
	} else {
		vmgrlogit (func, VMG09, vmgrconfigfile, "incorrect");
		return (CONFERR);
	}
	(void) fclose (cf);

	(void) vmgr_init_dbpkg ();

	/* create a pool of threads */

	if ((ipool = Cpool_create (VMGR_NBTHREADS, NULL)) < 0) {
		vmgrlogit (func, VMG02, "Cpool_create", sstrerror(serrno));
		return (SYERR);
	}
	for (i = 0; i < VMGR_NBTHREADS; i++) {
		vmgr_srv_thread_info[i].s = -1;
		vmgr_srv_thread_info[i].dbfd.idx = i;
	}

	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

	/* open request socket */

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		vmgrlogit (func, VMG02, "socket", neterror());
		return (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	sin.sin_family = AF_INET ;
#ifdef CSEC
	if ((p = getenv ("SVMGR_PORT")) || (p = getconfent ("SVMGR", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("svmgr", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)SVMGR_PORT);
	}
#else
	if ((p = getenv ("VMGR_PORT")) || (p = getconfent ("VMGR", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("vmgr", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)VMGR_PORT);
	}
#endif
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
		vmgrlogit (func, VMG02, "setsockopt", neterror());
	if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		vmgrlogit (func, VMG02, "bind", neterror());
		return (CONFERR);
	}
	listen (s, 5) ;

	FD_SET (s, &readmask);

		/* main loop */

	while (1) {
		if (being_shutdown) {
			int nb_active_threads = 0;
			for (i = 0; i < VMGR_NBTHREADS; i++) {
				if (vmgr_srv_thread_info[i].s >= 0) {
					nb_active_threads++;
					continue;
				}
				if (vmgr_srv_thread_info[i].db_open_done)
					(void) vmgr_closedb (&vmgr_srv_thread_info[i].dbfd);
			}
			if (nb_active_threads == 0)
				return (0);
		}
		if (FD_ISSET (s, &readfd)) {
			FD_CLR (s, &readfd);
			rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
			if ((thread_index = Cpool_next_index (ipool)) < 0) {
				vmgrlogit (func, VMG02, "Cpool_next_index",
					sstrerror(serrno));
				if (serrno == SEWOULDBLOCK) {
					sendrep (rqfd, VMGR_RC, serrno);
					continue;
				} else
					return (SYERR);
			}
			vmgr_srv_thread_info[thread_index].s = rqfd;
			if (Cpool_assign (ipool, &doit,
			    &vmgr_srv_thread_info[thread_index], 1) < 0) {
				vmgr_srv_thread_info[thread_index].s = -1;
				vmgrlogit (func, VMG02, "Cpool_assign", sstrerror(serrno));
				return (SYERR);
			}
		}
                memcpy (&readfd, &readmask, sizeof(readmask));
                timeval.tv_sec = CHECKI;
                timeval.tv_usec = 0;
                if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
                        FD_ZERO (&readfd);
                }
	}
}

main()
{
#if ! defined(_WIN32)
	if ((maxfds = Cinitdaemon ("vmgr_serv", NULL)) < 0)
		exit (SYERR);
	exit (vmgr_main (NULL));
#else
	if (Cinitservice ("vmgr", &vmgr_main))
		exit (SYERR);
#endif
}

void *
doit(arg)
void *arg;
{
	int c;
	char *clienthost;
	int magic;
	char req_data[REQBUFSZ-3*LONGSIZE];
	int req_type = 0;
	struct vmgr_srv_thread_info *thip = (struct vmgr_srv_thread_info *) arg;
#ifdef CSEC
	char *username;
	Csec_server_reinitContext(&(thip->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
	if (Csec_server_establishContext(&(thip->sec_ctx),thip->s) < 0) {
	  vmgrlogit(func, "Could not establish context: %s !\n", Csec_getErrorMessage());
	  netclose (thip->s);
	  thip->s = -1;
	  return (NULL);
	}
	/* Connection could be done from another castor service */
	if ((c = Csec_server_isClientAService(&(thip->sec_ctx))) >= 0) {
	  vmgrlogit(func, "CSEC: Client is castor service type: %d\n", c);
	  thip->Csec_service_type = c;
	}
	else {
	  if (Csec_server_mapClientToLocalUser(&(thip->sec_ctx), &username, &(thip->Csec_uid), &(thip->Csec_gid)) == 0) {
	    vmgrlogit(func, "CSEC: Client is %s (%d/%d)\n",
		      username,
		      thip->Csec_uid,
		      thip->Csec_gid);
	    thip->Csec_service_type = -1;
	  }
	  else {
	    vmgrlogit(func, "CSEC: Can't get client username\n");
	    netclose (thip->s);
	    thip->s = -1;
	    return (NULL);
	  }
	}

#endif

	if ((c = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) == 0)
		procreq (magic, req_type, req_data, clienthost, thip);
	else if (c > 0)
		sendrep (thip->s, VMGR_RC, c);
	else
		netclose (thip->s);
	thip->s = -1;
	return (NULL);
}

getreq(s, magic, req_type, req_data, clienthost)
int s;
int *magic;
int *req_type;
char *req_data;
char **clienthost;
{
	struct sockaddr_in from;
	int fromlen = sizeof(from);
	struct hostent *hp;
	int l;
	int msglen;
	int n;
	char *rbp;
	char req_hdr[3*LONGSIZE];

	serrno = 0;
	l = netread_timeout (s, req_hdr, sizeof(req_hdr), VMGR_TIMEOUT);
	if (l == sizeof(req_hdr)) {
		rbp = req_hdr;
		unmarshall_LONG (rbp, n);
		*magic = n;
		unmarshall_LONG (rbp, n);
		*req_type = n;
		unmarshall_LONG (rbp, msglen);
		if (msglen > REQBUFSZ) {
			vmgrlogit (func, VMG46, REQBUFSZ);
			return (-1);
		}
		l = msglen - sizeof(req_hdr);
		n = netread_timeout (s, req_data, l, VMGR_TIMEOUT);
		if (being_shutdown) {
			return (EVMGRNACT);
		}
		if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
			vmgrlogit (func, VMG02, "getpeername", neterror());
			return (SEINTERNAL);
		}
		hp = Cgethostbyaddr ((char *)(&from.sin_addr),
			sizeof(struct in_addr), from.sin_family);
		if (hp == NULL)
			*clienthost = inet_ntoa (from.sin_addr);
		else
			*clienthost = hp->h_name ;
		return (0);
	} else {
		if (l > 0)
			vmgrlogit (func, VMG04, l);
		else if (l < 0)
			vmgrlogit (func, VMG02, "netread", neterror());
		return (SEINTERNAL);
	}
}

proclistreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int c;
	DBLISTPTR dblistptr;
	int endlist = 0;
	int new_req_type = -1;
	int rc = 0;
	fd_set readfd, readmask;
	struct vmgr_tape_info tape;
	struct timeval timeval;

	memset (&dblistptr, 0, sizeof(DBLISTPTR));

	/* wait for list requests and process them */

	FD_ZERO (&readmask);
	FD_SET (thip->s, &readmask);
	while (1) {
		switch (req_type) {
		case VMGR_LISTDENMAP:
			if (c = vmgr_srv_listdenmap (magic, req_data,
			    clienthost, thip, endlist, &dblistptr))
				return (c);
			break;
		case VMGR_LISTDGNMAP:
			if (c = vmgr_srv_listdgnmap (magic, req_data,
			    clienthost, thip, endlist, &dblistptr))
				return (c);
			break;
		case VMGR_LISTLIBRARY:
			if (c = vmgr_srv_listlibrary (magic, req_data,
			    clienthost, thip, endlist, &dblistptr))
				return (c);
			break;
		case VMGR_LISTMODEL:
			if (c = vmgr_srv_listmodel (magic, req_data,
			    clienthost, thip, endlist, &dblistptr))
				return (c);
			break;
		case VMGR_LISTPOOL:
			if (c = vmgr_srv_listpool (magic, req_data,
			    clienthost, thip, endlist, &dblistptr))
				return (c);
			break;
		case VMGR_LISTTAPE:
			if (c = vmgr_srv_listtape (magic, req_data,
			    clienthost, thip, &tape, endlist, &dblistptr))
				return (c);
			break;
		}
		if (endlist) break;
		sendrep (thip->s, VMGR_IRC, 0);
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = VMGR_LISTTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
			endlist = 1;
			continue;
		}
		if (rc = getreq (thip->s, &magic, &new_req_type, req_data, &clienthost) != 0) {
			endlist = 1;
			continue;
		}
		if (new_req_type != req_type)
			endlist = 1;
	}
	return (rc);
}

procreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int c;

	/* connect to the database if not done yet */

	if (! thip->db_open_done) {
		if (Cupv_seterrbuf (thip->errbuf, PRTBUFSZ)) {
			c = SEINTERNAL;
			sendrep (thip->s, MSG_ERR, "Cupv_seterrbuf error: %s\n",
			    sstrerror(serrno));
			sendrep (thip->s, VMGR_RC, c);
			return;
		}
		if (req_type != VMGR_SHUTDOWN) {
			if (vmgr_opendb (db_srvr, db_user, db_pwd, &thip->dbfd) < 0) {
				c = serrno;
				sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
				sendrep (thip->s, VMGR_RC, c);
				return;
			}
			thip->db_open_done = 1;
		}
	}
	switch (req_type) {
	case VMGR_DELTAPE:
		c = vmgr_srv_deletetape (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTTAPE:
		c = vmgr_srv_entertape (magic, req_data, clienthost, thip);
		break;
	case VMGR_GETTAPE:
		c = vmgr_srv_gettape (magic, req_data, clienthost, thip);
		break;
	case VMGR_MODTAPE:
		c = vmgr_srv_modifytape (magic, req_data, clienthost, thip);
		break;
	case VMGR_QRYTAPE:
		c = vmgr_srv_querytape (magic, req_data, clienthost, thip);
		break;
	case VMGR_UPDTAPE:
		c = vmgr_srv_updatetape (magic, req_data, clienthost, thip);
		break;
	case VMGR_SHUTDOWN:
		c = vmgr_srv_shutdown (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELMODEL:
		c = vmgr_srv_deletemodel (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTMODEL:
		c = vmgr_srv_entermodel (magic, req_data, clienthost, thip);
		break;
	case VMGR_MODMODEL:
		c = vmgr_srv_modifymodel (magic, req_data, clienthost, thip);
		break;
	case VMGR_QRYMODEL:
		c = vmgr_srv_querymodel (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELPOOL:
		c = vmgr_srv_deletepool (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTPOOL:
		c = vmgr_srv_enterpool (magic, req_data, clienthost, thip);
		break;
	case VMGR_MODPOOL:
		c = vmgr_srv_modifypool (magic, req_data, clienthost, thip);
		break;
	case VMGR_QRYPOOL:
		c = vmgr_srv_querypool (magic, req_data, clienthost, thip);
		break;
	case VMGR_TPMOUNTED:
		c = vmgr_srv_tpmounted (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELDENMAP:
		c = vmgr_srv_deletedenmap (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTDENMAP:
		c = vmgr_srv_enterdenmap (magic, req_data, clienthost, thip);
		break;
	case VMGR_LISTDENMAP:
	case VMGR_LISTDGNMAP:
	case VMGR_LISTLIBRARY:
	case VMGR_LISTMODEL:
	case VMGR_LISTPOOL:
	case VMGR_LISTTAPE:
		c = proclistreq (magic, req_type, req_data, clienthost, thip);
		break;
	case VMGR_RECLAIM:
		c = vmgr_srv_reclaim (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELLIBRARY:
		c = vmgr_srv_deletelibrary (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTLIBRARY:
		c = vmgr_srv_enterlibrary (magic, req_data, clienthost, thip);
		break;
	case VMGR_MODLIBRARY:
		c = vmgr_srv_modifylibrary (magic, req_data, clienthost, thip);
		break;
	case VMGR_QRYLIBRARY:
		c = vmgr_srv_querylibrary (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELDGNMAP:
		c = vmgr_srv_deletedgnmap (magic, req_data, clienthost, thip);
		break;
	case VMGR_ENTDGNMAP:
		c = vmgr_srv_enterdgnmap (magic, req_data, clienthost, thip);
		break;
	case VMGR_DELTAG:
		c = vmgr_srv_deltag (magic, req_data, clienthost, thip);
		break;
	case VMGR_GETTAG:
		c = vmgr_srv_gettag (magic, req_data, clienthost, thip);
		break;
	case VMGR_SETTAG:
		c = vmgr_srv_settag (magic, req_data, clienthost, thip);
		break;
	default:
		sendrep (thip->s, MSG_ERR, VMG03, req_type);
		c = SEINTERNAL;
	}
	sendrep (thip->s, VMGR_RC, c);
}
