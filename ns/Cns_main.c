
/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_main.c,v $ $Revision: 1.14 $ $Date: 2006/05/11 12:38:37 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#include "Cns.h"
#include "Cns_server.h"
#include "Cpool_api.h"
#ifdef CSEC
#include "Csec_api.h"
#endif
#include "Cupv_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"

int being_shutdown;
char *cmd;
char db_name[33];
char db_pwd[33];
char db_srvr[33];
char db_user[33];
char func[16];
int jid;
char lcgdmmapfile[CA_MAXPATHLEN+1];
char localdomain[CA_MAXHOSTNAMELEN+1];
char localhost[CA_MAXHOSTNAMELEN+1];
char logfile[CA_MAXPATHLEN+1];
int maxfds;
int rdonly;
struct Cns_srv_thread_info *Cns_srv_thread_info;

Cns_main(main_args)
struct main_args *main_args;
{
	int c;
	FILE *fopen(), *cf;
	char cfbuf[80];
	struct Cns_dbfd dbfd;
	struct Cns_file_metadata direntry;
	void *doit(void *);
	char *dp;
	struct sockaddr_in from;
	int fromlen = sizeof(from);
	char *getconfent();
	int i;
	int ipool;
	int nbthreads = CNS_NBTHREADS;
	int on = 1;	/* for REUSEADDR */
	char nsconfigfile[CA_MAXPATHLEN+1];
	char *p;
	char *p_n, *p_p, *p_s, *p_u;
	fd_set readfd, readmask;
	int rqfd;
	int s;
	struct sockaddr_in sin;
	struct servent *sp;
	int thread_index;
	struct timeval timeval;

	jid = getpid();
	strcpy (func, "Cns_serv");
	nsconfigfile[0] = '\0';
	strcpy (logfile, LOGFILE);
#if ! defined(_WIN32)
	if ((cmd = strrchr (main_args->argv[0], '/')))
#else
	if ((cmd = strrchr (main_args->argv[0], '\\')))
#endif
		cmd++;
	else
		cmd = main_args->argv[0];

	/* process command line options if any */

	while ((c = getopt (main_args->argc, main_args->argv, "c:l:m:rt:")) != EOF) {
		switch (c) {
		case 'c':
			strncpy (nsconfigfile, optarg, sizeof(nsconfigfile));
			nsconfigfile[sizeof(nsconfigfile) - 1] = '\0';
			break;
		case 'l':
			strncpy (logfile, optarg, sizeof(logfile));
			logfile[sizeof(logfile) - 1] = '\0';
			break;
		case 'm':
			strncpy (lcgdmmapfile, optarg, sizeof(lcgdmmapfile));
			lcgdmmapfile[sizeof(lcgdmmapfile) - 1] = '\0';
			break;
		case 'r':
			rdonly++;
			break;
		case 't':
			if ((nbthreads = strtol (optarg, &dp, 10)) < 0 ||
			    nbthreads >= CNS_MAXNBTHREADS || *dp != '\0') {
				nslogit (func, "Invalid number of threads: %s\n",
				    optarg);
				exit (USERR);
			}
			break;
		}
	}

	nslogit (func, "started\n");
	gethostname (localhost, CA_MAXHOSTNAMELEN+1);
	if (Cdomainname (localdomain, sizeof(localdomain)) < 0) {
		nslogit (func, "Unable to get domainname\n");
		exit (SYERR);
	}
	if (strchr (localhost, '.') == NULL) {
		strcat (localhost, ".");
		strcat (localhost, localdomain);
	}

	/* get DB login info from the name server config file */

	if (! *nsconfigfile) {
		if (strncmp (NSCONFIG, "%SystemRoot%\\", 13) == 0 &&
		    (p = getenv ("SystemRoot")))
			sprintf (nsconfigfile, "%s%s", p, strchr (NSCONFIG, '\\'));
		else
			strcpy (nsconfigfile, NSCONFIG);
	}
	if ((cf = fopen (nsconfigfile, "r")) == NULL) {
		nslogit (func, NS023, nsconfigfile);
		return (CONFERR);
	}
	if (fgets (cfbuf, sizeof(cfbuf), cf) &&
		strlen (cfbuf) >= 5) {
		p_u = strtok (cfbuf, "/\n");
		p_p = strtok (NULL, "@\n");
		p_s = strtok (NULL, "/\n");
		p_n = strtok (NULL, "\n");
		db_user[0]= db_pwd[0] = db_srvr[0] = '\0';
		if (p_u != NULL) strcpy (db_user, p_u);
		if (p_p != NULL) strcpy (db_pwd, p_p);
		if (p_s != NULL) strcpy (db_srvr, p_s);
		if (p_n != NULL)  {
			strcpy (db_name, p_n);
		} else {
			strcpy(db_name, "Cns_db");
		}
	} else {
		nslogit (func, NS009, nsconfigfile, "incorrect");
		return (CONFERR);
	}
	(void) fclose (cf);

	(void) Cns_init_dbpkg ();

	/* create entry in the catalog for "/" if not already done */

	memset (&dbfd, 0, sizeof(dbfd));
	dbfd.idx = nbthreads;
	if (Cns_opendb (db_srvr, db_user, db_pwd, db_name, &dbfd) < 0)
		return (SYERR);
	if (Cns_get_fmd_by_fullid (&dbfd, (u_signed64) 0, "/", &direntry, 0, NULL) < 0) {
		if (serrno != ENOENT)
			return (SYERR);
		nslogit (func, "creating /\n");
		memset (&direntry, 0, sizeof(direntry));
		direntry.fileid = 2;
		strcpy (direntry.name, "/");
		direntry.filemode = S_IFDIR | 0755;
		direntry.atime = time (0);
		direntry.mtime = direntry.atime;
		direntry.ctime = direntry.atime;
		direntry.status = '-';
		(void) Cns_start_tr (0, &dbfd);
		if (Cns_insert_fmd_entry (&dbfd, &direntry) < 0) {
			(void) Cns_abort_tr (&dbfd);
			(void) Cns_closedb (&dbfd);
			return (SYERR);
		}
		(void) Cns_end_tr (&dbfd);
	}
	(void) Cns_closedb (&dbfd);

	/* create a pool of threads */

	if ((ipool = Cpool_create (nbthreads, NULL)) < 0) {
		nslogit (func, NS002, "Cpool_create", sstrerror(serrno));
		return (SYERR);
	}
	if ((Cns_srv_thread_info =
	    calloc (nbthreads, sizeof(struct Cns_srv_thread_info))) == NULL) {
		nslogit (func, NS002, "calloc", strerror(errno));
		return (SYERR);
	}
	for (i = 0; i < nbthreads; i++) {
		(Cns_srv_thread_info+i)->s = -1;
		(Cns_srv_thread_info+i)->dbfd.idx = i;
	}

	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

	/* open request socket */

	serrno = 0;
	if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		nslogit (func, NS002, "socket", neterror());
		return (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	sin.sin_family = AF_INET ;
	if ((p = getenv (CNS_PORT_ENV)) || (p = getconfent (CNS_SCE, "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname (CNS_SVC, "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)CNS_PORT);
	}
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	serrno = 0;
	if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
		nslogit (func, NS002, "setsockopt", neterror());
	if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		nslogit (func, NS002, "bind", neterror());
		return (CONFERR);
	}
	listen (s, 5) ;

	FD_SET (s, &readmask);

		/* main loop */

	while (1) {
		if (being_shutdown) {
			int nb_active_threads = 0;
			for (i = 0; i < nbthreads; i++) {
				if ((Cns_srv_thread_info+i)->s >= 0) {
					nb_active_threads++;
					continue;
				}
				if ((Cns_srv_thread_info+i)->db_open_done)
					(void) Cns_closedb (&(Cns_srv_thread_info+i)->dbfd);
#ifdef CSEC
				(void) Csec_clearContext (&(Cns_srv_thread_info+i)->sec_ctx);
#endif
			}
			if (nb_active_threads == 0)
				return (0);
		}
		if (FD_ISSET (s, &readfd)) {
			FD_CLR (s, &readfd);
			rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
			{
			  int on = 1;
			  /* Set socket option */
			  setsockopt(rqfd,SOL_SOCKET,SO_KEEPALIVE,(char *) &on,sizeof(on));
			}
#endif
			if ((thread_index = Cpool_next_index (ipool)) < 0) {
				nslogit (func, NS002, "Cpool_next_index",
					sstrerror(serrno));
				if (serrno == SEWOULDBLOCK) {
					sendrep (rqfd, CNS_RC, serrno);
					continue;
				} else
					return (SYERR);
			}
			(Cns_srv_thread_info+thread_index)->s = rqfd;
			if (Cpool_assign (ipool, &doit,
			    Cns_srv_thread_info+thread_index, 1) < 0) {
				(Cns_srv_thread_info+thread_index)->s = -1;
				nslogit (func, NS002, "Cpool_assign", sstrerror(serrno));
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

main(argc, argv)
int argc;
char **argv;
{
#if ! defined(_WIN32)
	struct main_args main_args;

	if ((maxfds = Cinitdaemon ("nsdaemon", NULL)) < 0)
		exit (SYERR); 
	main_args.argc = argc;
	main_args.argv = argv;
	exit (Cns_main (&main_args));
#else
	if (Cinitservice ("cns", &Cns_main))
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
	struct Cns_srv_thread_info *thip = (struct Cns_srv_thread_info *) arg;

#ifdef CSEC
	Csec_server_reinitContext (&thip->sec_ctx, CSEC_SERVICE_TYPE_HOST, NULL);
	if (Csec_server_establishContext (&thip->sec_ctx, thip->s) < 0) {
		nslogit (func, "Could not establish security context: %s !\n",
		    Csec_getErrorMessage());
		sendrep (thip->s, CNS_RC, ESEC_NO_CONTEXT);
		thip->s = -1;
		return NULL;
	}
	Csec_server_getClientId (&thip->sec_ctx, &thip->Csec_mech, &thip->Csec_auth_id);
	if (strcmp (thip->Csec_mech, "ID") == 0 ||
	    Csec_isIdAService (thip->Csec_mech, thip->Csec_auth_id) >= 0) {
		if (isTrustedHost (thip->s, localhost, localdomain, CNS_SCE, "TRUST")) {
			if (Csec_server_getAuthorizationId (&thip->sec_ctx,
			    &thip->Csec_mech, &thip->Csec_auth_id) < 0) {
				thip->Csec_uid = 0;
				thip->Csec_gid = 0;
#ifndef VIRTUAL_ID
			} else if (Csec_mapToLocalUser (thip->Csec_mech, thip->Csec_auth_id,
			    NULL, 0, &thip->Csec_uid, &thip->Csec_gid) < 0) {
				nslogit (func, "Could not map to local user: %s !\n",
				    sstrerror (serrno));
				sendrep (thip->s, CNS_RC, serrno);
				thip->s = -1;
				return NULL;
#else
			} else {	/* mapping will be done later */
				thip->Csec_uid = (uid_t) -1;
				thip->Csec_gid = (gid_t) -1;
#endif
			}
		} else {
			nslogit (func, "Host is not trusted\n");
			sendrep (thip->s, CNS_RC, EACCES);
			thip->s = -1;
			return NULL;
		}
#ifndef VIRTUAL_ID
	} else if (Csec_mapToLocalUser (thip->Csec_mech, thip->Csec_auth_id,
	    NULL, 0, &thip->Csec_uid, &thip->Csec_gid) < 0) {
		nslogit (func, "Could not map to local user: %s !\n",
		    sstrerror (serrno));
		sendrep (thip->s, CNS_RC, serrno);
		thip->s = -1;
		return NULL;
#else
	} else {	/* mapping will be done later */
		thip->Csec_uid = (uid_t) -1;
		thip->Csec_gid = (gid_t) -1;
#endif
	}
#endif
	if ((c = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) == 0) {
		c = procreq (magic, req_type, req_data, clienthost, thip);
		sendrep (thip->s, CNS_RC, c);
	} else if (c > 0)
		sendrep (thip->s, CNS_RC, c);
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
	l = netread_timeout (s, req_hdr, sizeof(req_hdr), CNS_TIMEOUT);
	if (l == sizeof(req_hdr)) {
		rbp = req_hdr;
		unmarshall_LONG (rbp, n);
		*magic = n;
		unmarshall_LONG (rbp, n);
		*req_type = n;
		unmarshall_LONG (rbp, msglen);
		if (msglen > REQBUFSZ) {
			nslogit (func, NS046, REQBUFSZ);
			return (-1);
		}
		l = msglen - sizeof(req_hdr);
		n = netread_timeout (s, req_data, l, CNS_TIMEOUT);
		if (being_shutdown) {
			return (ENSNACT);
		}
		if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
			nslogit (func, NS002, "getpeername", neterror());
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
			nslogit (func, NS004, l);
		else if (l < 0)
			nslogit (func, NS002, "netread", neterror());
		return (SEINTERNAL);
	}
}

procdirreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
	struct Cns_class_metadata class_entry;
	DBLISTPTR dblistptr;
	int endlist = 0;
	struct Cns_file_metadata fmd_entry;
	struct Cns_symlinks lnk_entry;
	int new_req_type = -1;
	int rc = 0;
	fd_set readfd, readmask;
	struct Cns_file_replica rep_entry;
	struct Cns_seg_metadata smd_entry;
	DBLISTPTR smdlistptr;
	struct timeval timeval;
	struct Cns_user_metadata umd_entry;

	memset (&dblistptr, 0, sizeof(DBLISTPTR));
	if (req_type == CNS_OPENDIR) {
		memset (&smdlistptr, 0, sizeof(DBLISTPTR));
		if (c = Cns_srv_opendir (magic, req_data, clienthost, thip))
			return (c);
	} else if (req_type == CNS_LISTCLASS) {
		if (c = Cns_srv_listclass (magic, req_data, clienthost, thip,
		    &class_entry, endlist, &dblistptr))
			return (c);
	} else if (req_type == CNS_LISTLINKS) {
		if (c = Cns_srv_listlinks (magic, req_data, clienthost, thip,
		    &lnk_entry, endlist, &dblistptr))
			return (c);
	} else if (req_type == CNS_LISTREP4GC) {
		if (c = Cns_srv_listrep4gc (magic, req_data, clienthost, thip,
		    &rep_entry, endlist, &dblistptr))
			return (c);
	} else if (req_type == CNS_LISTREPLICA) {
		if (c = Cns_srv_listreplica (magic, req_data, clienthost, thip,
		    &fmd_entry, &rep_entry, endlist, &dblistptr))
			return (c);
	} else if (req_type == CNS_LISTREPLICAX) {
		if (c = Cns_srv_listreplicax (magic, req_data, clienthost, thip,
		    &rep_entry, endlist, &dblistptr))
			return (c);
	} else {
		if (c = Cns_srv_listtape (magic, req_data, clienthost, thip,
		    &fmd_entry, &smd_entry, endlist, &dblistptr))
			return (c);
	}
	sendrep (thip->s, CNS_IRC, 0);

	/* wait for readdir/listclass/listtape requests and process them */

	FD_ZERO (&readmask);
	FD_SET (thip->s, &readmask);
	while (1) {
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CNS_DIRTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0)
			endlist = 1;
		if (rc = getreq (thip->s, &magic, &new_req_type, req_data, &clienthost))
			endlist = 1;
		if (req_type == CNS_OPENDIR) {
			if (new_req_type != CNS_READDIR)
				endlist = 1;
			if (c = Cns_srv_readdir (magic, req_data, clienthost, thip,
			    &fmd_entry, &smd_entry, &umd_entry,
			    endlist, &dblistptr, &smdlistptr))
				return (c);
		} else if (req_type == CNS_LISTCLASS) {
			if (new_req_type != CNS_LISTCLASS)
				endlist = 1;
			if (c = Cns_srv_listclass (magic, req_data, clienthost, thip,
			    &class_entry, endlist, &dblistptr))
				return (c);
		} else if (req_type == CNS_LISTLINKS) {
			if (new_req_type != CNS_LISTLINKS)
				endlist = 1;
			if (c = Cns_srv_listlinks (magic, req_data, clienthost, thip,
			    &lnk_entry, endlist, &dblistptr))
				return (c);
		} else if (req_type == CNS_LISTREP4GC) {
			if (new_req_type != CNS_LISTREP4GC)
				endlist = 1;
			if (c = Cns_srv_listrep4gc (magic, req_data, clienthost, thip,
			    &rep_entry, endlist, &dblistptr))
				return (c);
		} else if (req_type == CNS_LISTREPLICA) {
			if (new_req_type != CNS_LISTREPLICA)
				endlist = 1;
			if (c = Cns_srv_listreplica (magic, req_data, clienthost, thip,
			    &fmd_entry, &rep_entry, endlist, &dblistptr))
				return (c);
		} else if (req_type == CNS_LISTREPLICAX) {
			if (new_req_type != CNS_LISTREPLICAX)
				endlist = 1;
			if (c = Cns_srv_listreplicax (magic, req_data, clienthost, thip,
			    &rep_entry, endlist, &dblistptr))
				return (c);
		} else {
			if (new_req_type != CNS_LISTTAPE)
				endlist = 1;
			if (c = Cns_srv_listtape (magic, req_data, clienthost, thip,
			    &fmd_entry, &smd_entry, endlist, &dblistptr))
				return (c);
		}
		if (endlist) break;
		sendrep (thip->s, CNS_IRC, 0);
	}
	return (rc);
}

procreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
	char **fqan = NULL;
	int nbfqans = 0;

	/* connect to the database if not done yet */

	if (! thip->db_open_done) {
		if (Cupv_seterrbuf (thip->errbuf, PRTBUFSZ)) {
			c = SEINTERNAL;
			sendrep (thip->s, MSG_ERR, "Cupv_seterrbuf error: %s\n",
			    sstrerror(serrno));
			return (c);
		}
		if (req_type != CNS_SHUTDOWN) {
			if (Cns_opendb (db_srvr, db_user, db_pwd, db_name,
			    &thip->dbfd) < 0) {
				c = serrno;
				sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
				return (c);
			}
			thip->db_open_done = 1;
		}
	}
#ifdef VIRTUAL_ID
	if (thip->Csec_uid == -1) {
#ifdef USE_VOMS
		fqan = Csec_server_get_client_fqans (&thip->sec_ctx, &nbfqans);
		if (nbfqans > 1) nbfqans = 1;
#endif
		if ((c = getidmap (&thip->dbfd, thip->Csec_auth_id, nbfqans, fqan,
		    &thip->Csec_uid, &thip->Csec_gid))) {
			sendrep (thip->s, MSG_ERR, "Could not get virtual id: %s !\n",
			    sstrerror (c));
			return (SENOMAPFND);
		}
	}
#endif
	switch (req_type) {
	case CNS_ACCESS:
		c = Cns_srv_access (magic, req_data, clienthost, thip);
		break;
	case CNS_CHCLASS:
		c = Cns_srv_chclass (magic, req_data, clienthost, thip);
		break;
	case CNS_CHDIR:
		c = Cns_srv_chdir (magic, req_data, clienthost, thip);
		break;
	case CNS_CHMOD:
		c = Cns_srv_chmod (magic, req_data, clienthost, thip);
		break;
	case CNS_CHOWN:
		c = Cns_srv_chown (magic, req_data, clienthost, thip);
		break;
	case CNS_CREAT:
		c = Cns_srv_creat (magic, req_data, clienthost, thip);
		break;
	case CNS_DELCLASS:
		c = Cns_srv_deleteclass (magic, req_data, clienthost, thip);
		break;
	case CNS_DELCOMMENT:
		c = Cns_srv_delcomment (magic, req_data, clienthost, thip);
		break;
	case CNS_DELETE:
		c = Cns_srv_delete (magic, req_data, clienthost, thip);
		break;
	case CNS_ENTCLASS:
		c = Cns_srv_enterclass (magic, req_data, clienthost, thip);
		break;
	case CNS_GETACL:
		c = Cns_srv_getacl (magic, req_data, clienthost, thip);
		break;
	case CNS_GETCOMMENT:
		c = Cns_srv_getcomment (magic, req_data, clienthost, thip);
		break;
	case CNS_GETPATH:
		c = Cns_srv_getpath (magic, req_data, clienthost, thip);
		break;
	case CNS_GETSEGAT:
		c = Cns_srv_getsegattrs (magic, req_data, clienthost, thip);
		break;
	case CNS_LCHOWN:
		c = Cns_srv_lchown (magic, req_data, clienthost, thip);
		break;
	case CNS_LISTCLASS:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_LISTTAPE:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_LSTAT:
		c = Cns_srv_lstat (magic, req_data, clienthost, thip);
		break;
	case CNS_MKDIR:
		c = Cns_srv_mkdir (magic, req_data, clienthost, thip);
		break;
	case CNS_MODCLASS:
		c = Cns_srv_modifyclass (magic, req_data, clienthost, thip);
		break;
	case CNS_OPEN:
		c = Cns_srv_open (magic, req_data, clienthost, thip);
		break;
	case CNS_OPENDIR:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_QRYCLASS:
		c = Cns_srv_queryclass (magic, req_data, clienthost, thip);
		break;
	case CNS_READLINK:
		c = Cns_srv_readlink (magic, req_data, clienthost, thip);
		break;
	case CNS_RENAME:
		c = Cns_srv_rename (magic, req_data, clienthost, thip);
		break;
	case CNS_RMDIR:
		c = Cns_srv_rmdir (magic, req_data, clienthost, thip);
		break;
	case CNS_SETACL:
		c = Cns_srv_setacl (magic, req_data, clienthost, thip);
		break;
	case CNS_SETATIME:
		c = Cns_srv_setatime (magic, req_data, clienthost, thip);
		break;
	case CNS_SETCOMMENT:
		c = Cns_srv_setcomment (magic, req_data, clienthost, thip);
		break;
	case CNS_SETFSIZE:
		c = Cns_srv_setfsize (magic, req_data, clienthost, thip);
		break;
	case CNS_SETSEGAT:
		c = Cns_srv_setsegattrs (magic, req_data, clienthost, thip);
		break;
	case CNS_SHUTDOWN:
		c = Cns_srv_shutdown (magic, req_data, clienthost, thip);
		break;
	case CNS_STAT:
		c = Cns_srv_stat (magic, req_data, clienthost, thip);
		break;
	case CNS_SYMLINK:
		c = Cns_srv_symlink (magic, req_data, clienthost, thip);
		break;
	case CNS_UNDELETE:
		c = Cns_srv_undelete (magic, req_data, clienthost, thip);
		break;
	case CNS_UNLINK:
		c = Cns_srv_unlink (magic, req_data, clienthost, thip);
		break;
	case CNS_UTIME:
		c = Cns_srv_utime (magic, req_data, clienthost, thip);
		break;
	case CNS_REPLACESEG:
		c = Cns_srv_replaceseg (magic, req_data, clienthost, thip);
		break;
	case CNS_REPLACETAPECOPY:
		c = Cns_srv_replacetapecopy (magic, req_data, clienthost, thip);
		break;
	case CNS_UPDATESEG_CHECKSUM:
		c = Cns_srv_updateseg_checksum (magic, req_data, clienthost, thip);
		break;
	case CNS_ADDREPLICA:
		c = Cns_srv_addreplica (magic, req_data, clienthost, thip);
		break;
	case CNS_DELREPLICA:
		c = Cns_srv_delreplica (magic, req_data, clienthost, thip);
		break;
	case CNS_LISTREPLICA:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_STARTTRANS:
		c = proctransreq (magic, req_data, clienthost, thip);
		break;
	case CNS_ENDTRANS:
		c = Cns_srv_endtrans (magic, req_data, clienthost, thip);
		break;
	case CNS_ABORTTRANS:
		c = Cns_srv_aborttrans (magic, req_data, clienthost, thip);
		break;
	case CNS_LISTLINKS:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_SETFSIZEG:
		c = Cns_srv_setfsizeg (magic, req_data, clienthost, thip);
		break;
	case CNS_STATG:
		c = Cns_srv_statg (magic, req_data, clienthost, thip);
		break;
	case CNS_STATR:
		c = Cns_srv_statr (magic, req_data, clienthost, thip);
		break;
	case CNS_SETPTIME:
		c = Cns_srv_setptime (magic, req_data, clienthost, thip);
		break;
	case CNS_SETRATIME:
		c = Cns_srv_setratime (magic, req_data, clienthost, thip);
		break;
	case CNS_SETRSTATUS:
		c = Cns_srv_setrstatus (magic, req_data, clienthost, thip);
		break;
	case CNS_ACCESSR:
		c = Cns_srv_accessr (magic, req_data, clienthost, thip);
		break;
	case CNS_LISTREP4GC:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_LISTREPLICAX:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_STARTSESS:
		c = procsessreq (magic, req_data, clienthost, thip);
		break;
	case CNS_ENDSESS:
		c = Cns_srv_endsess (magic, req_data, clienthost, thip);
		break;
	case CNS_DU:
		c = Cns_srv_du (magic, req_data, clienthost, thip);
		break;
	case CNS_GETGRPID:
		c = Cns_srv_getgrpbynam (magic, req_data, clienthost, thip);
		break;
	case CNS_GETGRPNAM:
		c = Cns_srv_getgrpbygid (magic, req_data, clienthost, thip);
		break;
	case CNS_GETIDMAP:
		c = Cns_srv_getidmap (magic, req_data, clienthost, thip);
		break;
	case CNS_GETUSRID:
		c = Cns_srv_getusrbynam (magic, req_data, clienthost, thip);
		break;
	case CNS_GETUSRNAM:
		c = Cns_srv_getusrbyuid (magic, req_data, clienthost, thip);
		break;
	case CNS_MODGRPMAP:
		c = Cns_srv_modgrpmap (magic, req_data, clienthost, thip);
		break;
	case CNS_MODUSRMAP:
		c = Cns_srv_modusrmap (magic, req_data, clienthost, thip);
		break;
	case CNS_RMGRPMAP:
		c = Cns_srv_rmgrpmap (magic, req_data, clienthost, thip);
		break;
	case CNS_RMUSRMAP:
		c = Cns_srv_rmusrmap (magic, req_data, clienthost, thip);
		break;
	case CNS_GETLINKS:
		c = Cns_srv_getlinks (magic, req_data, clienthost, thip);
		break;
	case CNS_GETREPLICA:
		c = Cns_srv_getreplica (magic, req_data, clienthost, thip);
		break;
	case CNS_ENTGRPMAP:
		c = Cns_srv_entergrpmap (magic, req_data, clienthost, thip);
		break;
	case CNS_ENTUSRMAP:
		c = Cns_srv_enterusrmap (magic, req_data, clienthost, thip);
		break;
	default:
		sendrep (thip->s, MSG_ERR, NS003, req_type);
		c = SEINTERNAL;
	}
	return (c);
}

procsessreq(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
	int req_type = -1;
	int rc = 0;
	fd_set readfd, readmask;
	struct timeval timeval;

	(void) Cns_srv_startsess (magic, req_data, clienthost, thip);
	sendrep (thip->s, CNS_IRC, 0);

	/* wait for requests and process them */

	FD_ZERO (&readmask);
	FD_SET (thip->s, &readmask);
	while (1) {
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CNS_TRANSTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
			return (SEINTERNAL);
		}
		if (rc = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) {
			return (rc);
		}
		rc = procreq (magic, req_type, req_data, clienthost, thip);
		if (req_type == CNS_ENDSESS) break;
		sendrep (thip->s, CNS_IRC, rc);
	}
	return (rc);
}

proctransreq(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
	int req_type = -1;
	int rc = 0;
	fd_set readfd, readmask;
	struct timeval timeval;

	(void) Cns_srv_starttrans (magic, req_data, clienthost, thip);
	sendrep (thip->s, CNS_IRC, 0);

	/* wait for requests and process them */

	FD_ZERO (&readmask);
	FD_SET (thip->s, &readmask);
	while (1) {
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CNS_TRANSTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
			(void) Cns_srv_aborttrans (magic, req_data, clienthost, thip);
			return (SEINTERNAL);
		}
		if (rc = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) {
			(void) Cns_srv_aborttrans (magic, req_data, clienthost, thip);
			return (rc);
		}
		rc = procreq (magic, req_type, req_data, clienthost, thip);
		if (req_type == CNS_ENDTRANS || req_type == CNS_ABORTTRANS) break;
		if (rc && req_type != CNS_ACCESS && req_type != CNS_ACCESSR &&
		    req_type != CNS_DU && req_type != CNS_GETACL &&
		    req_type != CNS_GETCOMMENT && req_type != CNS_GETLINKS &&
		    req_type != CNS_GETPATH && req_type != CNS_GETREPLICA &&
		    req_type != CNS_LSTAT && req_type != CNS_READLINK &&
		    req_type != CNS_STAT && req_type != CNS_STATG &&
		    req_type != CNS_STATR && req_type != CNS_GETGRPID &&
		    req_type != CNS_GETGRPNAM && req_type != CNS_GETUSRID &&
		    req_type != CNS_GETUSRNAM) break;
		sendrep (thip->s, CNS_IRC, rc);
	}
	return (rc);
}
