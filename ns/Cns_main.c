/*
 * $Id: Cns_main.c,v 1.10 2005/06/22 14:32:40 jdurand Exp $
 *
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)Cns_main.c,v 1.35 2004/03/03 08:51:30 CERN IT-PDP/DM Jean-Philippe Baud";
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
#include "Cupv_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#ifdef CSEC
#include "Csec_api.h"
#endif

int num_THR = CNS_NBTHREADS;
int being_shutdown;
char db_pwd[33];
char db_srvr[33];
char db_user[33];
char func[16];
int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;
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
	char domainname[CA_MAXHOSTNAMELEN+1];
	struct sockaddr_in from;
	int fromlen = sizeof(from);
	char *getconfent();
	int i;
	int ipool;
	int on = 1;	/* for REUSEADDR */
	char nsconfigfile[CA_MAXPATHLEN+1];
	char *p;
	char *p_p, *p_s, *p_u;
	fd_set readfd, readmask;
	int rqfd;
	int s;
	struct sockaddr_in sin;
	struct servent *sp;
	int thread_index;
	struct timeval timeval;
	int errflg;

	jid = getpid();
	strcpy (func, "Cns_serv");
	nslogit (func, "started\n");
	gethostname (localhost, CA_MAXHOSTNAMELEN+1);

	errflg = 0;

	optind = 1;
	opterr = 0;

	while ((c = getopt (main_args->argc, main_args->argv, "t:")) != EOF) {
	  switch (c) {
	  case 't':
	    if ((num_THR = atoi(optarg)) <= 0) {
	      nslogit(func, "-t option value is <= 0\n");
	      errflg++;
	    }
	    break;
	  default:
	    errflg++;
	    break;
	  }
	}

	if (errflg) {
	  nslogit (func, "Parameter error\n");
	  exit (USERR);
	}

	nslogit(func, "initialize with %d threads\n", num_THR); 

	if (strchr (localhost, '.') == NULL) {
		if (Cdomainname (domainname, sizeof(domainname)) < 0) {
			nslogit (func, "Unable to get domainname\n");
			exit (SYERR);
		}
		strcat (localhost, ".");
		strcat (localhost, domainname);
	}

	/* get login info from the name server config file */
	if (strncmp (NSCONFIG, "%SystemRoot%\\", 13) == 0 &&
	    (p = getenv ("SystemRoot")))
		sprintf (nsconfigfile, "%s%s", p, strchr (NSCONFIG, '\\'));
	else
		strcpy (nsconfigfile, NSCONFIG);
	if ((cf = fopen (nsconfigfile, "r")) == NULL) {
		nslogit (func, NS023, nsconfigfile);
		return (CONFERR);
	}
	if (fgets (cfbuf, sizeof(cfbuf), cf) &&
	    strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n")) &&
	    (p_p = strtok (NULL, "@\n")) && (p_s = strtok (NULL, "\n"))) {
		strcpy (db_user, p_u);
		strcpy (db_pwd, p_p);
		strcpy (db_srvr, p_s);
	} else {
		nslogit (func, NS009, nsconfigfile, "incorrect");
		return (CONFERR);
	}
	(void) fclose (cf);

	(void) Cns_init_dbpkg ();

	/* create entry in the catalog for "/" if not already done */
	/* we use an extra SQL context in addition to num_THR */
	/* that's why dbfd.idx is num_THR and not num_THR-1 */

	memset (&dbfd, 0, sizeof(dbfd));
	dbfd.idx = num_THR;
	if (Cns_opendb (db_srvr, db_user, db_pwd, &dbfd) < 0)
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

	if ((ipool = Cpool_create (num_THR, NULL)) < 0) {
		nslogit (func, NS002, "Cpool_create", sstrerror(serrno));
		return (SYERR);
	}
	for (i = 0; i < num_THR; i++) {
		Cns_srv_thread_info[i].s = -1;
		Cns_srv_thread_info[i].dbfd.idx = i;
	}

	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

	/* open request socket */

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		nslogit (func, NS002, "socket", neterror());
		return (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	sin.sin_family = AF_INET ;
#ifdef CSEC
	if ((p = getenv ("SCNS_PORT")) || (p = getconfent ("SCNS", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("scns", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)SCNS_PORT);
	}
#else
	if ((p = getenv ("CNS_PORT")) || (p = getconfent ("CNS", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("cns", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)CNS_PORT);
	}
#endif
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
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
			for (i = 0; i < num_THR; i++) {
				if (Cns_srv_thread_info[i].s >= 0) {
					nb_active_threads++;
					continue;
				}
				if (Cns_srv_thread_info[i].db_open_done)
					(void) Cns_closedb (&Cns_srv_thread_info[i].dbfd);
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
			Cns_srv_thread_info[thread_index].s = rqfd;
			if (Cpool_assign (ipool, &doit,
			    &Cns_srv_thread_info[thread_index], 1) < 0) {
				Cns_srv_thread_info[thread_index].s = -1;
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

main()
{
#if ! defined(_WIN32)
  	if ((maxfds = Cinitdaemon ("nsdaemon", NULL)) < 0)  
  		exit (SYERR);   
	exit (Cns_main (NULL));
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
	Csec_server_reinitContext(&(thip->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
	if (Csec_server_establishContext(&(thip->sec_ctx),thip->s) < 0) {
	  nslogit(func, "Could not establish context: %s !\n", Csec_geterrmsg());
	  netclose (thip->s);
	  thip->s = -1;
	  return NULL;
	}
	/* Connection could be done from another castor service */
	if ((c = Csec_server_isClientAService(&(thip->sec_ctx))) >= 0) {
	  nslogit(func, "CSEC: Client is castor service type: %d\n", c);
	  thip->Csec_service_type = c;
	}
	else {
	  char *username;
	  if (Csec_server_mapClientToLocalUser(&(thip->sec_ctx),  &username, &(thip->Csec_uid), &(thip->Csec_gid)) ==0) {
	    nslogit(func, "CSEC: Client is %s (%d/%d)\n",
		    username,
		    thip->Csec_uid,
		    thip->Csec_gid);
	    thip->Csec_service_type = -1;
	  }
	  else {
	    nslogit(func, "CSEC: Can't get client username\n");
	    netclose (thip->s);
	    thip->s = -1;
	    return (NULL);
	  }
	}
		
#endif

	if ((c = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) == 0)
		procreq (magic, req_type, req_data, clienthost, thip);
	else if (c > 0)
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
			nslogit (func, NS002, "netread", strerror(errno));
		return (SEINTERNAL);
	}
}

procdirreq(magic, req_type, req_data, clienthost, thip)
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
	int new_req_type = -1;
	int rc = 0;
	fd_set readfd, readmask;
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
		} else {
			if (new_req_type != CNS_LISTTAPE)
				endlist = 1;
			if (c = Cns_srv_listtape (magic, req_data, clienthost, thip,
			    &fmd_entry, &smd_entry, endlist, &dblistptr))
				return (c);
		}
		if (endlist) break;
		sendrep (thip->s, CNS_IRC, 0);
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CNS_DIRTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0)
			endlist = 1;
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

	/* connect to the database if not done yet */

	if (! thip->db_open_done) {
		if (Cupv_seterrbuf (thip->errbuf, PRTBUFSZ)) {
			c = SEINTERNAL;
			sendrep (thip->s, MSG_ERR, "Cupv_seterrbuf error: %s\n",
			    sstrerror(serrno));
			sendrep (thip->s, CNS_RC, c);
			return;
		}
		if (req_type != CNS_SHUTDOWN) {
			if (Cns_opendb (db_srvr, db_user, db_pwd, &thip->dbfd) < 0) {
				c = serrno;
				sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
				sendrep (thip->s, CNS_RC, c);
				return;
			}
			thip->db_open_done = 1;
		}
	}
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
	case CNS_GETCOMMENT:
		c = Cns_srv_getcomment (magic, req_data, clienthost, thip);
		break;
	case CNS_GETPATH:
		c = Cns_srv_getpath (magic, req_data, clienthost, thip);
		break;
	case CNS_GETSEGAT:
		c = Cns_srv_getsegattrs (magic, req_data, clienthost, thip);
		break;
	case CNS_LISTCLASS:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
		break;
	case CNS_LISTTAPE:
		c = procdirreq (magic, req_type, req_data, clienthost, thip);
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
	case CNS_RENAME:
		c = Cns_srv_rename (magic, req_data, clienthost, thip);
		break;
	case CNS_RMDIR:
		c = Cns_srv_rmdir (magic, req_data, clienthost, thip);
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
	case CNS_UPDATESEG_CHECKSUM:
		c = Cns_srv_updateseg_checksum (magic, req_data, clienthost, thip);
		break;
	default:
		sendrep (thip->s, MSG_ERR, NS003, req_type);
		c = SEINTERNAL;
	}
	sendrep (thip->s, CNS_RC, c);
}
