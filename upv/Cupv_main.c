/*
 * $Id: Cupv_main.c,v 1.4 2005/03/15 23:16:27 bcouturi Exp $
 *
 * Copyright (C) 1999-2002 by CERN IT-DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: Cupv_main.c,v $ $Revision: 1.4 $ $Date: 2005/03/15 23:16:27 $ CERN IT-DS/HSM Ben Couturier";
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
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "Cupv.h"
#include "Cupv_server.h"
#include "Cregexp.h"
#include "Cgetopt.h"
#ifdef CSEC
#include "Csec_api.h"
#endif

int being_shutdown;
int always_reply_yes = 0;

char db_pwd[33];
char db_srvr[33];
char db_user[33];
char func[16];
int jid;
int maxfds;
struct Cupv_srv_thread_info cupv_srv_thread_info[CUPV_NBTHREADS];

Cupv_main(main_args)
struct main_args *main_args;
{
	int c;
	FILE *fopen(), *cf;
	char cfbuf[80];
	struct Cupv_dbfd dbfd;
	void *doit(void *);
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
	char Cupvconfigfile[CA_MAXPATHLEN+1];

	jid = getpid();
	strcpy (func, "Cupv_serv");
	Cupvlogit (func, "Cupvdaemon V1.1 started\n");

	/* get login info from the volume manager config file */

	if (strncmp (CUPVCONFIG, "%SystemRoot%\\", 13) == 0 &&
	   (p = getenv ("SystemRoot")))
		sprintf (Cupvconfigfile, "%s%s", p, strchr (CUPVCONFIG, '\\'));
	else
		strcpy (Cupvconfigfile, CUPVCONFIG);
	if ((cf = fopen (Cupvconfigfile, "r")) == NULL) {
		Cupvlogit (func, CUP23, Cupvconfigfile);
		return (CONFERR);
	}
	if (fgets (cfbuf, sizeof(cfbuf), cf) &&
	    strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n")) &&
	    (p_p = strtok (NULL, "@\n")) && (p_s = strtok (NULL, "\n"))) {
		strcpy (db_user, p_u);
		strcpy (db_pwd, p_p);
		strcpy (db_srvr, p_s);
	} else {
		Cupvlogit (func, CUP09, Cupvconfigfile, "incorrect");
		return (CONFERR);
	}
	(void) fclose (cf);

	(void) Cupv_init_dbpkg ();

	/* create a pool of threads */

	if ((ipool = Cpool_create (CUPV_NBTHREADS, NULL)) < 0) {
		Cupvlogit (func, CUP02, "Cpool_create", sstrerror(serrno));
		return (SYERR);
	}
	for (i = 0; i < CUPV_NBTHREADS; i++) {
		cupv_srv_thread_info[i].s = -1;
		cupv_srv_thread_info[i].dbfd.idx = i;
	}

	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
    signal (SIGXFSZ, SIG_IGN);
#endif
    
	/* open request socket */

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		Cupvlogit (func, CUP02, "socket", neterror());
		return (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	sin.sin_family = AF_INET ;
#ifdef CSEC
	if ((p = getenv ("SCUPV_PORT")) || (p = getconfent ("SCUPV", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("sCupv", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)SCUPV_PORT);
	}
#else
	if ((p = getenv ("CUPV_PORT")) || (p = getconfent ("CUPV", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if (sp = getservbyname ("Cupv", "tcp")) {
		sin.sin_port = sp->s_port;
	} else {
		sin.sin_port = htons ((unsigned short)CUPV_PORT);
	}
#endif
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
		Cupvlogit (func, CUP02, "setsockopt", neterror());
	if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		Cupvlogit (func, CUP02, "bind", neterror());
		return (CONFERR);
	}
	listen (s, 5) ;

	FD_SET (s, &readmask);

		/* main loop */

	while (1) {
		if (being_shutdown) {
			int nb_active_threads = 0;
			for (i = 0; i < CUPV_NBTHREADS; i++) {
				if (cupv_srv_thread_info[i].s >= 0) {
					nb_active_threads++;
					continue;
				}
				if (cupv_srv_thread_info[i].db_open_done)
					(void) Cupv_closedb (&cupv_srv_thread_info[i].dbfd);
			}
			if (nb_active_threads == 0)
				return (0);
		}
		if (FD_ISSET (s, &readfd)) {
			FD_CLR (s, &readfd);
			rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
			if ((thread_index = Cpool_next_index (ipool)) < 0) {
				Cupvlogit (func, CUP02, "Cpool_next_index",
					sstrerror(serrno));
				if (serrno == SEWOULDBLOCK) {
					sendrep (rqfd, CUPV_RC, serrno);
					continue;
				} else
					return (SYERR);
			}
			cupv_srv_thread_info[thread_index].s = rqfd;
			if (Cpool_assign (ipool, &doit,
			    &cupv_srv_thread_info[thread_index], 1) < 0) {
				cupv_srv_thread_info[thread_index].s = -1;
				Cupvlogit (func, CUP02, "Cpool_assign", sstrerror(serrno));
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

int
main(argc, argv)
     int argc;
     char **argv;
{

  int c;
  int foreground = 0;
  always_reply_yes = 0;

  Coptind = 1;
  Copterr = 1;

  strcpy(func, "Cupv_serv");

  while ((c = Cgetopt(argc, argv, "fy")) != EOF) {
    switch(c) {
    case 'f':
      foreground = 1;
      Cupvlogit(func, "STARTUP - Foreground mode ON\n");
      break;
    case 'y':
      always_reply_yes = 1;
      Cupvlogit(func, "STARTUP - YES mode ON\n");
      break;
    case '?':
      printf("usage: Cupvdaemon [-fy]\n");
      exit(1);
    }
  }

  if (foreground) {
    Cupv_main();
  } else {
#if ! defined(_WIN32)
	if ((maxfds = Cinitdaemon ("Cupv_serv", NULL)) < 0)
		exit (SYERR);
	exit (Cupv_main (NULL));
#else
	if (Cinitservice ("Cupv", &Cupv_main))
		exit (SYERR);
#endif
  }
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
	struct Cupv_srv_thread_info *thip = (struct Cupv_srv_thread_info *) arg;
#ifdef CSEC
	Csec_server_reinitContext(&(thip->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
	if (Csec_server_establishContext(&(thip->sec_ctx),thip->s) < 0) {
	  Cupvlogit(func, "Could not establish context: %s !\n", Csec_geterrmsg());
	  netclose (thip->s);
	  thip->s = -1;
	  return (NULL);
	}
	/* Connection could be done from another castor service */
	if ((c = Csec_server_isClientAService(&(thip->sec_ctx))) >= 0) {
	  Cupvlogit(func, "CSEC: Client is castor service type: %d\n", c);
	  thip->Csec_service_type = c;
	}
	else {
	  char *username;
	  if (Csec_server_mapClientToLocalUser(&(thip->sec_ctx), &username, &(thip->Csec_uid), &(thip->Csec_gid)) == 0) {
	    Cupvlogit(func, "CSEC: Client is %s (%d/%d)\n",
		      username,
		      thip->Csec_uid,
		      thip->Csec_gid);
	    thip->Csec_service_type = -1;
	  }
	  else {
	    Cupvlogit(func, "CSEC: Can't get client username\n");
	    netclose (thip->s);
	    return (NULL);
	  }
	}
#endif

	if ((c = getreq (thip->s, &magic, &req_type, req_data, &clienthost)) == 0)
		procreq (magic, req_type, req_data, clienthost, thip);
	else if (c > 0)
		sendrep (thip->s, CUPV_RC, c);
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

	l = netread_timeout (s, req_hdr, sizeof(req_hdr), CUPV_TIMEOUT);
	if (l == sizeof(req_hdr)) {
		rbp = req_hdr;
		unmarshall_LONG (rbp, n);
		*magic = n;
		unmarshall_LONG (rbp, n);
		*req_type = n;
		unmarshall_LONG (rbp, msglen);
		if (msglen > REQBUFSZ) {
			Cupvlogit (func, CUP46, REQBUFSZ);
			return (-1);
		}
		l = msglen - sizeof(req_hdr);
		n = netread_timeout (s, req_data, l, CUPV_TIMEOUT);
		if (being_shutdown) {
			return (ECUPVNACT);
		}
		if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
			Cupvlogit (func, CUP02, "getpeername", neterror());
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
			Cupvlogit (func, CUP04, l);
		else if (l < 0)
			Cupvlogit (func, CUP02, "netread", strerror(errno));
		return (SEINTERNAL);
	}
}

proclistreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{
	int c;
	int new_req_type = -1;
	fd_set readfd, readmask;
	struct timeval timeval;
	DBLISTPTR dblistptr;
	int endlist = 0;

	/* wait for list requests and process them */

	FD_ZERO (&readmask);
	FD_SET (thip->s, &readmask);
	while (1) {

	        switch (req_type) {
		case CUPV_LIST:
		  if (c = Cupv_srv_list (magic, req_data, clienthost, thip, endlist, &dblistptr ))
		    return (c); 
		  break;
		}

		if(endlist) break;

		sendrep (thip->s, CUPV_IRC, 0);
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CUPV_LISTTIMEOUT;
		timeval.tv_usec = 0;
		if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
		  endlist = 1;
		  continue;
		}
		
		if (c = getreq (thip->s, &magic, &new_req_type, req_data, &clienthost) < 0) {
			endlist =1;
			continue;
		}
		
		if (new_req_type != req_type)
		  endlist = 1;
	}
	return (c);
}

procreq(magic, req_type, req_data, clienthost, thip)
int magic;
int req_type;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{
	int c;

	/* connect to the database if not done yet */

	if (! thip->db_open_done) {
		if (Cupv_opendb (db_srvr, db_user, db_pwd, &thip->dbfd) < 0) {
			c = serrno;
			sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
			sendrep (thip->s, CUPV_RC, c);
			return;
		}
		thip->db_open_done = 1;
	}

	switch (req_type) {

	case CUPV_ADD:
		c = Cupv_srv_add (magic, req_data, clienthost, thip);
		break;
	case CUPV_DELETE:
		c = Cupv_srv_delete (magic, req_data, clienthost, thip);
		break;
	case CUPV_MODIFY:
		c = Cupv_srv_modify (magic, req_data, clienthost, thip);
		break;
	case CUPV_CHECK:
	        c = Cupv_srv_check (magic, req_data, clienthost, thip);
		break;
	case CUPV_SHUTDOWN:
		c = Cupv_srv_shutdown (magic, req_data, clienthost, thip);
		break;
	case CUPV_LIST:
		c = proclistreq (magic, req_type, req_data, clienthost, thip);
		break;
	default:
		sendrep (thip->s, MSG_ERR, CUP03, req_type);
		c = SEINTERNAL;
	}
	sendrep (thip->s, CUPV_RC, c);
}

/* Checks that the machine is in the right domain */
int Cupv_check_domain(char *host) {

  char domain[MAXDOMAINLEN + 1];
  char *dotpos;
  int cmpres;

  dotpos = strchr(host, '.');

/*    Cupvlogit(func, "Checking domain in <%s>\n", host); */

  if (dotpos == NULL) {
    /* Not found, we are on the same machine */
/*      Cupvlogit("CheckDomain", "On same machine, domain check ok\n"); */
    return(0);
  } else {

    Cdomainname(domain, MAXDOMAINLEN);
/*      Cupvlogit("CheckDomain", "Comparing <%s> and <%s>\n", ((char *)dotpos) + 1, domain);  */
    cmpres = strcmp(((char *)dotpos) + 1, domain);

    if (cmpres == 0) {
      *dotpos = '\0';
      return(0);
    } else {
      return(-1);
    }
  }
}


/* Checks a regular expression */
int Cupv_check_regexp_syntax(char *tobechecked) {

  char tmp[CA_MAXREGEXPLEN + 1];
  int i=0;
  int beginok = 0, endok =0;

  if (strlen(tobechecked) == 0) {
    return(0);
  }

  if (tobechecked[0] == REGEXP_START_CHAR) {
    beginok = 1;
  }

  while(tobechecked[i] != 0 && i <= CA_MAXREGEXPLEN) {
    i++;
  }
  
  if (tobechecked[i-1] == REGEXP_END_CHAR) { 
    endok = 1;
  }

    Cupvlogit(func, "Check Syntax for <%s>:Beginning OK: %d, End OK:%d\n", tobechecked, beginok, endok);

  /* Checking that the buffer can hold the complete address */
  if (i + (!beginok) + (!endok) > CA_MAXREGEXPLEN) {
    serrno = EINVAL;
    return(-1);
  }

  if (!beginok) {
    tmp[0] = REGEXP_START_CHAR;
    tmp[1] = '\0';
    strcat(tmp, tobechecked);
  } else {
    strcpy(tmp, tobechecked);
  }

  if (!endok) {
    strcat(tmp, REGEXP_END_STR); 
  }

  strcpy(tobechecked, tmp);

  return(0);
}



/* Checks a regular expression */
int Cupv_check_regexp(char *tobechecked) {

  Cregexp_t *rex;

  if ((rex = Cregexp_comp(tobechecked)) == NULL) {
    return(-1);
  } else {
    free((char *)rex);
    return(0);
  }
}

/* Checks a request against a rule */
/* The rule Cupv_userpriv is the one considered as regular expressions */
int Cupv_compare_priv(struct Cupv_userpriv *requested, struct Cupv_userpriv *rule) {

  Cregexp_t *rex;
  Cregexp_t *rex2;

  /* Checking uid & gid */
  if (requested->uid != rule->uid || requested->gid != rule->gid) {
    return(-1);
  }

 /*   Cupvlogit("CMP", "uid / gid OK\n"); */

  /* Checking srchost */
  if ((rex = Cregexp_comp(rule->srchost)) == NULL) {
  /*    Cupvlogit("CMP", "Bad rule\n"); */
    return(-1);
  } else {
    if (Cregexp_exec(rex, requested->srchost) != 0) {
      /* Cupvlogit("CMP", "Do not match: <%s> <%s>\n", rule->srchost, requested->srchost); */
      free((char *)rex);
      return(-1);
    }
  }

  free((char *)rex);

 /*   Cupvlogit("CMP", "src host OK\n"); */

  /* Checking tgthost */
  if ((rex2 = Cregexp_comp(rule->tgthost)) == NULL) {
    return(-1);
  } else {
    if (Cregexp_exec(rex2, requested->tgthost) != 0) {
      free((char *)rex2);
      return(-1);
    }
  }

  free((char *)rex2);
  
/*    Cupvlogit("CMP", "tgt host OK\n"); */

  if ( (rule->privcat & requested->privcat) != requested->privcat) {
    return(-1);
  } else {
    return(0);
  }
}

/* Checks that a UPV admin action is authorized */
/* It returns 0 if authorization is granted,
   -1 if there was an error,
   1 if the authorization was not granted... */
int Cupv_util_check(struct Cupv_userpriv *requested, struct Cupv_srv_thread_info *thip) {

  int bol =  1;
  struct Cupv_userpriv db_entry;
  struct Cupv_userpriv filter;
  int c;
  DBLISTPTR dblistptr;

  memset (&dblistptr, 0, sizeof(DBLISTPTR));

  if (Cupv_check_domain(requested->srchost) != 0) {    
    Cupvlogit(func, "Access DENIED - SRC Domain is different\n");
    return(1);
  }
  
  if (Cupv_check_domain(requested->tgthost) != 0) {    
    Cupvlogit(func, "Access DENIED - TGT Domain is different\n");
    return(1);
  }

  /* Action is authorized is the user is root on local machine */
  if ( requested->uid == 0 
       && (strcmp(requested->srchost, requested->tgthost) == 0)) {
    Cupvlogit(func, "Access GRANTED - user is root on local machine\n");
    return(0);
  }  

  /* Initializing the db_entry structure with uid/gid, so that the Cupv_list_privilege
     functions returns all the rows for that uid/gid */
  filter.uid = requested->uid;
  filter.gid = requested->gid;
  filter.srchost[0] = 0;
  filter.tgthost[0] = 0;
  filter.privcat = -1;
  


  /* Looping on corresponding entries to check authorization */
  while ((c = Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, &filter, 0, &dblistptr )) == 0) { 
    
    if (c<0) {
      Cupvlogit(func, "Access DENIED - Problem accessing DB\n");
      return(-1);
    }

    /*  Cupvlogit(func, "Scanning row <%d> <%d> <%s> <%s> <%d>\n", db_entry.uid, db_entry.gid, db_entry.srchost, db_entry.tgthost, db_entry.privcat); */
      
    if (Cupv_compare_priv(requested, &db_entry) == 0) {
      Cupvlogit(func, "Access GRANTED - Authorization found in DB\n");
      
      /* Calling list_privilege_entry with endlist = 1 to free the resources*/ 
      Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, requested, 1, &dblistptr );
      
      return(0);
    }

    bol = 0; 
  }

  /* Nothing was found, return 1 */
  Cupvlogit(func, "Access DENIED - NO Authorization found in DB\n");
  
  /* Calling list_privilege_entry with endlist = 1 to free the resources*/ 
  Cupv_list_privilege_entry (&thip->dbfd, bol, &db_entry, requested, 1, &dblistptr );
  return(1);
}



