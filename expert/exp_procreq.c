/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: exp_procreq.c,v $ $Revision: 1.3 $ $Date: 2005/01/07 09:18:00 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <stdio.h>
#include <unistd.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "Cthread_api.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "serrno.h"
#include "expert.h"
#include "expert_daemon.h"

extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];
 
/*	exp_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */


void
exp_logreq(func, logbuf)
char *func;
char *logbuf;
{
	int n1, n2;
	char *p;
	char savechrs1[2];
	char savechrs2[2];

	n1 = LOGBUFSZ - strlen (func) - 36;
	n2 = strlen (logbuf);
	p = logbuf;
	while (n2 > n1) {
		savechrs1[0] = *(p + n1);
		savechrs1[1] = *(p + n1 + 1);
		*(p + n1) = '\\';
		*(p + n1 + 1) = '\0';
		explogit (func, EXP98, p);
		if (p != logbuf) {
			*p = savechrs2[0];
			*(p + 1) = savechrs2[1];
		}
		p += n1 - 2;
		savechrs2[0] = *p;
		savechrs2[1] = *(p + 1);
		*p = '+';

		*(p + 1) = ' ';
		*(p + 2) = savechrs1[0];
		*(p + 3) = savechrs1[1];
		n2 -= n1;
	}
	explogit (func, EXP98, p);
	if (p != logbuf) {
		*p = savechrs2[0];
		*(p + 1) = savechrs2[1];
	}
}


/*	exp_srv_execute - process request */

int exp_srv_execute(req_type, magic, req_data, clienthost, s)
int req_type;
int magic;
char *req_data;
char *clienthost;
int s;
{
	char func[18];
	gid_t gid;
	char *rbp;
	uid_t uid;
	pid_t c_pid;
	int fds[2];
	int n;
	char read_buff[80];

	char args[80][80];
	char *argvec[80];
	int i, c;
	char outbuf[100];
	char *sbp;
	int status;
	char wdir[CA_MAXPATHLEN + 1];

	strcpy (func, "exp_srv_execute");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	/* Read the configuration file */
	c = exp_get_from_conf (req_type, wdir, args);
	if (c < 0) {
	  explogit (func, EXP98, "error reading the configration file\n");
	  sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, serrno); /* Send error status */
	  RETURN (0);
	}

	explogit(func, EXP98, wdir);
	c = chdir (wdir); /* Change to working directory */
	if (c < 0) {
	  explogit (func, EXP98, "can't change directory\n");
	  sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, EEXPCDWDIR); /* Send error status */
	  RETURN (0);
	}

	for (i = 0; i < 80; i++) {
	  argvec[i] = args[i];
	  explogit(func, EXP98, argvec[i]);
	  if (strlen (args[i]) == 0) break;
	}
	argvec[i]=NULL;

	explogit (func, EXP92, "execute", uid, gid, clienthost);

	/******/

	if (dup2 (s, STDIN_FILENO) < 0) {
	  explogit (func, EXP98, "dup2 error\n");
	  sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, errno);
	  RETURN (0);
	}
	if (dup2 (s, STDOUT_FILENO) < 0) {
	  explogit (func, EXP98, "dup2 error\n");
	  sendrep (s, EXP_RP_STATUS, EXP_ST_ERROR, errno);
	  RETURN (0);
	}
	sendrep (s, EXP_RP_STATUS, EXP_ST_ACCEPTED, 0); /* Send status to client */	
	if (execv (argvec[0], argvec) < 0) {
	  explogit (func, EXP98, "execv error\n");
	  sendrep (s, EXP_RP_ERRSTRING);
	}

	/******/

	RETURN (0);
}

char *getenv();
char *getconfent();
char *exp_get_token();

int exp_get_from_conf (req_type, wdir, args)
int req_type;
char *wdir;
char *args;
{
  struct _exp_req {
    int req_type;
    char *req_name;
  };
  char *p;
  char filename[CA_MAXPATHLEN + 1];
  FILE *fp;
  char buf[BUFSIZ];
  struct _exp_req exp_requests[] = {
    { EXP_RQ_FILESYSTEM, "EXP_RQ_FILESYSTEM" },
    { EXP_RQ_MIGRATOR,"EXP_RQ_MIGRATOR" },
    { EXP_RQ_RECALLER,"EXP_RQ_RECALLER" },
    { EXP_RQ_GC,"EXP_RQ_GC" },
    { EXP_RQ_REPLICATION,"EXP_RQ_REPLICATION" },
    { EXP_EXECUTE, "EXP_EXECUTE" },
    { 0,                 "" } /* End of list */
  };
  int i;
  char *rq_name;
  char req_name[32];
  char del;
  char func[32];
  int err;
  int found;

  strcpy(func, "exp_get_from_conf");

  if ((p = getenv("EXPERT_CONFIG")) || (p = getconfent ("EXPERT", "CONFIG", 0))) {
    strncpy (filename, p, CA_MAXPATHLEN);
  }
  else {
#if defined (EXPERT_CONFIG)
    strncpy (filename, EXPERT_CONFIG, CA_MAXPATHLEN);
#else
    serrno = EEXPNOCONFIG;
    return (-1);
#endif
  }
  if ((fp = fopen(filename,"r")) == NULL) {
    serrno = EEXPNOCONFIG;
    return (-1);
  }
  /* Convert req_type to the string */
	
  for (i = 0; exp_requests[i].req_type != 0; i++) {
    if (req_type == exp_requests[i].req_type)
      break;
  }
  if (exp_requests[i].req_type == 0) { /* Not found in the table */
    fclose (fp);
    serrno = SEINTERNAL;
    return (-1);
  }
  rq_name = exp_requests[i].req_name;

  err = 0;
  found = 0;
  while (fgets (buf, sizeof(buf), fp) != NULL) {
    p = exp_get_token (buf, " \t", &del, req_name, sizeof(req_name));
    if (p == NULL || req_name[0] == '#') continue; /* End of line or comment */
    if (strcmp(req_name, rq_name) != 0) continue;
    /* Record found */
    found++;
    /* Try to get working directory */
    p = exp_get_token (p, " \t", &del, wdir, 256);
    if (p == NULL) {
      err++;    /* line format error */
      break;
    }
    /* Fill arglist */
    for (i=0; i < 80; i++) {
      p = exp_get_token (p, " \t", &del, args + i*80, 80);
      if (p == NULL) {
	*(args + i*80) = '\0';
	if (i <= 0) err++; /* line format error - there must be at least command name */
	break;
      }
    }
    if (found || err) break;
  }
  fclose (fp);
  if (err) {
    serrno = EEXPCONFERR;
    return (-1);
  }
  if (found)
    return (0);
  else {
    serrno = EEXPRQNOTFOUND;
    return (-1);
  }
}

char DLL_DECL * exp_get_token (ptr, delim, del, buf, buf_size)
const char *ptr;
const char *delim;
char *del;
char *buf;
int buf_size;
{
	char *p;
	char *p1;
	char *p2;
	char *buf_end;
	
	*del = '\0';
	buf_end = buf + buf_size;
	/* Skip spaces */
	for (p = (char*)ptr; *p == ' ' || *p == '\t'; p++);
	if (*p == '\0' || *p == '\n') 
		return (NULL); /* No more tokens */
	for (p1 = buf; *p != '\0' && *p != '\n' && p1 < buf_end; p++, p1++) {
		for (p2 = (char*)delim; *p2 != '\0' && *p != *p2; p2++);
		if (*p == *p2) {
			*del = *p2;
			break;
		}
		*p1 = *p;
	}
	if (p1 >= buf_end)
		return (NULL);
	/* Terminate string */
	*p1 = '\0';
	if (*p == '\0') 
		return (p);
	else
		return (++p); /* Pointer to the next character after the delimiter */
}
