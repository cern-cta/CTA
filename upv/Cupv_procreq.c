/*
 * Copyright (C) 1999-2002 by CERN IT-DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: Cupv_procreq.c,v $ $Revision: 1.2 $ $Date: 2004/07/26 15:10:17 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */
 
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
#include <unistd.h>
#endif
#include "Cthread_api.h"
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"

#include "Cupv.h"
#include "Cupv_server.h"

extern int being_shutdown;
extern int always_reply_yes;

#define RESETID(UID,GID) resetid(&UID, &GID, thip);
 
void resetid(uid_t *u, gid_t *g, struct Cupv_srv_thread_info *thip) {
#ifdef CSEC
  if (thip->Csec_service_type < 0) {
    *u = thip->Csec_uid;
    *g = thip->Csec_gid;
  }
#endif
}

/*	Cupv_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */
void
Cupv_logreq(func, logbuf)
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
		Cupvlogit (func, CUP98, p);
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
	Cupvlogit (func, CUP98, p);
	if (p != logbuf) {
		*p = savechrs2[0];
		*(p + 1) = savechrs2[1];
	}
}


/*	Cupv_srv_delete - Deletes a privilege entry */

Cupv_srv_delete(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{

	char func[20];
	uid_t uid;
	gid_t gid;
	struct Cupv_userpriv priv;
	char *rbp;
	int len;
	Cupv_dbrec_addr rec_addr;

	strcpy (func, "Cupv_srv_delete");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	Cupvlogit (func, CUP92, "delete", uid, gid, clienthost);

	unmarshall_LONG (rbp, priv.uid);
	unmarshall_LONG (rbp, priv.gid);

	if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0) 
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
		RETURN (EINVAL);

	if (check_server_perm(uid, gid, clienthost, thip) != 0) {
	  RETURN (EPERM);
	}
	
	Cupvlogit(func, "Parameters: <%d> <%d> <%s> <%s>\n", priv.uid, priv.gid, 
		  priv.srchost, priv.tgthost);

	/* Adding the ^ and $ if needed */
	if (Cupv_check_regexp_syntax(priv.srchost) != 0 || Cupv_check_regexp_syntax(priv.tgthost) != 0) {
	  RETURN(EINVAL);
	}

	/* start transaction */

	(void) Cupv_start_tr (thip->s, &thip->dbfd);

	if(Cupv_get_privilege_entry(&thip->dbfd,
				    &priv,
				    1,
				    &rec_addr)) {
	  RETURN(serrno);
	}

	if (Cupv_delete_privilege_entry(&thip->dbfd, 
				  &rec_addr)) {
		RETURN (serrno);
	}
	
	RETURN (0);
}





/*	Cupv_srv_add - enter a new privilege entry */

Cupv_srv_add(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{

	char func[20];
	uid_t uid;
	gid_t gid;
	char *rbp;
	int len;
	struct Cupv_userpriv priv;

	strcpy (func, "Cupv_srv_add");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	Cupvlogit (func, CUP92, "add", uid, gid, clienthost);

	unmarshall_LONG (rbp, priv.uid);
	unmarshall_LONG (rbp, priv.gid);

	if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, priv.privcat);

	if (check_server_perm(uid, gid, clienthost, thip) != 0) {
	  RETURN (EPERM);
	}
	
	Cupvlogit(func, "Parameters: <%d> <%d> <%s> <%s>\n", priv.uid, priv.gid, 
		  priv.srchost, priv.tgthost);

	/* Adding the ^ and $ if needed */
	if (Cupv_check_regexp_syntax(priv.srchost) != 0 || Cupv_check_regexp_syntax(priv.tgthost) != 0) {
	  RETURN(EINVAL);
	}

	/* Checking the regular expressions */
	if ( Cupv_check_regexp(priv.srchost) != 0 ||  Cupv_check_regexp(priv.tgthost) != 0  ) {
	  RETURN (EINVAL);
	}

	/* start transaction */

	(void) Cupv_start_tr (thip->s, &thip->dbfd);

	if (Cupv_insert_privilege_entry(&thip->dbfd, 
					&priv)) {
		RETURN (serrno);
	}
	RETURN (0);
}

/*	Cupv_srv_add - enter a new privilege entry */

Cupv_srv_modify(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{

	char func[20];
	uid_t uid;
	gid_t gid;
	struct Cupv_userpriv priv;
	struct Cupv_userpriv newpriv;
	char *rbp;
	int len;
	Cupv_dbrec_addr rec_addr;

	/* Initializing the newpriv struct */
	newpriv.uid = newpriv.gid = newpriv.privcat = -1;
	newpriv.srchost[0] = newpriv.tgthost[0] = 0;

	strcpy (func, "Cupv_srv_modify");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	Cupvlogit (func, CUP92, "modify", uid, gid, clienthost);

	unmarshall_LONG (rbp, priv.uid);
	unmarshall_LONG (rbp, priv.gid);

	if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
		RETURN (EINVAL);
	
	unmarshall_STRINGN (rbp, newpriv.srchost, CA_MAXREGEXPLEN);
	unmarshall_STRINGN (rbp, newpriv.tgthost, CA_MAXREGEXPLEN);
	unmarshall_LONG (rbp, newpriv.privcat);

  	if (check_server_perm(uid, gid, clienthost, thip) != 0) { 
  	  RETURN (EPERM);
  	} 
	
	Cupvlogit(func, "Parameters: <%d> <%d> <%s> <%s> <%d> <%s> <%s>\n", 
		  priv.uid, priv.gid, priv.srchost, priv.tgthost, 
		  newpriv.privcat, newpriv.srchost, newpriv.tgthost);

	/* BEWARE
	   At this point, are filled:
	   priv.uid
	   priv.gid
	   priv.srchost
	   priv.tgthost

	   priv.privcat is empty, until filled by get privilege (hence the later check)

	   At least one of newpriv.srchost, newpriv.tgthost and newpriv.privcat is filled.
	   An error is sent if this is not the case */

	/* Adding the ^ and $ if needed */
	if (Cupv_check_regexp_syntax(priv.srchost) != 0 || Cupv_check_regexp_syntax(priv.tgthost) != 0) {
	  RETURN(EINVAL);
	}
	
	/* start transaction */
	
	(void) Cupv_start_tr (thip->s, &thip->dbfd);
	
	if (Cupv_get_privilege_entry (&thip->dbfd, &priv, 1, &rec_addr)) {
		RETURN (serrno);
	}


	/* Filling up newpriv with the right values for update... */

	newpriv.uid = priv.uid;
	newpriv.gid = priv.gid;
	
	if (strlen(newpriv.srchost) == 0) {
	  strcpy(newpriv.srchost, priv.srchost);
	}

	if (strlen(newpriv.tgthost) == 0) {
	  strcpy(newpriv.tgthost, priv.tgthost);
	}
	
	/* Adding the ^ and $ if needed */
	if (Cupv_check_regexp_syntax(newpriv.srchost) != 0 || Cupv_check_regexp_syntax(newpriv.tgthost) != 0) {
	  RETURN(EINVAL);
	}

	/* Checking the regular expressions */
	if ( Cupv_check_regexp(newpriv.srchost) != 0 ||  Cupv_check_regexp(newpriv.tgthost) != 0  ) {
	  RETURN (EINVAL);
	}


	if (newpriv.privcat < 0) {
	  newpriv.privcat = priv.privcat;
	}
	
	if (Cupv_update_privilege_entry (&thip->dbfd, &rec_addr, &newpriv)) {
		RETURN (serrno);
	}
	
	RETURN (0);

}


/*      Cupv_srv_list - list privileges */

Cupv_srv_list(magic, req_data, clienthost, thip, endlist, dblistptr)
     int magic; 
     char *req_data; 
     char *clienthost; 
     struct Cupv_srv_thread_info *thip; 
     int endlist;
     DBLISTPTR *dblistptr;
{ 
  int bol;	/* beginning of list flag */
  int c; 
  struct Cupv_userpriv priv_entry; 
  struct Cupv_userpriv filter;
 
  int eol = 0;  
  char func[19]; 
  gid_t gid; 
  int listentsz;
  int maxnbentries; 
  int nbentries = 0; 
  char outbuf[LISTBUFSZ+4]; 
  char *p; 
  char *rbp; 
  char *sbp; 
  uid_t uid;

  strcpy (func, "Cupv_srv_list"); 
  rbp = req_data; 
  unmarshall_LONG (rbp, uid); 
  unmarshall_LONG (rbp, gid); 

  RESETID(uid, gid);

  Cupvlogit (func, CUP92, "list", uid, gid, clienthost); 
  unmarshall_WORD (rbp, listentsz); 
  unmarshall_WORD (rbp, bol); 

  maxnbentries = LISTBUFSZ / listentsz; 
  sbp = outbuf; 
  marshall_WORD (sbp, nbentries);	       

  unmarshall_LONG(rbp, filter.uid);
  unmarshall_LONG(rbp, filter.gid);
  unmarshall_STRINGN(rbp, filter.srchost, CA_MAXREGEXPLEN);
  unmarshall_STRINGN(rbp, filter.tgthost, CA_MAXREGEXPLEN);
  unmarshall_LONG(rbp, filter.privcat);

  /* Adding the ^ and $ if needed */
  if (Cupv_check_regexp_syntax(filter.srchost) != 0 || Cupv_check_regexp_syntax(filter.tgthost) != 0) {
    RETURN(EINVAL);
  }

/*    Cupvlogit(func, "Filter: <%d><%d><%s><%s><%d>\n", filter.uid, filter.gid, filter.srchost, */
/*  	    filter.tgthost, filter.privcat); */

   while (nbentries < maxnbentries && 
	 (c = Cupv_list_privilege_entry (&thip->dbfd, bol, &priv_entry, &filter,
				   endlist, dblistptr)) == 0) { 

    marshall_LONG (sbp, priv_entry.uid); 
    marshall_LONG (sbp, priv_entry.gid); 
    marshall_STRING (sbp, priv_entry.srchost); 
    marshall_STRING (sbp, priv_entry.tgthost); 
    marshall_LONG (sbp, priv_entry.privcat); 
    nbentries++; 
    bol = 0; 
  } 
  if (c < 0) 
    RETURN (serrno); 
  if (c == 1) 
    eol = 1; 
  marshall_WORD (sbp, eol); 
  p = outbuf; 
  marshall_WORD (p, nbentries);		/* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf); 
  RETURN (0); 
} 


/*      Cupv_srv_check - Check privileges */

Cupv_srv_check(magic, req_data, clienthost, thip) 
     int magic; 
     char *req_data; 
     char *clienthost; 
     struct Cupv_srv_thread_info *thip; 
{ 
  int bol=1;	/* beginning of list flag */
  int priv_granted = 0; 
  int c; 
  struct Cupv_userpriv db_entry, requested; 

   char func[19]; 
  uid_t uid;
  gid_t gid; 
  int nbentries = 0; 
  char *p; 
  char *rbp; 

  strcpy (func, "Cupv_srv_check"); 
  rbp = req_data; 
  unmarshall_LONG (rbp, uid); 
  unmarshall_LONG (rbp, gid); 

  RESETID(uid, gid);

  Cupvlogit (func, CUP92, "check", uid, gid, clienthost); 

  /* Storing the values to check against the db */
  unmarshall_LONG(rbp, requested.uid);
  unmarshall_LONG(rbp, requested.gid);

  /* Setting the gid to 0 if uid to 0 */
  /* To avoid problems with SUN machines */
  if (requested.uid == 0) {
    Cupvlogit (func, "Setting GID to 0\n"); 
    requested.gid = 0;
  }

  unmarshall_STRINGN(rbp, requested.srchost, CA_MAXREGEXPLEN);
  unmarshall_STRINGN(rbp, requested.tgthost, CA_MAXREGEXPLEN);
  unmarshall_LONG(rbp, requested.privcat); 

  Cupvlogit(func, "<%d><%d><%s><%s><%d>\n", requested.uid, requested.gid, requested.srchost,
	    requested.tgthost, requested.privcat);

  if (always_reply_yes) {
    Cupvlogit(func, "ACCES GRANTED as in YES mode\n");
    RETURN(0);
  }

  /* Case SRC not specified, replace with clienthost */
  if (requested.srchost[0] == 0) {
    strcpy(requested.srchost, clienthost);
    Cupvlogit(func, "srchost = clienthost = %s\n", clienthost);
  }  

  /* Case TGT not specified, replace with clienthost */
  if (requested.tgthost[0] == 0) {
    strcpy(requested.tgthost, clienthost);
    Cupvlogit(func, "tgthost = clienthost = %s\n", clienthost);
  }

  c = Cupv_util_check(&requested, thip);

  if (c < 0) {
    RETURN (serrno); 
  } else if (c == 0) {
    RETURN(0);
  } else {
    RETURN (EACCES);
  }
} 



/*	Cupv_srv_shutdown - shutdown the volume manager */

Cupv_srv_shutdown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cupv_srv_thread_info *thip;
{
	int force = 0;
	char func[18];
	gid_t gid;
	char *rbp;
	uid_t uid;
	struct Cupv_userpriv requested;

	strcpy (func, "Cupv_srv_shutdown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	Cupvlogit (func, CUP92, "shutdown", uid, gid, clienthost);
	unmarshall_WORD (rbp, force);

	if (check_server_perm(uid, gid, clienthost, thip) != 0) {
	  RETURN (EPERM);
	}
	 
	being_shutdown = force + 1;
	RETURN (0);
}



/* Checks that a user has a right to place admin request for UPV */
int check_server_perm(int uid, int gid, char *clienthost, struct Cupv_srv_thread_info *thip) {
  
  struct Cupv_userpriv requested;
  char func[20];

  memset((void *)&requested, 0, sizeof(requested));
  strcpy(func, "Perm_Check");

  requested.privcat =  P_UPV_ADMIN;
  requested.uid = uid;
  requested.gid = gid;

 Cupvlogit(func , "Checking that %d,%d from %s has right %d\n", uid, gid, 
	   clienthost, requested.privcat);

  strcpy(requested.srchost, clienthost);

  if (gethostname(requested.tgthost, CA_MAXHOSTNAMELEN) == -1) {
    return(-1);
  }
  
  return (Cupv_util_check(&requested, thip));
}







