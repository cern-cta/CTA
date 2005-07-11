/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlf_lib.c,v $ $Revision: 1.2 $ $Date: 2005/07/11 11:19:45 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"

int dlf_enterfacility(fac_name, fac_no)
const char *fac_name;
int fac_no;
{
	U_BYTE facno;
	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();

	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  return (-1);

	/* Build request header */

	facno = fac_no;
	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_ENTERFAC);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_BYTE (sbp, facno);
	marshall_STRING (sbp, fac_name);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_enterfacility", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_enterfacility", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}

int dlf_entertext(fac_name, msg_no, msg_txt)
const char *fac_name;
int msg_no;
const char *msg_txt;
{

	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();

	if ((dst = (dlf_log_dst_t*) dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_ENTERTXT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);
	marshall_SHORT (sbp, msg_no);
	marshall_STRING (sbp, msg_txt);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_entertext", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_entertext", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}

int dlf_modifyfacility(fac_name, facno)
const char *fac_name;
int facno;
{
	U_BYTE fac_no;
	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();

	fac_no = facno;

	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_MODFAC);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);
	marshall_BYTE (sbp, fac_no);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_modifyfacility", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_modifyfacility", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}

int dlf_modifytext(fac_name, msg_no, msg_txt)
const char *fac_name;
int msg_no;
const char *msg_txt;
{
	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();

	if ((dst = (dlf_log_dst_t*) dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  exit (SYERR);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_MODTXT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);
	marshall_SHORT (sbp, msg_no);
	marshall_STRING (sbp, msg_txt);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_modifytext", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_modifytext", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}


int dlf_deletefacility(fac_name)
const char *fac_name;
{
	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();


	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  exit (SYERR);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_DELFAC);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_deletefacility", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_deletefacility", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}

int dlf_deletetext(fac_name, txt_no)
const char *fac_name;
int txt_no;
{
	char* q;
	char* sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	gid_t gid;
	dlf_log_dst_t* dst;
	int socket;
	int msglen;
	char *reply;
	int  rep_size; 
	int rep_type;
	int rv;

	uid = geteuid();
	gid = getegid();

	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  exit (SYERR);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_DELTXT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);
	marshall_SHORT (sbp, txt_no);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  return (-1);
	/* Get completion status */
	while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
	  if (rep_type == MSG_ERR) {
	    free (reply);
	    continue;
	  }
	  else if (rep_type == DLF_RC) {
	    break;
	  }
	  else {
	    dlf_errmsg(NULL, DLF02, "dlf_deletetext", "Protocol error");
	    return (-1);
	  }
	}
	if (rv < 0) {
	  dlf_errmsg(NULL, DLF02, "dlf_deletetext", sstrerror(serrno));
	  return(-1);
	}
	return (0);
}

