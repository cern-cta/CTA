/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlf_procreq.c,v $ $Revision: 1.5 $ $Date: 2005/08/12 08:06:54 $ CERN IT-ADC/CA Vitaly Motyakov";
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
#endif
#include "Cthread_api.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"
#include "dlf.h"
#include "dlf_server.h"
#include "dlf_api.h"

extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];
 
/*	dlf_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */


void
dlf_logreq(func, logbuf)
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
		dlflogit (func, DLF98, p);
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
	dlflogit (func, DLF98, p);
	if (p != logbuf) {
		*p = savechrs2[0];
		*(p + 1) = savechrs2[1];
	}
}


/*     dlf_srv_getmsgtexts - get texts for given facility */

int dlf_srv_getmsgtexts(magic, req_data, clienthost, thip)
int magic;
char* req_data;
char* clienthost;
struct dlf_srv_thread_info* thip;
{

	char func[32];
	gid_t gid;
	uid_t uid;
	char* rbp;
	char* sbp;
	char outbuf[DLF_REPBUFSZ];
	char facname[DLF_MAXFACNAMELEN + 1];
	int msg_no;
	char msg_text[DLF_MAXSTRVALLEN + 1];
	int bol = 1;
	int status = 0;
	char* p;
	int c;
	int fac_no;
	char* buf_end;

	DBLISTPTR dblistptr;
	int endlist = 0;

	strcpy (func, "dlf_srv_getmsgtexts");
	memset (&dblistptr, 0, sizeof(DBLISTPTR));

	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);


	if (unmarshall_STRINGN (rbp, facname, DLF_MAXFACNAMELEN + 1) < 0)
	  RETURN (EINVAL);

	sbp = outbuf;
	marshall_BYTE (sbp, status);		/* will be updated (may be)*/

	/* Try to obtain facility number */
	c = dlf_get_facility_no (&thip->dbfd, facname, &fac_no, &dblistptr);
	if (c != 0) {
	  RETURN (serrno);
	}

	marshall_BYTE (sbp, fac_no);

	/* Try to not overload the buffer */
	buf_end = outbuf + sizeof(outbuf);

	while (status == 0) {
	    while ((buf_end - sbp) > (DLF_MAXSTRVALLEN + 3)) { /* Have we room to store message? */

		if ((c = dlf_get_text_entry (&thip->dbfd, bol, fac_no, &msg_no, msg_text,
					     endlist, &dblistptr)) == 0) {
		    marshall_SHORT (sbp, msg_no);
		    marshall_STRING (sbp, msg_text);
		    bol = 0;
		}
		else
		  break;
	    }
	    if (c < 0) {
		RETURN (serrno);
	    }
	    if (c == 1)
	      status = DLF_LAST_BUFFER;

	    p = outbuf;
	    marshall_BYTE (p, status);		/* status byte */
	    sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	    if (status == 0) { /* We have to send at least one more buffer */
		sbp = outbuf;
		marshall_BYTE (sbp, status);
		marshall_BYTE (sbp, fac_no);
	    }
	}
	RETURN (0);
}

int dlf_srv_getfacilites(magic, req_data, clienthost, thip)
int magic;
char* req_data;
char* clienthost;
struct dlf_srv_thread_info* thip;
{

	char func[32];
	gid_t gid;
	uid_t uid;
	char* rbp;
	char* sbp;
	char outbuf[DLF_REPBUFSZ];
	char fac_name[DLF_MAXFACNAMELEN + 1];
	int bol = 1;
	int status = 0;
	char* p;
	int c;
	int fac_no;
	char* buf_end;

	DBLISTPTR dblistptr;
	int endlist = 0;

	strcpy (func, "dlf_srv_getfacilities");
	memset (&dblistptr, 0, sizeof(DBLISTPTR));

	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	sbp = outbuf;
	marshall_BYTE (sbp, status);		/* will be updated (may be)*/

	/* Try to not overload the buffer */
	buf_end = outbuf + sizeof(outbuf);
	while (status == 0) {
	    while ((buf_end - sbp) > (DLF_MAXFACNAMELEN + 3)) { /* Have we room to store message? */
		if ((c = dlf_get_facility_entry (&thip->dbfd, bol, &fac_no, fac_name,
					     endlist, &dblistptr)) == 0) {
		    marshall_BYTE (sbp, fac_no);
		    marshall_STRING (sbp, fac_name);
		    bol = 0;
		}
		else
		  break;
	    }
	    if (c < 0) {
		RETURN (serrno);
	    }
	    if (c == 1)
	      status = DLF_LAST_BUFFER;

	    p = outbuf;
	    marshall_BYTE (p, status);		/* status byte */
	    sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	    if (status == 0) { /* We have to send at least one more buffer */
		sbp = outbuf;
		marshall_BYTE (sbp, status);
	    }
	}
	RETURN (0);
}

int dlf_srv_enterfacility(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
	char func[32];
	gid_t gid;
	uid_t uid;
	char *rbp;
	U_BYTE fac_no;
	char fac_name[DLF_MAXFACNAMELEN + 1];

	strcpy (func, "dlf_srv_enterfacility");
	rbp = req_data;

	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	unmarshall_BYTE (rbp, fac_no);
	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
	  RETURN (EINVAL);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

 	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_insert_facility_entry (&thip->dbfd, fac_no, fac_name)) {
	    RETURN (serrno);
	}

	(void) dlf_end_tr (&thip->dbfd);
	
	RETURN (0);

}

int dlf_srv_entertext (magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
	char func[32];
	gid_t gid;
	uid_t uid;
	char *rbp;
	U_SHORT msg_no;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	char msg_txt[DLF_MAXSTRVALLEN + 1];

	strcpy (func, "dlf_srv_entertext");
	rbp = req_data;

	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
	  RETURN (EINVAL);
	unmarshall_SHORT (rbp, msg_no);
	if (unmarshall_STRINGN (rbp, msg_txt, DLF_MAXSTRVALLEN + 1) < 0)
	  RETURN (EINVAL);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

 	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_insert_text_entry (&thip->dbfd, fac_name, msg_no, msg_txt)) {
	    RETURN (serrno);
	}

	(void) dlf_end_tr (&thip->dbfd);
	
	RETURN (0);

}

int dlf_srv_modfacility (magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
        U_BYTE fac_no; 
        uid_t uid;
	gid_t gid;
	char func[30];
	char* rbp;
	char fac_name[DLF_MAXFACNAMELEN + 1];

	strcpy (func, "dlf_srv_modfacility");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_BYTE(rbp, fac_no);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

	/* start transaction */

	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_modify_facility_entry (&thip->dbfd, fac_name, fac_no))
		RETURN (serrno);

	RETURN (0);
}

int dlf_srv_delfacility (magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
        uid_t uid;
	gid_t gid;
	char func[30];
	char* rbp;
	char fac_name[DLF_MAXFACNAMELEN + 1];

	strcpy (func, "dlf_srv_delfacility");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
		RETURN (EINVAL);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

	/* start transaction */

	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_delete_facility_entry (&thip->dbfd, fac_name))
		RETURN (serrno);

	RETURN (0);
}

int dlf_srv_modtext (magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
	char func[30];
	gid_t gid;
	uid_t uid;
	char *rbp;
	U_SHORT msg_no;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	char msg_txt[DLF_MAXSTRVALLEN + 1];

	strcpy (func, "dlf_srv_modtext");
	rbp = req_data;

	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
	  RETURN (EINVAL);
	unmarshall_SHORT (rbp, msg_no);
	if (unmarshall_STRINGN (rbp, msg_txt, DLF_MAXSTRVALLEN + 1) < 0)
	  RETURN (EINVAL);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

 	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_modify_text_entry (&thip->dbfd, fac_name, msg_no, msg_txt)) {
	    RETURN (serrno);
	}

	(void) dlf_end_tr (&thip->dbfd);
	
	RETURN (0);
}

int dlf_srv_deltext (magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
        uid_t uid;
	gid_t gid;
	char func[30];
	char* rbp;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	int txt_no;

	strcpy (func, "dlf_srv_deltext");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_SHORT(rbp, txt_no);

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

	/* start transaction */

	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_delete_text_entry (&thip->dbfd, fac_name, txt_no))
		RETURN (serrno);

	RETURN (0);


}

/*	dlf_srv_entermessage - enter a log message into the database */

int dlf_srv_entermessage(magic, req_data, req_data_length, clienthost, thip, last)
int magic;
char *req_data;
int req_data_length;
char *clienthost;
struct dlf_srv_thread_info *thip;
int *last;
{

	char func[32];
	gid_t gid;
	uid_t uid;
	char *rbp;
	char* end_of_buf;
	U_BYTE last_message;

	dlf_log_message_t log_message;
	dlf_msg_param_t* param;

	strcpy (func, "dlf_srv_entermessage");
	
	rbp = req_data;
	end_of_buf = rbp + req_data_length;

	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	unmarshall_BYTE (rbp, last_message);
	*last = last_message;

	memset ((char *) &log_message, 0, sizeof(dlf_log_message_t));

	if (unmarshall_STRINGN (rbp, log_message.time, DLF_TIMESTRLEN + 1) < 0)
	  RETURN (EINVAL);
	unmarshall_LONG (rbp, log_message.time_usec);
	if (unmarshall_STRINGN (rbp, log_message.hostname, CA_MAXHOSTNAMELEN + 1) < 0)
	  RETURN (EINVAL);
	unmarshall_UUID (rbp, log_message.request_id);
	unmarshall_LONG (rbp, log_message.pid);
	unmarshall_LONG (rbp, log_message.cid);

	if (unmarshall_STRINGN (rbp, log_message.ns_fileid.server, CA_MAXHOSTNAMELEN + 1) < 0)
	  RETURN (EINVAL);
	unmarshall_HYPER (rbp, log_message.ns_fileid.fileid);

	unmarshall_BYTE (rbp, log_message.facility_no);
	unmarshall_BYTE (rbp, log_message.severity);
	unmarshall_SHORT (rbp, log_message.message_no);

	while (rbp < end_of_buf) {
	    if ((param = new_dlf_param()) == NULL) RETURN (EINVAL);
	    unmarshall_BYTE (rbp, param->type);
	    if (unmarshall_STRINGN (rbp, param->name, DLF_MAXPARNAMELEN+1) < 0) {
	      dlf_delete_param_list (&log_message.param_list);
	      RETURN (EINVAL);
	    }
	    switch (param->type) {
	      case DLF_MSG_PARAM_TPVID:
	      case DLF_MSG_PARAM_STR:
		if (unmarshall_STRINGN (rbp, (char*)param->strval,DLF_MAXSTRVALLEN+1) < 0) {
		  dlf_delete_param_list (&log_message.param_list);
		  RETURN (EINVAL);
		}
		break;
	      case DLF_MSG_PARAM_INT64:
		unmarshall_HYPER (rbp, param->numval);
		break;
	      case DLF_MSG_PARAM_DOUBLE:
		if (unmarshall_STRINGN (rbp, (char*)param->strval,DLF_MAXSTRVALLEN+1) < 0) {
		  dlf_delete_param_list (&log_message.param_list);
		  RETURN (EINVAL);
		}
		param->dval = atof((char*)param->strval);
		break;
	      case DLF_MSG_PARAM_UUID:
		unmarshall_UUID (rbp, *((Cuuid_t*)param->strval));
		break;
	      default:
		dlf_delete_param_list (&log_message.param_list);
		RETURN (EINVAL);
	    }
	    dlf_add_to_param_list(&log_message.param_list, param);
	}

	/*	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/

	/* start transaction */

	(void) dlf_start_tr (thip->s, &thip->dbfd);

	if (dlf_insert_message_entry (&thip->dbfd, &log_message)) {
	    dlf_delete_param_list (&log_message.param_list);
	    RETURN (serrno);
	}

	(void) dlf_end_tr (&thip->dbfd);

	dlf_delete_param_list (&log_message.param_list);
	RETURN (0);
}

/*	dlf_srv_shutdown - shutdown the dlf server */

int dlf_srv_shutdown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct dlf_srv_thread_info *thip;
{
	int force = 0;
	char func[18];
	gid_t gid;
	char *rbp;
	uid_t uid;

	strcpy (func, "dlf_srv_shutdown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	dlflogit (func, DLF92, "shutdown", uid, gid, clienthost);
	unmarshall_WORD (rbp, force);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	being_shutdown = force + 1;
	RETURN (0);
}

