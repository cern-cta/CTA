/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlflistfacility.c,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:03:49 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"

main(argc, argv)
int argc;
char **argv;
{
	U_BYTE fac_no;
	dlf_msg_text_slist_t text_list;
	dlf_msg_text_t* p;
	char fac_name[DLF_MAXFACNAMELEN + 1];

	int c;
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	int status;
	int flags;
	char* reply;
	int rep_size;
	char* rep_buf_end;
	char* rbp;
	int rep_type;
	int socket;
	dlf_log_dst_t* dst;
	int end_rc;

#if defined(_WIN32)
	WSADATA wsadata;
#endif
 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, DLF52);
		exit (SYERR);
	}
#endif
	if (dlf_init("DLF-CONTROL") < 0 ) {
	    fprintf (stderr, "Error in initializing global structure.\n");
	    exit (SYERR);
	}

	uid = geteuid();
	gid = getegid();


	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_GETFAC);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */


	if ((dst = (dlf_log_dst_t*)dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  exit (SYERR);

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0) {
	    exit(SYERR);
	}

	fprintf(stdout, "List of facilities:\n");
	fprintf(stdout, "===================\n");

	/* Get data */
	end_rc = 0;
	do {
	    if (getrep (socket, &reply, &rep_size, &rep_type) < 0) {
	        dlf_errmsg(NULL, DLF02, "dlflistfacilities", sstrerror(serrno));
		exit(SYERR);
	    }
	    if (rep_type == MSG_DATA) {

	      rep_buf_end = reply + rep_size;
	      rbp = reply;
	      unmarshall_BYTE(rbp, status);

	      while(rbp < rep_buf_end) {
		unmarshall_BYTE(rbp, fac_no);
		if (unmarshall_STRINGN (rbp, fac_name, DLF_MAXFACNAMELEN + 1) < 0) {
		  dlf_errmsg(NULL, DLF02, "dlflistfacilities", sstrerror(EINVAL));
		  exit(SYERR);
		}
		fprintf(stdout, "%u\t%s\n", fac_no, fac_name);
	      }
	    free(reply);
	    }
	    else if (rep_type == MSG_ERR) {
	      /* Error was printed by getrep() */
	      free(reply);
	    }
	    else if (rep_type == DLF_RC) {
	      end_rc++;
	      break;
	    }
	}
	while (status == 0);
	/* Get completion status */
	if (!end_rc) {
	    if (getrep (socket, &reply, &rep_size, &rep_type) < 0) {
	        dlf_errmsg(NULL, DLF02, "dlflistfacility", sstrerror(serrno));
		exit (SYERR);
	    }
	    else if (rep_type != DLF_RC) {
	        dlf_errmsg(NULL, DLF02, "dlflistfacility", "Protocol error");
	        exit (SYERR);
	    }
	}

#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
