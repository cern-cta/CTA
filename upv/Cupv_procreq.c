/*
 * Copyright (C) 1999-2002 by CERN IT-DS/HSM
 * All rights reserved
 */

#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>
#include <netinet/in.h>
#include <unistd.h>

#include "Cgrp.h"
#include "Cpwd.h"
#include "Cthread_api.h"
#include "Cupv.h"
#include "Cupv_server.h"
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"

void get_client_actual_id (struct Cupv_srv_thread_info *thip)
{
  struct passwd *pw;

#ifdef CUPVCSEC
  if (thip->Csec_service_type < 0) {
    thip->reqinfo.uid = thip->Csec_uid;
    thip->reqinfo.gid = thip->Csec_gid;
  }
#endif
  if ((pw = Cgetpwuid(thip->reqinfo.uid)) == NULL) {
    thip->reqinfo.username = "UNKNOWN";
  } else {
    thip->reqinfo.username = pw->pw_name;
  }
}

/* Checks that a user has a right to place admin request for UPV */
int check_server_perm(struct Cupv_srv_thread_info *thip)
{
  struct Cupv_userpriv requested;

  memset((void *)&requested, 0, sizeof(requested));

  requested.privcat = P_UPV_ADMIN;
  requested.uid = thip->reqinfo.uid;
  requested.gid = thip->reqinfo.gid;

  cupvlogit("MSG=\"Checking for UPV_ADMIN privilege\" REQID=%s Uid=%d Gid=%d "
            "FromHost=\"%s\"",
            thip->reqinfo.reqid, thip->reqinfo.uid, thip->reqinfo.gid,
            thip->reqinfo.clienthost);

  strcpy(requested.srchost, thip->reqinfo.clienthost);
  if (gethostname(requested.tgthost, CA_MAXHOSTNAMELEN) == -1) {
    return(-1);
  }

  return (Cupv_util_check(&requested, thip));
}

/*        Cupv_srv_delete - Deletes a privilege entry */

int Cupv_srv_delete(char *req_data,
                    struct Cupv_srv_thread_info *thip,
                    struct Cupv_srv_request_info *reqinfo)
{
  char *func = "delete";
  struct Cupv_userpriv priv;
  char *rbp;
  Cupv_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, priv.uid);
  unmarshall_LONG (rbp, priv.gid);
  if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "PrivUid=%d PrivGid=%d SourceHost=\"%s\" TargetHost=\"%s\"",
           priv.uid, priv.gid, priv.srchost, priv.tgthost);

  /* Check that the user is authorized to delete CUPV entries */
  if (check_server_perm(thip) != 0) {
    RETURN (EPERM);
  }

  /* Adding the ^ and $ if needed */
  if (Cupv_check_regexp_syntax(priv.srchost, reqinfo) != 0 ||
      Cupv_check_regexp_syntax(priv.tgthost, reqinfo) != 0) {
    RETURN(EINVAL);
  }

  /* Start transaction */
  (void) Cupv_start_tr (&thip->dbfd);

  if(Cupv_get_privilege_entry(&thip->dbfd, &priv, 1, &rec_addr)) {
    RETURN(serrno);
  }

  if (Cupv_delete_privilege_entry(&thip->dbfd, &rec_addr)) {
    RETURN (serrno);
  }

  RETURN (0);
}

/*        Cupv_srv_add - enter a new privilege entry */

int Cupv_srv_add(char *req_data,
                 struct Cupv_srv_thread_info *thip,
                 struct Cupv_srv_request_info *reqinfo)
{
  char *func = "add";
  char *rbp;
  struct Cupv_userpriv priv;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, priv.uid);
  unmarshall_LONG (rbp, priv.gid);

  if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);
  unmarshall_LONG (rbp, priv.privcat);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "PrivUid=%d PrivGid=%d SourceHost=\"%s\" TargetHost=\"%s\" "
           "Privilege=%d",
           priv.uid, priv.gid, priv.srchost, priv.tgthost, priv.privcat);

  /* Check that the user is authorized to add CUPV entries */
  if (check_server_perm(thip) != 0) {
    RETURN (EPERM);
  }

  /* Adding the ^ and $ if needed */
  if (Cupv_check_regexp_syntax(priv.srchost, reqinfo) != 0 ||
      Cupv_check_regexp_syntax(priv.tgthost, reqinfo) != 0) {
    RETURN(EINVAL);
  }

  /* Checking the regular expressions */
  if (Cupv_check_regexp(priv.srchost) != 0 ||
      Cupv_check_regexp(priv.tgthost) != 0) {
    RETURN (EINVAL);
  }

  /* Start transaction */
  (void) Cupv_start_tr (&thip->dbfd);

  if (Cupv_insert_privilege_entry(&thip->dbfd, &priv)) {
    RETURN (serrno);
  }
  RETURN (0);
}

/*        Cupv_srv_add - enter a new privilege entry */

int Cupv_srv_modify(char *req_data,
                    struct Cupv_srv_thread_info *thip,
                    struct Cupv_srv_request_info *reqinfo)
{
  char *func = "modify";
  struct Cupv_userpriv priv;
  struct Cupv_userpriv newpriv;
  char *rbp;
  Cupv_dbrec_addr rec_addr;

  /* Initializing the newpriv struct */
  newpriv.uid = newpriv.gid = newpriv.privcat = -1;
  newpriv.srchost[0] = newpriv.tgthost[0] = 0;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, priv.uid);
  unmarshall_LONG (rbp, priv.gid);

  if (unmarshall_STRINGN (rbp, priv.srchost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, priv.tgthost, CA_MAXREGEXPLEN) < 0)
    RETURN (EINVAL);

  unmarshall_STRINGN (rbp, newpriv.srchost, CA_MAXREGEXPLEN);
  unmarshall_STRINGN (rbp, newpriv.tgthost, CA_MAXREGEXPLEN);
  unmarshall_LONG (rbp, newpriv.privcat);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "PrivUid=%d PrivGid=%d SourceHost=\"%s\" TargetHost=\"%s\" "
           "NewPrivilege=%d NewSourceHost=\"%s\" NewTargetHost=\"%s\"",
           priv.uid, priv.gid, priv.srchost, priv.tgthost, newpriv.privcat,
           newpriv.srchost, newpriv.tgthost);

  /* Check that the user is authorized to modify CUPV entries */
  if (check_server_perm(thip) != 0) {
    RETURN (EPERM);
  }

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
  if (Cupv_check_regexp_syntax(priv.srchost, reqinfo) != 0 ||
      Cupv_check_regexp_syntax(priv.tgthost, reqinfo) != 0) {
    RETURN(EINVAL);
  }

  /* Start transaction */
  (void) Cupv_start_tr (&thip->dbfd);

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
  if (Cupv_check_regexp_syntax(newpriv.srchost, reqinfo) != 0 ||
      Cupv_check_regexp_syntax(newpriv.tgthost, reqinfo) != 0) {
    RETURN(EINVAL);
  }

  /* Checking the regular expressions */
  if (Cupv_check_regexp(newpriv.srchost) != 0 ||
      Cupv_check_regexp(newpriv.tgthost) != 0) {
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

int Cupv_srv_list(char *req_data,
                  struct Cupv_srv_thread_info *thip,
                  struct Cupv_srv_request_info *reqinfo,
                  int endlist)
{
  int bol;        /* beginning of list flag */
  int c;
  struct Cupv_userpriv priv_entry;
  struct Cupv_userpriv filter;

  int eol = 0;
  char *func = "list";
  int listentsz;
  int maxnbentries;
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p;
  char *rbp;
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
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
  if (Cupv_check_regexp_syntax(filter.srchost, reqinfo) != 0 ||
      Cupv_check_regexp_syntax(filter.tgthost, reqinfo) != 0) {
    RETURN(EINVAL);
  }

  while (nbentries < maxnbentries &&
         (c = Cupv_list_privilege_entry (&thip->dbfd, bol, &priv_entry, &filter,
                                         endlist)) == 0) {

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
  marshall_WORD (p, nbentries);  /* Update nbentries in reply */

  if (sendrep(thip->s, MSG_DATA, sbp - outbuf, outbuf) < 0) {
    RETURN (serrno);
  }
  RETURN (0);
}

/*      Cupv_srv_check - Check privileges */

int Cupv_srv_check(char *req_data,
                   struct Cupv_srv_thread_info *thip,
                   struct Cupv_srv_request_info *reqinfo)
{
  int c;
  struct Cupv_userpriv requested;
  char *func = "check";
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  /* Storing the values to check against the db */
  unmarshall_LONG(rbp, requested.uid);
  unmarshall_LONG(rbp, requested.gid);

  /* Setting the gid to 0 if uid to 0, this avoids problems with SUN machines */
  if (requested.uid == 0) {
    requested.gid = 0;
  }
  unmarshall_STRINGN(rbp, requested.srchost, CA_MAXREGEXPLEN);
  unmarshall_STRINGN(rbp, requested.tgthost, CA_MAXREGEXPLEN);
  unmarshall_LONG(rbp, requested.privcat);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "PrivUid=%d PrivGid=%d SourceHost=\"%s\" TargetHost=\"%s\" "
           "Pivilege=%d",
           requested.uid, requested.gid, requested.srchost, requested.tgthost,
           requested.privcat);

  /* Case SRC not specified, replace with clienthost */
  if (requested.srchost[0] == 0) {
    strcpy(requested.srchost, reqinfo->clienthost);
    cupvlogit("MSG=\"Source Host not specified, using client host\" REQID=%s "
              "ClientHost=\"%s\"",
              reqinfo->reqid, reqinfo->clienthost);
  }

  /* Case TGT not specified, replace with clienthost */
  if (requested.tgthost[0] == 0) {
    strcpy(requested.tgthost, reqinfo->clienthost);
    cupvlogit("MSG=\"Target Host not specified, using client host\" REQID=%s "
              "ClientHost=\"%s\"",
              reqinfo->reqid, reqinfo->clienthost);
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
