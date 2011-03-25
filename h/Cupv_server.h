/*
 * Cupv_server.h,v 1.5 2002/06/06 14:18:39 bcouturi Exp
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CUPV_SERVER_H
#define _CUPV_SERVER_H

#ifdef UPVCSEC
#include "Csec_api.h"
#endif

#include "Cupv_struct.h"
#include "Cuuid.h"
#include "Cupv_constants.h"
#include "Cupv_util.h"
#include "Cupv.h"

                        /* UPV constants and macros */

#define CHECKI            5    /* max interval to check for work to be done */
#define CUPV_MAXNBTHREADS 100  /* maximum number of threads */
#define CUPV_NBTHREADS    6

#define RETURN(x)                                                       \
  {                                                                     \
    if (thip->dbfd.tr_started) {                                        \
      if (x) {                                                          \
        (void) Cupv_abort_tr (&thip->dbfd);                             \
      } else {                                                          \
        (void) Cupv_end_tr (&thip->dbfd);                               \
      }                                                                 \
    }                                                                   \
    cupvlogreq(reqinfo, func, x);                                       \
    return ((x));                                                       \
  }

                        /* UPV tables and structures */

struct Cupv_dbfd {
        int             idx;                /* index in array of Cupv_dbfd */
        int             tr_started;
        int             connected;
};

typedef char Cupv_dbrec_addr[19];

struct Cupv_srv_request_info {
        uid_t           uid;
        gid_t           gid;
        char            *username;
        char            *clienthost;
        char            reqid[CUUID_STRING_LEN + 1];
        char            logbuf[LOGBUFSZ];
        u_signed64      starttime;
};

struct Cupv_srv_thread_info {
        int             s;                 /* socket for communication with client */
        struct          Cupv_dbfd dbfd;
#ifdef UPVCSEC
        Csec_context_t  sec_ctx;
        uid_t           Csec_uid;
        gid_t           Csec_gid;
        int             Csec_service_type; /* Type of the service if client is another Castor server */
#endif
        struct          Cupv_srv_request_info reqinfo;
};

                        /* UPV function prototypes */

EXTERN_C int sendrep (int, int, ...);

EXTERN_C int openlog (const char *, const char *);
EXTERN_C int closelog (void);
EXTERN_C int cupvlogit (const char *, ...);
EXTERN_C int cupvlogreq (struct Cupv_srv_request_info *, const char *, const int);

EXTERN_C int Cupv_util_check (struct Cupv_userpriv *, struct Cupv_srv_thread_info *thip);
EXTERN_C int Cupv_check_regexp (char *);
EXTERN_C int Cupv_check_regexp_syntax (char *, struct Cupv_srv_request_info *);
EXTERN_C int Cupv_compare_priv (struct Cupv_userpriv *, struct Cupv_userpriv *);
EXTERN_C int Cupv_opendb (struct Cupv_dbfd *);
EXTERN_C int Cupv_start_tr (struct Cupv_dbfd *);
EXTERN_C int Cupv_end_tr (struct Cupv_dbfd *);
EXTERN_C int Cupv_init_dbpkg ();
EXTERN_C int Cupv_abort_tr (struct Cupv_dbfd *);
EXTERN_C int Cupv_closedb  (struct Cupv_dbfd *);

EXTERN_C int Cupv_get_privilege_entry (struct Cupv_dbfd *, struct Cupv_userpriv *, int, Cupv_dbrec_addr *);
EXTERN_C int Cupv_delete_privilege_entry (struct Cupv_dbfd *, Cupv_dbrec_addr *);
EXTERN_C int Cupv_insert_privilege_entry (struct Cupv_dbfd *, struct Cupv_userpriv *);
EXTERN_C int Cupv_update_privilege_entry (struct Cupv_dbfd *, Cupv_dbrec_addr *, struct Cupv_userpriv *);
EXTERN_C int Cupv_list_privilege_entry  (struct Cupv_dbfd *, int, struct Cupv_userpriv *, struct Cupv_userpriv *, int);

EXTERN_C int Cupv_srv_list (char *, struct Cupv_srv_thread_info *, struct Cupv_srv_request_info *, int);
EXTERN_C int Cupv_srv_add (char *, struct Cupv_srv_thread_info *, struct Cupv_srv_request_info *);
EXTERN_C int Cupv_srv_delete (char *, struct Cupv_srv_thread_info *, struct Cupv_srv_request_info *);
EXTERN_C int Cupv_srv_modify (char *, struct Cupv_srv_thread_info *, struct Cupv_srv_request_info *);
EXTERN_C int Cupv_srv_check (char *, struct Cupv_srv_thread_info *, struct Cupv_srv_request_info *);
#endif









