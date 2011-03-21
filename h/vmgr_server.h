/*
 * $Id: vmgr_server.h,v 1.6 2009/04/06 12:37:19 waldron Exp $
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _VMGR_SERVER_H
#define _VMGR_SERVER_H

#ifdef VMGRCSEC
#include "Csec_api.h"
#endif

#include "vmgr_struct.h"
#include "Cuuid.h"
#include "vmgr_constants.h"
#include "vmgr.h"

                        /* volume manager constants and macros */

#define CHECKI            5    /* max interval to check for work to be done */
#define VMGR_MAXNBTHREADS 100  /* maximum number of threads */
#define VMGR_NBTHREADS    6

#define RETURN(x)                                                       \
  {                                                                     \
    if (thip->dbfd.tr_started) {                                        \
      if (x) {                                                          \
        (void) vmgr_abort_tr (&thip->dbfd);                             \
      } else {                                                          \
        (void) vmgr_end_tr (&thip->dbfd);                               \
      }                                                                 \
    }                                                                   \
    vmgrlogreq(reqinfo, func, x);                                       \
    return ((x));                                                       \
  }

                        /* volume manager tables and structures */

struct vmgr_dbfd {
        int             idx;               /* index in array of vmgr_dbfd */
        int             tr_started;
        int             connected;
};

typedef char vmgr_dbrec_addr[19];
typedef int DBLISTPTR;

struct vmgr_srv_request_info {
        uid_t           uid;
        gid_t           gid;
        char            *username;
        char            *clienthost;
        char            reqid[CUUID_STRING_LEN + 1];
        char            logbuf[LOGBUFSZ];
        u_signed64      starttime;
};

struct vmgr_srv_thread_info {
        int             s;                 /* socket for communication with client */
        struct          vmgr_dbfd dbfd;
        char            errbuf[PRTBUFSZ];
#ifdef VMGRCSEC
        Csec_context_t  sec_ctx;
        uid_t           Csec_uid;
        gid_t           Csec_gid;
        int             Csec_service_type; /* Type of the service if client is another Castor server */
#endif
        struct          vmgr_srv_request_info reqinfo;
};

struct vmgr_tape_side_byte_u64 {
        char            vid[CA_MAXVIDLEN+1];
        int             side;
        char            poolname[CA_MAXPOOLNAMELEN+1];
        short           status;            /* TAPE_FULL, DISABLED, EXPORTED */
        u_signed64      estimated_free_space_byte_u64;
        int             nbfiles;
};

struct vmgr_tape_tag {
        char            vid[CA_MAXVIDLEN+1];
        char            text[CA_MAXTAGLEN+1];
};

                        /* volume manager function prototypes */

EXTERN_C int sendrep (int, int, ...);

EXTERN_C int openlog (const char *, const char *);
EXTERN_C int closelog (void);
EXTERN_C int vmgrlogit (const char *, ...);
EXTERN_C int vmgrlogreq (struct vmgr_srv_request_info *, const char *, const int);


EXTERN_C int vmgr_init_dbpkg();
EXTERN_C int vmgr_abort_tr (struct vmgr_dbfd *);
EXTERN_C int vmgr_closedb (struct vmgr_dbfd *);
EXTERN_C int vmgr_delete_denmap_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_dgnmap_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_library_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_model_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_pool_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_side_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_tag_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_delete_tape_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *);
EXTERN_C int vmgr_end_tr (struct vmgr_dbfd *);
EXTERN_C int vmgr_get_denmap_entry_byte_u64 (struct vmgr_dbfd *, char *, char *, char *, struct vmgr_tape_denmap_byte_u64 *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_dgnmap_entry (struct vmgr_dbfd *, char *, char *, struct vmgr_tape_dgnmap *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_library_entry (struct vmgr_dbfd *, char *, struct vmgr_tape_library *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_model_entry (struct vmgr_dbfd *, char *, struct vmgr_tape_media *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_pool_entry_byte_u64 (struct vmgr_dbfd *, char *, struct vmgr_tape_pool_byte_u64 *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_side_by_fullid_byte_u64 (struct vmgr_dbfd *, char *, int, struct vmgr_tape_side_byte_u64 *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_side_by_size_byte_u64 (struct vmgr_dbfd *, char *, u_signed64, struct vmgr_tape_side_byte_u64 *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_tag_by_vid (struct vmgr_dbfd *, char *, struct vmgr_tape_tag *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_get_tape_by_vid_byte_u64 (struct vmgr_dbfd *, char *, struct vmgr_tape_info_byte_u64 *, int, vmgr_dbrec_addr *);
EXTERN_C int vmgr_insert_denmap_entry_byte_u64 (struct vmgr_dbfd *, struct vmgr_tape_denmap_byte_u64 *);
EXTERN_C int vmgr_insert_dgnmap_entry (struct vmgr_dbfd *, struct vmgr_tape_dgnmap *);
EXTERN_C int vmgr_insert_library_entry (struct vmgr_dbfd *, struct vmgr_tape_library *);
EXTERN_C int vmgr_insert_model_entry (struct vmgr_dbfd *, struct vmgr_tape_media *);
EXTERN_C int vmgr_insert_pool_entry_byte_u64 (struct vmgr_dbfd *, struct vmgr_tape_pool_byte_u64 *);
EXTERN_C int vmgr_insert_side_entry_byte_u64 (struct vmgr_dbfd *, struct vmgr_tape_side_byte_u64 *);
EXTERN_C int vmgr_insert_tag_entry (struct vmgr_dbfd *, struct vmgr_tape_tag *);
EXTERN_C int vmgr_insert_tape_entry_byte_u64 (struct vmgr_dbfd *, struct vmgr_tape_info_byte_u64 *);
EXTERN_C int vmgr_list_denmap_entry_byte_u64 (struct vmgr_dbfd *, int, struct vmgr_tape_denmap_byte_u64 *, int);
EXTERN_C int vmgr_list_dgnmap_entry (struct vmgr_dbfd *, int, struct vmgr_tape_dgnmap *, int);
EXTERN_C int vmgr_list_library_entry (struct vmgr_dbfd *, int, struct vmgr_tape_library *, int);
EXTERN_C int vmgr_list_model_entry (struct vmgr_dbfd *, int, struct vmgr_tape_media *, int);
EXTERN_C int vmgr_list_pool_entry_byte_u64 (struct vmgr_dbfd *, int, struct vmgr_tape_pool_byte_u64 *, int);
EXTERN_C int vmgr_list_side_entry_byte_u64 (struct vmgr_dbfd *, int, char *, char *, struct vmgr_tape_side_byte_u64 *, int);
EXTERN_C int vmgr_opendb (struct vmgr_dbfd *);
EXTERN_C int vmgr_start_tr (int, struct vmgr_dbfd *);
EXTERN_C int vmgr_update_library_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_library *);
EXTERN_C int vmgr_update_model_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_media *);
EXTERN_C int vmgr_update_pool_entry_byte_u64 (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_pool_byte_u64 *);
EXTERN_C int vmgr_update_side_entry_byte_u64 (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_side_byte_u64 *);
EXTERN_C int vmgr_update_tag_entry (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_tag *);
EXTERN_C int vmgr_update_tape_entry_byte_u64 (struct vmgr_dbfd *, vmgr_dbrec_addr *, struct vmgr_tape_info_byte_u64 *);

EXTERN_C int vmgr_srv_deletedenmap (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deletedgnmap (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deletelibrary (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deletemodel (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deletepool (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deletetape (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_deltag (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_enterdenmap (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_enterdenmap_byte_u64 (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_enterdgnmap (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_enterlibrary (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_entermodel (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_enterpool (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_entertape (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_gettag (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_gettape (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_listdenmap (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listdenmap_byte_u64 (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listdgnmap (char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listlibrary (char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listmodel (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listpool (char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, const int);
EXTERN_C int vmgr_srv_listtape (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, struct vmgr_tape_info_byte_u64 *const, const int);
EXTERN_C int vmgr_srv_listtape_byte_u64 (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *, struct vmgr_tape_info_byte_u64 *const, const int);
EXTERN_C int vmgr_srv_modifylibrary (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_modifymodel (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_modifypool (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_modifytape (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_querypool (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_querylibrary (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_querymodel (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_querytape (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_querytape_byte_u64 (const int, char *const, struct vmgr_srv_thread_info *const, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_reclaim (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_settag (char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_shutdown (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_tpmounted (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
EXTERN_C int vmgr_srv_updatetape (int, char *, struct vmgr_srv_thread_info *, struct vmgr_srv_request_info *);
#endif
