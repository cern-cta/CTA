/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CNS_SERVER_H
#define _CNS_SERVER_H

#include "Cns_struct.h"
#include "Cuuid.h"
#include "Csec_api.h"

/* name server constants and macros */

#define CHECKI           5    /* max interval to check for work to be done */
#define CNS_MAXNBTHREADS 100  /* maximum number of threads */
#define CNS_NBTHREADS    20

#define RETURN(x)                                                       \
  {                                                                     \
    if (thip->dbfd.tr_started) {                                        \
      if (x) {                                                          \
        (void) Cns_abort_tr (&thip->dbfd);                              \
      } else if (! thip->dbfd.tr_mode) {                                \
        (void) Cns_end_tr (&thip->dbfd);                                \
      }                                                                 \
    }                                                                   \
    nslogreq(reqinfo, func, x);                                         \
    return ((x));                                                       \
  }

#define RETURNQ(x)                                                      \
  {                                                                     \
    nslogreq(reqinfo, func, x);                                         \
    return ((x));                                                       \
  }

                        /* name server tables and structures */

struct Cns_dbfd {
        int                idx;                /* index in array of Cns_dbfd */
        int                tr_mode;
        int                tr_started;
        int                connected;
};

typedef char Cns_dbrec_addr[19];

struct Cns_class_metadata {
        int                classid;
        char               name[CA_MAXCLASNAMELEN+1];
        uid_t              uid;
        gid_t              gid;
        int                min_filesize;       /* in Mbytes */
        int                max_filesize;       /* in Mbytes */
        int                flags;
        int                maxdrives;
        int                max_segsize;        /* in Mbytes */
        int                migr_time_interval;
        int                mintime_beforemigr;
        int                nbcopies;
        int                retenp_on_disk;
};

struct Cns_file_metadata {
        u_signed64         fileid;
        u_signed64         parent_fileid;
        char               guid[CA_MAXGUIDLEN+1];
        char               name[CA_MAXNAMELEN+1];
        mode_t             filemode;
        int                nlink;              /* number of files in a directory */
        uid_t              uid;
        gid_t              gid;
        u_signed64         filesize;
        time_t             atime;              /* last access to file */
        time_t             mtime;              /* last file modification */
        time_t             ctime;              /* last metadata modification */
        double             stagertime;         /* last openx time from a stager */
        short              fileclass;
        char               status;             /* '-' --> online, 'm' --> migrated, 'D' --> deleted */
        char               csumtype[3];
        char               csumvalue[CA_MAXCKSUMLEN+1];
        char               acl[CA_MAXACLENTRIES*13];
};

struct Cns_srv_request_info {
        uid_t              uid;
        gid_t              gid;
        char               *username;
        char               *clienthost;
        char               requuid[CUUID_STRING_LEN + 1];
        char               logbuf[LOGBUFSZ];
        u_signed64         starttime;
        u_signed64         fileid;
        int                secure;             /* flag to indicate whether security is enabled */
};

struct Cns_srv_thread_info {
        int                s;                  /* socket for communication with client */
        struct             Cns_dbfd dbfd;
        char               errbuf[PRTBUFSZ];
        Csec_context_t     sec_ctx;
        uid_t              Csec_uid;
        gid_t              Csec_gid;
        char               *Csec_mech;
        char               *Csec_auth_id;
        struct             Cns_srv_request_info reqinfo;
};

struct Cns_seg_metadata {
        u_signed64         s_fileid;
        gid_t              gid;                /* group id, same as in Cns_file_metadata */
        int                copyno;
        int                fsec;               /* file section number */
        u_signed64         segsize;            /* file section size */
        int                compression;        /* compression factor */
        char               s_status;           /* '-' --> valid, 'D' --> deleted */
        char               vid[CA_MAXVIDLEN+1];
        int                side;
        int                fseq;               /* file sequence number */
        unsigned char      blockid[4];         /* for positionning with locate command */
        char               checksum_name[CA_MAXCKSUMNAMELEN+1];
        unsigned long      checksum;
        double             creationtime;
        double             lastmodificationtime;
};

struct Cns_symlinks {
        u_signed64         fileid;
        char               linkname[CA_MAXPATHLEN+1];
};

struct Cns_tp_pool {
        int                classid;
        char               tape_pool[CA_MAXPOOLNAMELEN+1];
};

struct Cns_user_metadata {
        u_signed64         u_fileid;
        char               comments[CA_MAXCOMMENTLEN+1];
};

                        /* name server function prototypes */

EXTERN_C int sendrep (int, int, ...);

EXTERN_C int openlog (const char *, const char *);
EXTERN_C int closelog (void);
EXTERN_C int nslogit (const char *, ...);
EXTERN_C int nslogreq (struct Cns_srv_request_info *, const char *, const int);

EXTERN_C int Cns_abort_tr (struct Cns_dbfd *);
EXTERN_C int Cns_acl_chmod (struct Cns_file_metadata *);
EXTERN_C int Cns_acl_chown (struct Cns_file_metadata *);
EXTERN_C int Cns_acl_compare (const void *, const void *);
EXTERN_C int Cns_acl_inherit (struct Cns_file_metadata *, struct Cns_file_metadata *, mode_t mode);
EXTERN_C int Cns_acl_validate (struct Cns_acl *, int);
EXTERN_C int Cns_check_files_exist (struct Cns_dbfd *, u_signed64 *, int*);
EXTERN_C int Cns_chkaclperm (struct Cns_file_metadata *, mode_t, uid_t, gid_t);
EXTERN_C int Cns_chkbackperm (struct Cns_dbfd *, u_signed64, uid_t, gid_t, const char *);
EXTERN_C int Cns_chkentryperm (struct Cns_file_metadata *, mode_t, uid_t, gid_t, const char *);
EXTERN_C int Cns_closedb (struct Cns_dbfd *);
EXTERN_C int Cns_count_long_ops (struct Cns_dbfd *, int *, int);
EXTERN_C int Cns_delete_class_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_fmd_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_lnk_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_smd_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_tppool_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_umd_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_end_tr (struct Cns_dbfd *);
EXTERN_C int Cns_get_class_by_id (struct Cns_dbfd *, int, struct Cns_class_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_class_by_name (struct Cns_dbfd *, char *, struct Cns_class_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_fullid (struct Cns_dbfd *, u_signed64, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_guid (struct Cns_dbfd *, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_pfid (struct Cns_dbfd *, int, u_signed64, struct Cns_file_metadata *, int, int);
EXTERN_C int Cns_get_last_smd_by_vid (struct Cns_dbfd *, char *, int, struct Cns_seg_metadata *);
EXTERN_C int Cns_get_lnk_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_symlinks *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_max_copyno (struct Cns_dbfd *, u_signed64, int *);
EXTERN_C int Cns_get_smd_by_fullid (struct Cns_dbfd *, u_signed64, int, int, struct Cns_seg_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_smd_by_pfid (struct Cns_dbfd *, int, u_signed64, struct Cns_seg_metadata *, int,  Cns_dbrec_addr *, int);
EXTERN_C int Cns_get_smd_by_vid (struct Cns_dbfd *, int, char *, int, struct Cns_seg_metadata *, int);
EXTERN_C int Cns_get_smd_copy_count_by_pfid (struct Cns_dbfd *, u_signed64, int *, int);
EXTERN_C int Cns_get_tapesum_by_vid (struct Cns_dbfd *, char *, u_signed64 *, u_signed64 *, u_signed64 *, u_signed64 *);
EXTERN_C int Cns_get_tppool_by_cid (struct Cns_dbfd *, int, int, struct Cns_tp_pool *, int,  Cns_dbrec_addr *, int);
EXTERN_C int Cns_get_umd_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_user_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_getpath_by_fileid (struct Cns_dbfd *, u_signed64, char **);
EXTERN_C int Cns_insert_class_entry (struct Cns_dbfd *, struct Cns_class_metadata *);
EXTERN_C int Cns_insert_fmd_entry (struct Cns_dbfd *, struct Cns_file_metadata *);
EXTERN_C int Cns_insert_fmd_entry_open (struct Cns_dbfd *, struct Cns_file_metadata *);
EXTERN_C int Cns_insert_lnk_entry (struct Cns_dbfd *, struct Cns_symlinks *);
EXTERN_C int Cns_insert_tppool_entry (struct Cns_dbfd *, struct Cns_tp_pool *);
EXTERN_C int Cns_insert_umd_entry (struct Cns_dbfd *, struct Cns_user_metadata *);
EXTERN_C int Cns_list_class_entry (struct Cns_dbfd *, int, struct Cns_class_metadata *, int);
EXTERN_C int Cns_list_lnk_entry (struct Cns_dbfd *, int, char *, struct Cns_symlinks *, int);
EXTERN_C int Cns_opendb (struct Cns_dbfd *);
EXTERN_C int Cns_parsepath (struct Cns_dbfd *, u_signed64, char *, uid_t, gid_t, const char *, struct Cns_file_metadata *, Cns_dbrec_addr *, struct Cns_file_metadata *, Cns_dbrec_addr *, int);
EXTERN_C int Cns_start_tr (struct Cns_dbfd *);
EXTERN_C int Cns_unique_id (struct Cns_dbfd *, u_signed64 *);
EXTERN_C int Cns_update_class_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_class_metadata *);
EXTERN_C int Cns_update_fmd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_metadata *);
EXTERN_C int Cns_update_fmd_entry_open (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_metadata *);
EXTERN_C int Cns_update_smd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_seg_metadata *);
EXTERN_C int Cns_update_umd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_user_metadata *);
EXTERN_C int Cns_is_concurrent_open (struct Cns_dbfd *, struct Cns_file_metadata *, double, char *);

EXTERN_C int Cns_srv_aborttrans (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_access (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_accessr (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_bulkexist (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_chclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_chdir (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_chmod (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_chown (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_closex (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_creat (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_delcomment (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_deleteclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_delsegbycopyno (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_dropsegs (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_du (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_endsess (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_endtrans (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_enterclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getacl (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getcomment (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getidmap (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getlinks (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getpath (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_getsegattrs (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_lastfseq (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_lchown (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_lstat (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_mkdir (int,char *,  struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_modifyclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_openx (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_opendir (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_ping (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_queryclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_readlink (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_rename (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_replaceseg (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_rmdir (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setacl (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setatime (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setcomment (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setfsize (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setfsizecs (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_setfsizeg (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_shutdown (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_stat (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_statcs (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_statg (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_symlink (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_tapesum (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_unlink (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_unlinkbyvid (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_updatefile_checksum (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_updateseg_checksum (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_updateseg_status (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_utime (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);

EXTERN_C int Cns_srv_listclass (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *, struct Cns_class_metadata *, int);
EXTERN_C int Cns_srv_listlinks (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *, struct Cns_symlinks *, int);
EXTERN_C int Cns_srv_listtape (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *, struct Cns_file_metadata *, struct Cns_seg_metadata *, int);
EXTERN_C int Cns_srv_readdir (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *, struct Cns_file_metadata *, struct Cns_seg_metadata *, struct Cns_user_metadata *, int, int *);
EXTERN_C int Cns_srv_startsess (char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
EXTERN_C int Cns_srv_starttrans (int, char *, struct Cns_srv_thread_info *, struct Cns_srv_request_info *);
#endif
