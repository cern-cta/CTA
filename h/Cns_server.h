/*
 * $Id: Cns_server.h,v 1.30 2009/06/30 15:04:59 waldron Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cns_server.h,v $ $Revision: 1.30 $ $Date: 2009/06/30 15:04:59 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CNS_SERVER_H
#define _CNS_SERVER_H
#include "Cns_struct.h"
#include "Csec_api.h"

                        /* name server constants and macros */

#define CHECKI           5    /* max interval to check for work to be done */
#define CNS_MAXNBTHREADS 100  /* maximum number of threads */
#define CNS_NBTHREADS    20

#define LOWER(s) \
  { \
  char * q; \
  for (q = s; *q; q++) \
    if (*q >= 'A' && *q <= 'Z') *q = *q + ('a' - 'A'); \
  }
#define RETURN(x) \
  { \
  struct timeval end; \
  if (thip->dbfd.tr_started) { \
    if (x) { \
      (void) Cns_abort_tr (&thip->dbfd); \
    } else if (! thip->dbfd.tr_mode) { \
      (void) Cns_end_tr (&thip->dbfd); \
    } \
  } \
  gettimeofday(&end, NULL); \
  nslogit (func, "returns %d - elapsed: %.3f\n", (x), \
                 (((((double)end.tv_sec * 1000) + \
                 ((double)end.tv_usec / 1000))) - thip->starttime) * 0.001); \
  return ((x)); \
  }
#define RETURNQ(x) \
  { \
  struct timeval end; \
  gettimeofday(&end, NULL); \
  nslogit (func, "returns %d - elapsed: %.3f\n", (x), \
                 (((((double)end.tv_sec * 1000) + \
                 ((double)end.tv_usec / 1000))) - thip->starttime) * 0.001); \
  return ((x)); \
  }
#define END_TRANSACTION \
  { \
  struct timeval end; \
  if (thip->dbfd.tr_started) { \
    (void) Cns_end_tr (&thip->dbfd); \
  } \
  gettimeofday(&end, NULL); \
  nslogit (func, "returns 0 - elapsed: %.3f\n", \
                 (((((double)end.tv_sec * 1000) + \
                 ((double)end.tv_usec / 1000))) - thip->starttime) * 0.001); \
  }
#define START_TRANSACTION \
  { \
  struct timeval start; \
  gettimeofday(&start, NULL); \
  (void) Cns_start_tr (thip->s, &thip->dbfd); \
  thip->starttime = ((double)start.tv_sec * 1000) + ((double)start.tv_usec / 1000); \
  }

                        /* name server tables and structures */

struct Cns_dbfd {
        int                idx;                /* index in array of Cns_dbfd */
        int                tr_mode;
        int                tr_started;
        int                connected;
};

typedef char Cns_dbrec_addr[19];
typedef int DBLISTPTR;

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
        short              fileclass;          /* 1 --> experiment, 2 --> user */
        char               status;             /* '-' --> online, 'm' --> migrated */
        char               csumtype[3];
        char               csumvalue[CA_MAXCKSUMLEN+1];
        char               acl[CA_MAXACLENTRIES*13];
};

struct Cns_srv_thread_info {
        int                s;                  /* socket for communication with client */
        struct Cns_dbfd dbfd;
        char               errbuf[PRTBUFSZ];
        Csec_context_t     sec_ctx;
        uid_t              Csec_uid;
        gid_t              Csec_gid;
        char               *Csec_mech;
        char               *Csec_auth_id;
        int                secure;             /* flag to indicate whether security is enabled */
        u_signed64         starttime;
};

struct Cns_seg_metadata {
        u_signed64         s_fileid;
        int                copyno;
        int                fsec;               /* file section number */
        u_signed64         segsize;            /* file section size */
        int                compression;        /* compression factor */
        char               s_status;           /* 'd' --> deleted */
        char               vid[CA_MAXVIDLEN+1];
        int                side;
        int                fseq;               /* file sequence number */
        unsigned char      blockid[4];         /* for positionning with locate command */
        char               checksum_name[CA_MAXCKSUMNAMELEN+1];
        unsigned long      checksum;
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
        char               comments[CA_MAXCOMMENTLEN+1];        /* user comments */
};

struct Cns_groupinfo {
        gid_t              gid;
        char               groupname[256];
};

struct Cns_userinfo {
        uid_t              userid;
        char               username[256];
};

                        /* name server function prototypes */

EXTERN_C int sendrep (int, int, ...);
EXTERN_C int nslogit (char *, char *, ...);

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
EXTERN_C int Cns_delete_group_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_lnk_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_smd_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_tppool_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_umd_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_delete_user_entry (struct Cns_dbfd *, Cns_dbrec_addr *);
EXTERN_C int Cns_end_tr (struct Cns_dbfd *);
EXTERN_C int Cns_get_class_by_id (struct Cns_dbfd *, int, struct Cns_class_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_class_by_name (struct Cns_dbfd *, char *, struct Cns_class_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_fullid (struct Cns_dbfd *, u_signed64, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_guid (struct Cns_dbfd *, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_fmd_by_pfid (struct Cns_dbfd *, int, u_signed64, struct Cns_file_metadata *, int, int, DBLISTPTR *);
EXTERN_C int Cns_get_grpinfo_by_gid (struct Cns_dbfd *, gid_t, struct Cns_groupinfo *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_grpinfo_by_name (struct Cns_dbfd *, char *, struct Cns_groupinfo *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_last_smd_by_vid (struct Cns_dbfd *, char *, int, struct Cns_seg_metadata *);
EXTERN_C int Cns_get_lnk_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_symlinks *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_max_copyno (struct Cns_dbfd *, u_signed64, int *);
EXTERN_C int Cns_get_smd_by_copyno (struct Cns_dbfd *, int, u_signed64, int,struct Cns_seg_metadata *, int, Cns_dbrec_addr *, int, DBLISTPTR *);
EXTERN_C int Cns_get_smd_by_fullid (struct Cns_dbfd *, u_signed64, int, int, struct Cns_seg_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_smd_by_pfid (struct Cns_dbfd *, int, u_signed64, struct Cns_seg_metadata *, int,  Cns_dbrec_addr *, int, DBLISTPTR *);
EXTERN_C int Cns_get_smd_by_vid (struct Cns_dbfd *, int, char *, int, struct Cns_seg_metadata *, int, DBLISTPTR *);
EXTERN_C int Cns_get_smd_enabled_copy_count_by_pfid (struct Cns_dbfd *, u_signed64, int *);
EXTERN_C int Cns_get_tapesum_by_vid (struct Cns_dbfd *, char *, u_signed64 *, u_signed64 *, u_signed64 *, u_signed64 *);
EXTERN_C int Cns_get_tppool_by_cid (struct Cns_dbfd *, int, int, struct Cns_tp_pool *, int,  Cns_dbrec_addr *, int, DBLISTPTR *);
EXTERN_C int Cns_get_umd_by_fileid (struct Cns_dbfd *, u_signed64, struct Cns_user_metadata *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_usrinfo_by_name (struct Cns_dbfd *, char *, struct Cns_userinfo *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_get_usrinfo_by_uid (struct Cns_dbfd *, uid_t, struct Cns_userinfo *, int, Cns_dbrec_addr *);
EXTERN_C int Cns_getpath_by_fileid (struct Cns_dbfd *, u_signed64, char **);
EXTERN_C int Cns_insert_class_entry (struct Cns_dbfd *, struct Cns_class_metadata *);
EXTERN_C int Cns_insert_fmd_entry (struct Cns_dbfd *, struct Cns_file_metadata *);
EXTERN_C int Cns_insert_group_entry (struct Cns_dbfd *, struct Cns_groupinfo *);
EXTERN_C int Cns_insert_lnk_entry (struct Cns_dbfd *, struct Cns_symlinks *);
EXTERN_C int Cns_insert_smd_entry (struct Cns_dbfd *, struct Cns_seg_metadata *);
EXTERN_C int Cns_insert_tppool_entry (struct Cns_dbfd *, struct Cns_tp_pool *);
EXTERN_C int Cns_insert_umd_entry (struct Cns_dbfd *, struct Cns_user_metadata *);
EXTERN_C int Cns_insert_user_entry (struct Cns_dbfd *, struct Cns_userinfo *);
EXTERN_C int Cns_list_class_entry (struct Cns_dbfd *, int, struct Cns_class_metadata *, int, DBLISTPTR *);
EXTERN_C int Cns_list_lnk_entry (struct Cns_dbfd *, int, char *, struct Cns_symlinks *, int, DBLISTPTR *);
EXTERN_C int Cns_opendb (struct Cns_dbfd *);
EXTERN_C int Cns_parsepath (struct Cns_dbfd *, u_signed64, char *, uid_t, gid_t, const char *, struct Cns_file_metadata *, Cns_dbrec_addr *, struct Cns_file_metadata *, Cns_dbrec_addr *, int);
EXTERN_C int Cns_start_tr (int, struct Cns_dbfd *);
EXTERN_C int Cns_unique_gid (struct Cns_dbfd *, unsigned int *);
EXTERN_C int Cns_unique_id (struct Cns_dbfd *, u_signed64 *);
EXTERN_C int Cns_unique_uid (struct Cns_dbfd *, unsigned int *);
EXTERN_C int Cns_update_class_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_class_metadata *);
EXTERN_C int Cns_update_fmd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_metadata *);
EXTERN_C int Cns_update_group_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_groupinfo *);
EXTERN_C int Cns_update_smd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_seg_metadata *);
EXTERN_C int Cns_update_umd_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_user_metadata *);
EXTERN_C int Cns_update_user_entry (struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_userinfo *);

EXTERN_C int Cns_srv_aborttrans (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_access (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_accessr (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_bulkexist (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_chclass (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_chdir (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_chmod (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_chown (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_creat (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_delcomment (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_delete (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_deleteclass (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_delsegbycopyno (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_dropsegs (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_du (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_endsess (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_endtrans (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_enterclass (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_entergrpmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_enterusrmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getacl (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getcomment (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getgrpbygid (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getgrpbynam (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getidmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getlinks (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getpath (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getsegattrs (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getusrbynam (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_getusrbyuid (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_lastfseq (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_lchown (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_lstat (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_mkdir (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_modgrpmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_modifyclass (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_modusrmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_opendir (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_ping (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_queryclass (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_readlink (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_rename (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_replaceseg (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_replacetapecopy (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_rmdir (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_rmgrpmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_rmusrmap (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setacl (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setatime (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setcomment (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setfsize (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setfsizecs (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setfsizeg (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_setsegattrs (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_shutdown (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_stat (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_statcs (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_statg (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_symlink (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_tapesum (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_undelete (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_unlink (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_unlinkbyvid (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_updatefile_checksum (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_updateseg_checksum (int, char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_updateseg_status (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_utime (char *, const char *, struct Cns_srv_thread_info *);

EXTERN_C int Cns_srv_listclass (char *, const char *, struct Cns_srv_thread_info *, struct Cns_class_metadata *, int, DBLISTPTR *);
EXTERN_C int Cns_srv_listlinks (char *, const char *, struct Cns_srv_thread_info *, struct Cns_symlinks *, int, DBLISTPTR *);
EXTERN_C int Cns_srv_listtape (int, char *, const char *, struct Cns_srv_thread_info *, struct Cns_file_metadata *, struct Cns_seg_metadata *, int, DBLISTPTR *);
EXTERN_C int Cns_srv_readdir (int, char *, const char *, struct Cns_srv_thread_info *, struct Cns_file_metadata *, struct Cns_seg_metadata *, struct Cns_user_metadata *, int, DBLISTPTR *, DBLISTPTR *, int *);
EXTERN_C int Cns_srv_startsess (char *, const char *, struct Cns_srv_thread_info *);
EXTERN_C int Cns_srv_starttrans (int, char *, const char *, struct Cns_srv_thread_info *);
#endif
