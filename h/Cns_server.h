/*
 * $Id: Cns_server.h,v 1.6 2006/01/26 15:32:41 bcouturi Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*
 * @(#)$RCSfile: Cns_server.h,v $ $Revision: 1.6 $ $Date: 2006/01/26 15:32:41 $ CERN IT-PDP/DM Jean-Philippe Baud
 */
 
#ifndef _CNS_SERVER_H
#define _CNS_SERVER_H
#ifdef USE_MYSQL
#include <mysql.h>
#endif
#include "Cns_struct.h"
#ifdef CSEC
#include "Csec_api.h"
#endif
 
                        /* name server constants and macros */

#define CHECKI	5	/* max interval to check for work to be done */
#define CNS_MAXNBTHREADS 100	/* maximum number of threads */
#define CNS_NBTHREADS	20
#define LOWER(s) \
	{ \
	char * q; \
	for (q = s; *q; q++) \
		if (*q >= 'A' && *q <= 'Z') *q = *q + ('a' - 'A'); \
	}
#define RETURN(x) \
	{ \
	nslogit (func, "returns %d\n", (x)); \
	if (thip->dbfd.tr_started) \
		if (x) \
			(void) Cns_abort_tr (&thip->dbfd); \
		else if (! thip->dbfd.tr_mode) \
			(void) Cns_end_tr (&thip->dbfd); \
	return ((x)); \
	}
#define RETURNQ(x) \
	{ \
	nslogit (func, "returns %d\n", (x)); \
	return ((x)); \
	}

			/* name server tables and structures */

struct Cns_dbfd {
	int		idx;		/* index in array of Cns_dbfd */
#ifdef USE_MYSQL
	MYSQL		mysql;
#endif
	int		tr_mode;
	int		tr_started;
};

#ifdef USE_ORACLE
typedef char Cns_dbrec_addr[19];
typedef int DBLISTPTR;
#else
#ifdef USE_MYSQL
typedef char Cns_dbrec_addr[21];
typedef MYSQL_RES *DBLISTPTR;
#endif
#endif

struct Cns_class_metadata {
	int 	classid;
	char	name[CA_MAXCLASNAMELEN+1];
	uid_t	uid;
	gid_t	gid;
	int	min_filesize;	/* in Mbytes */
	int	max_filesize;	/* in Mbytes */
	int	flags;
	int	maxdrives;
	int	max_segsize;	/* in Mbytes */
	int	migr_time_interval;
	int	mintime_beforemigr;
	int	nbcopies;
	int	nbdirs_using_class;
	int	retenp_on_disk;
};

struct Cns_file_metadata {
	u_signed64	fileid;
	u_signed64	parent_fileid;
	char		guid[CA_MAXGUIDLEN+1];
	char		name[CA_MAXNAMELEN+1];
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* '-' --> online, 'm' --> migrated */
	char		csumtype[3];
	char		csumvalue[33];
	char		acl[CA_MAXACLENTRIES*13];
};

struct Cns_file_replica {
	u_signed64	fileid;
	u_signed64	nbaccesses;
	time_t		atime;		/* last access to replica */
	time_t		ptime;		/* replica pin time */
	char		status;
	char		f_type;
	char		poolname[CA_MAXPOOLNAMELEN+1];
	char		host[CA_MAXHOSTNAMELEN+1];
	char		fs[80];
	char		sfn[CA_MAXSFNLEN+1];
};

struct Cns_srv_thread_info {
	int		s;		/* socket for communication with client */
	int		db_open_done;
	struct Cns_dbfd dbfd;
	char		errbuf[PRTBUFSZ];
#ifdef CSEC
	Csec_context_t	sec_ctx;
	uid_t		Csec_uid;
	gid_t		Csec_gid;
	char		*Csec_mech;
	char		*Csec_auth_id;
#endif
};

struct Cns_seg_metadata {
	u_signed64	s_fileid;
	int		copyno;
	int		fsec;		/* file section number */
	u_signed64	segsize;	/* file section size */
	int		compression;	/* compression factor */
	char		s_status;	/* 'd' --> deleted */
	char		vid[CA_MAXVIDLEN+1];
	int		side;
	int		fseq;		/* file sequence number */
	unsigned char	blockid[4];	/* for positionning with locate command */
	char		checksum_name[CA_MAXCKSUMNAMELEN+1];
	unsigned long	checksum;
};

struct Cns_symlinks {
	u_signed64	fileid;
	char		linkname[CA_MAXPATHLEN+1];
};

struct Cns_tp_pool {
	int	classid;
	char	tape_pool[CA_MAXPOOLNAMELEN+1];
};

struct Cns_user_metadata {
	u_signed64	u_fileid;
	char		comments[CA_MAXCOMMENTLEN+1];	/* user comments */
};

struct Cns_groupinfo {
	gid_t		gid;
	char		groupname[256];
};

struct Cns_userinfo {
	uid_t		userid;
	char		username[256];
};

			/* name server function prototypes */

EXTERN_C int Cns_abort_tr _PROTO((struct Cns_dbfd *));
EXTERN_C int Cns_acl_chmod _PROTO((struct Cns_file_metadata *));
EXTERN_C int Cns_acl_chown _PROTO((struct Cns_file_metadata *));
EXTERN_C int Cns_acl_compare _PROTO((const void *, const void *));
EXTERN_C int Cns_acl_inherit _PROTO((struct Cns_file_metadata *, struct Cns_file_metadata *, mode_t mode));
EXTERN_C int Cns_acl_validate _PROTO((struct Cns_acl *, int));
EXTERN_C int Cns_chkaclperm _PROTO((struct Cns_file_metadata *, int, uid_t, gid_t));
EXTERN_C int Cns_chkbackperm _PROTO((struct Cns_dbfd *, u_signed64, int, uid_t, gid_t, char *));
EXTERN_C int Cns_chkentryperm _PROTO((struct Cns_file_metadata *, int, uid_t, gid_t, char *));
EXTERN_C int Cns_closedb _PROTO((struct Cns_dbfd *));
EXTERN_C int Cns_delete_class_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_fmd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_lnk_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_rep_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_smd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_tppool_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_umd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_end_tr _PROTO((struct Cns_dbfd *));
EXTERN_C int Cns_get_class_by_id _PROTO((struct Cns_dbfd *, int, struct Cns_class_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_class_by_name _PROTO((struct Cns_dbfd *, char *, struct Cns_class_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_fileid _PROTO((struct Cns_dbfd *, u_signed64, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_fullid _PROTO((struct Cns_dbfd *, u_signed64, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_guid _PROTO((struct Cns_dbfd *, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_pfid _PROTO((struct Cns_dbfd *, int, u_signed64, struct Cns_file_metadata *, int, int, DBLISTPTR *));
EXTERN_C int Cns_get_lnk_by_fileid _PROTO((struct Cns_dbfd *, u_signed64, struct Cns_symlinks *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_max_copyno _PROTO((struct Cns_dbfd *, u_signed64, int *));
EXTERN_C int Cns_get_rep_by_sfn _PROTO((struct Cns_dbfd *, char *, struct Cns_file_replica *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_smd_by_fullid _PROTO((struct Cns_dbfd *, u_signed64, int, int, struct Cns_seg_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_smd_by_pfid _PROTO((struct Cns_dbfd *, int, u_signed64, struct Cns_seg_metadata *, int,  Cns_dbrec_addr *, int, DBLISTPTR *));
EXTERN_C int Cns_get_smd_by_vid _PROTO((struct Cns_dbfd *, int, char *, struct Cns_seg_metadata *, int, DBLISTPTR *));
EXTERN_C int Cns_get_tppool_by_cid _PROTO((struct Cns_dbfd *, int, int, struct Cns_tp_pool *, int,  Cns_dbrec_addr *, int, DBLISTPTR *));
EXTERN_C int Cns_get_umd_by_fileid _PROTO((struct Cns_dbfd *, u_signed64, struct Cns_user_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_insert_class_entry _PROTO((struct Cns_dbfd *, struct Cns_class_metadata *));
EXTERN_C int Cns_insert_fmd_entry _PROTO((struct Cns_dbfd *, struct Cns_file_metadata *));
EXTERN_C int Cns_insert_lnk_entry _PROTO((struct Cns_dbfd *, struct Cns_symlinks *));
EXTERN_C int Cns_insert_rep_entry _PROTO((struct Cns_dbfd *, struct Cns_file_replica *));
EXTERN_C int Cns_insert_smd_entry _PROTO((struct Cns_dbfd *, struct Cns_seg_metadata *));
EXTERN_C int Cns_insert_tppool_entry _PROTO((struct Cns_dbfd *, struct Cns_tp_pool *));
EXTERN_C int Cns_insert_umd_entry _PROTO((struct Cns_dbfd *, struct Cns_user_metadata *));
EXTERN_C int Cns_list_class_entry _PROTO((struct Cns_dbfd *, int, struct Cns_class_metadata *, int, DBLISTPTR *));
EXTERN_C int Cns_list_lnk_entry _PROTO((struct Cns_dbfd *, int, char *, struct Cns_symlinks *, int, DBLISTPTR *));
EXTERN_C int Cns_list_rep4admin _PROTO((struct Cns_dbfd *, int, char *, char *, char *, struct Cns_file_replica *, int, DBLISTPTR *));
EXTERN_C int Cns_list_rep4gc _PROTO((struct Cns_dbfd *, int, char *, struct Cns_file_replica *, int, DBLISTPTR *));
EXTERN_C int Cns_list_rep_entry _PROTO((struct Cns_dbfd *, int, u_signed64, struct Cns_file_replica *, int,  Cns_dbrec_addr *, int, DBLISTPTR *));
EXTERN_C int Cns_opendb _PROTO((char *, char *, char *, char *, struct Cns_dbfd *));
EXTERN_C int Cns_parsepath _PROTO((struct Cns_dbfd *, u_signed64, char *, uid_t, gid_t, char *, struct Cns_file_metadata *, Cns_dbrec_addr *, struct Cns_file_metadata *, Cns_dbrec_addr *, int));
EXTERN_C int Cns_start_tr _PROTO((int, struct Cns_dbfd *));
EXTERN_C int Cns_unique_id _PROTO((struct Cns_dbfd *, u_signed64 *));
EXTERN_C int Cns_update_class_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_class_metadata *));
EXTERN_C int Cns_update_fmd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_metadata *));
EXTERN_C int Cns_update_rep_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_replica *));
EXTERN_C int Cns_update_smd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_seg_metadata *));
EXTERN_C int Cns_update_umd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_user_metadata *));

EXTERN_C int Cns_delete_group_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_user_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_get_grpinfo_by_gid _PROTO((struct Cns_dbfd *, gid_t, struct Cns_groupinfo *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_grpinfo_by_name _PROTO((struct Cns_dbfd *, char *, struct Cns_groupinfo *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_usrinfo_by_name _PROTO((struct Cns_dbfd *, char *, struct Cns_userinfo *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_usrinfo_by_uid _PROTO((struct Cns_dbfd *, uid_t, struct Cns_userinfo *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_insert_group_entry _PROTO((struct Cns_dbfd *, struct Cns_groupinfo *));
EXTERN_C int Cns_insert_user_entry _PROTO((struct Cns_dbfd *, struct Cns_userinfo *));
EXTERN_C int Cns_unique_gid _PROTO((struct Cns_dbfd *, unsigned int *));
EXTERN_C int Cns_unique_uid _PROTO((struct Cns_dbfd *, unsigned int *));
EXTERN_C int Cns_update_group_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_groupinfo *));
EXTERN_C int Cns_update_user_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_userinfo *));
#endif
