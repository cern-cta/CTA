/*
 * $Id: Cns_server.h,v 1.2 2004/07/14 16:51:17 motiakov Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*
 * @(#)Cns_server.h,v 1.38 2004/03/03 08:50:31 CERN IT-PDP/DM Jean-Philippe Baud
 */
 
#ifndef _CNS_SERVER_H
#define _CNS_SERVER_H
#ifdef USE_ORACLE
#ifdef USE_OCI
#include <ocidfn.h>
#ifdef __STDC__
#include <ociapr.h>
#else
#include <ocikpr.h>
#endif
#if defined(__alpha) && defined(__osf__)
#define HDA_SIZE 512
#else
#define HDA_SIZE 256
#endif
#endif
#else
#ifdef USE_MYSQL
#include <mysql.h>
#else
#ifdef USE_RAIMA
#include <rds.h>
#else
#include "Cdb_api.h"
#endif
#endif
#endif
#ifdef CSEC
#include "Csec_api.h"
#endif 
                        /* name server constants and macros */

#define CHECKI	5	/* max interval to check for work to be done */
#define CNS_NBTHREADS	6
#define RETURN(x) \
	{ \
	nslogit (func, "returns %d\n", (x)); \
	if (thip->dbfd.tr_started) \
		if (x) \
			(void) Cns_abort_tr (&thip->dbfd); \
		else \
			(void) Cns_end_tr (&thip->dbfd); \
	return ((x)); \
	}

			/* name server tables and structures */

struct Cns_dbfd {
	int		idx;		/* index in array of Cns_dbfd */
#ifdef USE_ORACLE
#ifdef USE_OCI
	Lda_Def		lda;		/* logon data area */
	ub1		hda[HDA_SIZE];	/* host data area */
#endif
#else
#ifdef USE_MYSQL
	MYSQL		mysql;
#else
#ifdef USE_RAIMA
	RDM_SESS	hSess;		/* session handle */
	RDM_DB		hDb;		/* database handle */
#else
	Cdb_sess_t	Cdb_sess;
	Cdb_db_t	Cdb_db;
#endif
#endif
#endif
	int		tr_started;
};

#ifdef USE_ORACLE
typedef char Cns_dbrec_addr[19];
typedef int DBLISTPTR;
#else
#ifdef USE_MYSQL
typedef char Cns_dbrec_addr[21];
typedef MYSQL_RES *DBLISTPTR;
#else
#ifdef USE_RAIMA
typedef DB_ADDR Cns_dbrec_addr;
#else
typedef Cdb_off_t Cns_dbrec_addr;
typedef Cdb_off_t DBLISTPTR;
#endif
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
};

struct Cns_srv_thread_info {
	int		s;		/* socket for communication with client */
	int		db_open_done;
	struct Cns_dbfd dbfd;
	char		errbuf[PRTBUFSZ];
#ifdef CSEC
        Csec_context sec_ctx;
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

struct Cns_tp_pool {
	int	classid;
	char	tape_pool[CA_MAXPOOLNAMELEN+1];
};

struct Cns_user_metadata {
	u_signed64	u_fileid;
	char		comments[CA_MAXCOMMENTLEN+1];	/* user comments */
};

			/* name server function prototypes */

EXTERN_C int Cns_chkbackperm _PROTO((struct Cns_dbfd *, u_signed64, int, uid_t, gid_t, char *));
EXTERN_C int Cns_chkdirperm _PROTO((struct Cns_dbfd *, u_signed64, char *, int, uid_t, gid_t, char *, struct Cns_file_metadata *, Cns_dbrec_addr *));
EXTERN_C int Cns_chkentryperm _PROTO((struct Cns_file_metadata *, int, uid_t, gid_t, char *));
EXTERN_C int Cns_closedb _PROTO((struct Cns_dbfd *));
EXTERN_C int Cns_delete_class_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_fmd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_smd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_tppool_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_delete_umd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *));
EXTERN_C int Cns_get_class_by_id _PROTO((struct Cns_dbfd *, int, struct Cns_class_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_class_by_name _PROTO((struct Cns_dbfd *, char *, struct Cns_class_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_fileid _PROTO((struct Cns_dbfd *, u_signed64, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_fullid _PROTO((struct Cns_dbfd *, u_signed64, char *, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_fmd_by_pfid _PROTO((struct Cns_dbfd *, int, u_signed64, struct Cns_file_metadata *, int, int, DBLISTPTR *));
EXTERN_C int Cns_get_max_copyno _PROTO((struct Cns_dbfd *, u_signed64, int *));
EXTERN_C int Cns_get_smd_by_fullid _PROTO((struct Cns_dbfd *, u_signed64, int, int, struct Cns_seg_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_get_smd_by_pfid _PROTO((struct Cns_dbfd *, int, u_signed64, struct Cns_seg_metadata *, int,  Cns_dbrec_addr *, int, DBLISTPTR *));
EXTERN_C int Cns_get_smd_by_vid _PROTO((struct Cns_dbfd *, int, char *, struct Cns_seg_metadata *, int, DBLISTPTR *));
EXTERN_C int Cns_get_tppool_by_cid _PROTO((struct Cns_dbfd *, int, int, struct Cns_tp_pool *, int,  Cns_dbrec_addr *, int, DBLISTPTR *));
EXTERN_C int Cns_get_umd_by_fileid _PROTO((struct Cns_dbfd *, u_signed64, struct Cns_user_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_insert_class_entry _PROTO((struct Cns_dbfd *, struct Cns_class_metadata *));
EXTERN_C int Cns_insert_fmd_entry _PROTO((struct Cns_dbfd *, struct Cns_file_metadata *));
EXTERN_C int Cns_insert_smd_entry _PROTO((struct Cns_dbfd *, struct Cns_seg_metadata *));
EXTERN_C int Cns_insert_tppool_entry _PROTO((struct Cns_dbfd *, struct Cns_tp_pool *));
EXTERN_C int Cns_insert_umd_entry _PROTO((struct Cns_dbfd *, struct Cns_user_metadata *));
EXTERN_C int Cns_list_class_entry _PROTO((struct Cns_dbfd *, int, struct Cns_class_metadata *, int, DBLISTPTR *));
EXTERN_C int Cns_opendb _PROTO((char *, char *, char *, struct Cns_dbfd *));
EXTERN_C int Cns_read_fmd_entry _PROTO((struct Cns_dbfd *, struct Cns_file_metadata *, int, Cns_dbrec_addr *));
EXTERN_C int Cns_splitname _PROTO((u_signed64, char *, char *));
EXTERN_C int Cns_unique_id _PROTO((struct Cns_dbfd *, u_signed64 *));
EXTERN_C int Cns_update_class_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_class_metadata *));
EXTERN_C int Cns_update_fmd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_file_metadata *));
EXTERN_C int Cns_update_smd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_seg_metadata *));
EXTERN_C int Cns_update_umd_entry _PROTO((struct Cns_dbfd *, Cns_dbrec_addr *, struct Cns_user_metadata *));
#endif
