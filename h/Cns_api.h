/*
 * $Id: Cns_api.h,v 1.26 2009/07/09 12:43:40 waldron Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cns_api.h,v $ $Revision: 1.26 $ $Date: 2009/07/09 12:43:40 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CNS_API_H
#define _CNS_API_H
#include <utime.h>
#include "Cns_constants.h"
#if defined(NSTYPE_LFC)
#include "lfc_api.h"
#elif defined(NSTYPE_DPNS)
#include "dpns_api.h"
#endif
#include "Cns_struct.h"
#include "osdep.h"

int *C__Cns_errno();
#define Cns_errno (*C__Cns_errno())

#define	CNS_LIST_BEGIN		0
#define	CNS_LIST_CONTINUE	1
#define	CNS_LIST_END		2

struct Cns_api_thread_info {
	u_signed64	cwd;		/* current HSM working directory */
	char *		errbufp;
	size_t		errbuflen;
	int		initialized;
	int		ns_errno;
	int		fd;
	mode_t		mask;		/* current HSM umask */
	char		server[CA_MAXHOSTNAMELEN+1];	/* current HSM Name Server */
	char            defserver[CA_MAXHOSTNAMELEN+1]; /* default HSM Name Server */
	uid_t		Csec_uid;
	gid_t		Csec_gid;
	char		Csec_mech[CA_MAXCSECPROTOLEN+1];
	char		Csec_auth_id[CA_MAXCSECNAMELEN+1];
        /* Authorization ID used by the API, otherwise it uses geteuid/getegid
	   In any case these uid/gid are only trusted if the clients has service credentials */
        int             use_authorization_id;
        uid_t           uid;
        gid_t           gid;

};

typedef struct {
	int		dd_fd;		/* socket for communication with server */
	u_signed64	fileid;
	int		bod;		/* beginning of directory */
	int		eod;		/* end of directory */
	int		dd_loc;		/* offset in buffer */
	int		dd_size;	/* amount of data in buffer */
	char		*dd_buf;	/* directory buffer */
	int		nbreplicas;
	char		*replicas;
} Cns_DIR;

struct Cns_direncomm {
	char		*comment;
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direnstat {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direnstatc {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	char		*comment;
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direntape {
	u_signed64	parent_fileid;
	u_signed64	fileid;
	int		copyno;
	int		fsec;		/* file section number */
	u_signed64	segsize;	/* file section size */
	int		compression;	/* compression factor */
	char		s_status;	/* 'd' --> deleted */
	char		vid[CA_MAXVIDLEN+1];
	char		checksum_name[CA_MAXCKSUMNAMELEN+1];
	unsigned long	checksum;
	int		side;
	int		fseq;		/* file sequence number */
	unsigned char	blockid[4];	/* for positionning with locate command */
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direnstatg {
	u_signed64	fileid;
	char		guid[CA_MAXGUIDLEN+1];
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	char		csumtype[3];
	char		csumvalue[CA_MAXCKSUMLEN+1];
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_fileclass {
	int		classid;
	char		name[CA_MAXCLASNAMELEN+1];
	uid_t		uid;
	gid_t		gid;
	int		min_filesize;	/* in Mbytes */
	int		max_filesize;	/* in Mbytes */
	int		flags;
	int		maxdrives;
	int		max_segsize;	/* in Mbytes */
	int		migr_time_interval;
	int		mintime_beforemigr;
	int		nbcopies;
	int		retenp_on_disk;
	int		nbtppools;
	char		*tppools;
};

struct Cns_fileid {
	char		server[CA_MAXHOSTNAMELEN+1];
	u_signed64	fileid;
};

struct Cns_filestat {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
};

struct Cns_filestatg {
	u_signed64	fileid;
	char		guid[CA_MAXGUIDLEN+1];
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	char		csumtype[3];
	char		csumvalue[CA_MAXCKSUMLEN+1];
};

struct Cns_filestatcs {
        u_signed64      fileid;
        mode_t          filemode;
        int             nlink;          /* number of files in a directory */
        uid_t           uid;
        gid_t           gid;
        u_signed64      filesize;
        time_t          atime;          /* last access to file */
        time_t          mtime;          /* last file modification */
        time_t          ctime;          /* last metadata modification */
        short           fileclass;      /* 1 --> experiment, 2 --> user */
        char            status;         /* ' ' --> online, 'm' --> migrated */
        char            csumtype[3];
        char            csumvalue[CA_MAXCKSUMLEN+1];
};

struct Cns_linkinfo {
	char		path[CA_MAXPATHLEN+1];
};

typedef struct {
	int		fd;		/* socket for communication with server */
	int		eol;		/* end of list */
	int		offset;		/* offset in buffer */
	int		len;		/* amount of data in buffer */
	char		*buf;		/* cache buffer for list entries */
} Cns_list;

struct Cns_segattrs {
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

			/* function prototypes */

EXTERN_C int Cns_access _PROTO((const char *, int));
EXTERN_C int Cns_accessUser _PROTO((const char *, int, uid_t, gid_t));
EXTERN_C int Cns_aborttrans _PROTO(());
EXTERN_C int Cns_apiinit _PROTO((struct Cns_api_thread_info **));
EXTERN_C int Cns_chclass _PROTO((const char *, int, char *));
EXTERN_C int Cns_chdir _PROTO((const char *));
EXTERN_C int Cns_chmod _PROTO((const char *, mode_t));
EXTERN_C int Cns_chown _PROTO((const char *, uid_t, gid_t));
EXTERN_C int Cns_client_getAuthorizationId _PROTO((uid_t *, gid_t *, char **, char **));
EXTERN_C int Cns_client_setAuthorizationId _PROTO((uid_t, gid_t, const char *, char *));
EXTERN_C int Cns_closedir _PROTO((Cns_DIR *));
EXTERN_C int Cns_creat _PROTO((const char *, mode_t));
EXTERN_C int Cns_creatg _PROTO((const char *, const char *, mode_t));
EXTERN_C int Cns_creatx _PROTO((const char *, mode_t, struct Cns_fileid *));
EXTERN_C int Cns_delcomment _PROTO((const char *));
EXTERN_C int Cns_delete _PROTO((const char *));
EXTERN_C int Cns_deleteclass _PROTO((char *, int, char *));
EXTERN_C int Cns_dropsegs _PROTO((const char *, struct Cns_fileid *));
EXTERN_C int Cns_delsegbycopyno _PROTO((const char *, struct Cns_fileid *, int));
EXTERN_C int Cns_du _PROTO((const char *, int, u_signed64 *, u_signed64 *));
EXTERN_C int Cns_endsess _PROTO(());
EXTERN_C int Cns_endtrans _PROTO(());
EXTERN_C int Cns_enterclass _PROTO((char *, struct Cns_fileclass *));
EXTERN_C int Cns_errmsg _PROTO((char *, char *, ...));
EXTERN_C int Cns_getacl _PROTO((const char *, int, struct Cns_acl *));
EXTERN_C int Cns_getcomment _PROTO((const char *, char *));
EXTERN_C char *Cns_getcwd _PROTO((char *, int));
EXTERN_C int Cns_getlinks _PROTO((const char *, const char *, int *, struct Cns_linkinfo **));
EXTERN_C int Cns_getpath _PROTO((char *, u_signed64, char *));
EXTERN_C int Cns_getsegattrs _PROTO((const char *, struct Cns_fileid *, int *, struct Cns_segattrs **));
EXTERN_C int Cns_lchown _PROTO((const char *, uid_t, gid_t));
EXTERN_C struct Cns_fileclass *Cns_listclass _PROTO((char *, int, Cns_list *));
EXTERN_C struct Cns_direntape *Cns_listtape _PROTO((char *, char *, int, Cns_list *, int));
EXTERN_C struct Cns_linkinfo *Cns_listlinks _PROTO((const char *, const char *, int, Cns_list *));
EXTERN_C int Cns_lstat _PROTO((const char *, struct Cns_filestat *));
EXTERN_C int Cns_lastfseq _PROTO((const char *, int, struct Cns_segattrs *));
EXTERN_C int Cns_bulkexist _PROTO((const char *, u_signed64 *, int *));
EXTERN_C int Cns_mkdir _PROTO((const char *, mode_t));
EXTERN_C int Cns_mkdirg _PROTO((const char *, const char *, mode_t));
EXTERN_C int Cns_modifyclass _PROTO((char *, int, char *, struct Cns_fileclass *));
EXTERN_C Cns_DIR *Cns_opendir _PROTO((const char *));
EXTERN_C Cns_DIR *Cns_opendirg _PROTO((const char *, const char *));
EXTERN_C Cns_DIR *Cns_opendirxg _PROTO((char *, const char *, const char *));
EXTERN_C int Cns_ping _PROTO((char *, char *));
EXTERN_C int Cns_queryclass _PROTO((char *, int, char *, struct Cns_fileclass *));
EXTERN_C struct dirent *Cns_readdir _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direncomm *Cns_readdirc _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direnstatg *Cns_readdirg _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direnstat *Cns_readdirx _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direnstatc *Cns_readdirxc _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direntape *Cns_readdirxt _PROTO((Cns_DIR *));
EXTERN_C int Cns_readlink _PROTO((const char *, char *, size_t));
EXTERN_C int Cns_rename _PROTO((const char *, const char *));
EXTERN_C int Cns_replacetapecopy _PROTO((struct Cns_fileid *, const char*, const char*, int, struct Cns_segattrs *, time_t last_mod_time));
EXTERN_C int Cns_replaceseg _PROTO((char *, u_signed64, struct Cns_segattrs *, struct Cns_segattrs *, time_t last_mod_time));
EXTERN_C int Cns_updateseg_checksum _PROTO((char *, u_signed64, struct Cns_segattrs *, struct Cns_segattrs *));
EXTERN_C int Cns_updateseg_status _PROTO((char *, u_signed64, struct Cns_segattrs *, const char));
EXTERN_C int Cns_updatefile_checksum _PROTO((const char *, const char *, const char *));
EXTERN_C void Cns_rewinddir _PROTO((Cns_DIR *));
EXTERN_C int Cns_rmdir _PROTO((const char *));
EXTERN_C int Cns_selectsrvr _PROTO((const char *, char *, char *, char **));
EXTERN_C int Cns_setacl _PROTO((const char *, int, struct Cns_acl *));
EXTERN_C int Cns_setatime _PROTO((const char *, struct Cns_fileid *));
EXTERN_C int Cns_setcomment _PROTO((const char *, char *));
EXTERN_C int Cns_seterrbuf _PROTO((char *, int));
EXTERN_C int Cns_setfsize _PROTO((const char *, struct Cns_fileid *, u_signed64, time_t, time_t));
EXTERN_C int Cns_setfsizecs _PROTO((const char *, struct Cns_fileid *, u_signed64, const char *, const char *, time_t, time_t));
EXTERN_C int Cns_setsegattrs _PROTO((const char *, struct Cns_fileid *, int, struct Cns_segattrs *, time_t));
EXTERN_C int Cns_setfsizeg _PROTO((const char *, u_signed64, const char *, const char *, time_t, time_t));
EXTERN_C int Cns_startsess _PROTO((char *, char *));
EXTERN_C int Cns_starttrans _PROTO((char *, char *));
EXTERN_C int Cns_stat _PROTO((const char *, struct Cns_filestat *));
EXTERN_C int Cns_statg _PROTO((const char *, const char *, struct Cns_filestatg *));
EXTERN_C int Cns_statx _PROTO((const char *, struct Cns_fileid *, struct Cns_filestat *));
EXTERN_C int Cns_statcsx _PROTO((const char *, struct Cns_fileid *, struct Cns_filestatcs *));
EXTERN_C int Cns_statcs _PROTO((const char *, struct Cns_filestatcs *));
EXTERN_C int Cns_symlink _PROTO((const char *, const char *));
EXTERN_C int Cns_tapesum _PROTO((char *, const char *, u_signed64 *, u_signed64 *, u_signed64 *, int));
EXTERN_C mode_t Cns_umask _PROTO((mode_t));
EXTERN_C int Cns_undelete _PROTO((const char *));
EXTERN_C int Cns_unlink _PROTO((const char *));
EXTERN_C int Cns_unlinkbyvid _PROTO((char *, const char *));
EXTERN_C int Cns_utime _PROTO((const char *, struct utimbuf *));
EXTERN_C int send2nsd _PROTO((int *, char *, char *, int, char *, int));
EXTERN_C int Cns_setid _PROTO((uid_t, gid_t));
EXTERN_C int Cns_getid _PROTO((uid_t *, gid_t *));
EXTERN_C int Cns_getrealid _PROTO((uid_t *, gid_t *));
EXTERN_C int Cns_unsetid _PROTO(());
EXTERN_C int send2nsdx _PROTO((int *, char *, char *, int, char *, int, void **, int *));

			/* function protypes for ID tables */

EXTERN_C int Cns_getgrpbygid _PROTO((gid_t, char *));
EXTERN_C int Cns_getgrpbynam _PROTO((char *, gid_t *));
EXTERN_C int Cns_getusrbyuid _PROTO((uid_t, char *));
EXTERN_C int Cns_getusrbynam _PROTO((char *, uid_t *));
EXTERN_C int Cns_getidmap _PROTO((const char *, int, const char **, uid_t *, gid_t *));
EXTERN_C int Cns_modifygrpmap _PROTO((gid_t, char *));
EXTERN_C int Cns_modifyusrmap _PROTO((uid_t, char *));
EXTERN_C int Cns_rmgrpmap _PROTO((gid_t, char *));
EXTERN_C int Cns_rmusrmap _PROTO((uid_t, char *));
#endif
